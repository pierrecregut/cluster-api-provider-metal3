/*
Copyright 2026 Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package baremetal

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	HostClaimAnnotation = "metal3.io/hostclaim"
	// nodeReuseCleared is set on a kept HostClaim when it is cleared during
	// Delete, as an RFC3339 timestamp for debugging / observability only.
	nodeReuseCleared = "infrastructure.cluster.x-k8s.io/node-reuse-cleared-at"
)

// MachineHostClaimManager is responsible for performing machine reconciliation.
// It is the variant for cluster targeting HostClaim instead of BareMetalHost
// It has exactly the same structure but will implement methods of the
// MachineManagerInterface differently.
type MachineHostClaimManager struct {
	MachineManager
	remoteClient *RemoteClient
}

var _ MachineManagerInterface = &MachineHostClaimManager{}

func NewMachineHostClaimManager(ctx context.Context, localClient client.Client, clientCache RemoteClientCacheInterface,
	cluster *clusterv1.Cluster, metal3Cluster *infrav1.Metal3Cluster,
	machine *clusterv1.Machine, metal3machine *infrav1.Metal3Machine,
	machineLog logr.Logger) (*MachineHostClaimManager, error) {
	log := machineLog
	if machine != nil {
		log = log.WithValues(LogFieldMachine, machine.Name)
	}
	if metal3machine != nil {
		log = log.WithValues(
			LogFieldMetal3Machine, metal3machine.Name,
			LogFieldNamespace, metal3machine.Namespace)
	}
	// Delegate the base init to NewMachineManager so any future work it does
	// (validation, extra defaults) applies to the HostClaim variant too.
	base, err := NewMachineManager(localClient, cluster, metal3Cluster, machine, metal3machine, log)
	if err != nil {
		return nil, err
	}
	remoteClient, err := clientCache.GetRemoteClient(ctx, log, metal3machine)
	if err != nil {
		base.SetCondition(infrav1.AssociateBareMetalHostCondition, metav1.ConditionFalse, infrav1.RemoteClusterUnavailableReason, err.Error())
		return nil, err
	}
	mhm := &MachineHostClaimManager{
		MachineManager: *base,
		remoteClient:   remoteClient,
	}
	// Override the base resolver so calls through m.resolver go to the
	// HostClaim methods on MachineHostClaimManager rather than the annotation-based
	// methods on MachineManager.
	mhm.resolver = mhm
	return mhm, nil
}

// The following functions are inherited from the metal3machine manager
// * SetFinalizer sets finalizer
// * UnsetFinalizer unsets finalizer
// * IsBootstrapReady checks if the machine is given Bootstrap data.
// * GetProviderIDAndBMHID returns providerID and bmhID.
// * SetProviderID sets the metal3 provider ID on the Metal3Machine.
// * DissociateM3Metadata removes machine from OwnerReferences of meta3DataTemplate, on failure requeue.
// * AssociateM3Metadata fetches the Metal3DataTemplate object and sets the owner references.
// * SetError sets the ErrorMessage and ErrorReason fields on the machine and logs the message.
// * SetCondition

// HasAnnotation makes sure the machine has an annotation that references a host.
func (m *MachineHostClaimManager) HasAnnotation() bool {
	annotations := m.Metal3Machine.ObjectMeta.GetAnnotations()
	if annotations == nil {
		return false
	}
	_, ok := annotations[HostClaimAnnotation]
	return ok
}

// Associate associates a machine and is invoked by the Machine Controller.
func (m *MachineHostClaimManager) Associate(ctx context.Context) (string, error) {
	// No need to lock this is an object creation
	m.Log.Info("Associating Metal3Machine with HostClaim")

	// load and validate the config
	if m.Metal3Machine == nil {
		// Should have been picked earlier. Do not requeue
		return "", nil
	}

	// look for associated HostClaim
	hostClaim, helper, err := m.getHostClaim(ctx)
	if err != nil {
		return "", err
	}

	// Check coherence of userData
	if err = m.getUserDataSecretName(ctx); err != nil {
		return "", err
	}

	if hostClaim == nil {
		// no HostClaim yet, create a new one
		hostClaim, helper, err = m.provideHostClaim(ctx)
		if err != nil {
			return "", err
		}
		m.Log.Info("Created HostClaim for association", LogFieldHostClaim, hostClaim.Name)
	} else {
		m.Log.Info("Machine already associated with HostClaim", LogFieldHostClaim, hostClaim.Name)
	}

	err = helper.Patch(ctx, hostClaim)
	if err != nil {
		return "", hideConflictError(err)
	}

	m.ensureAnnotation(ctx, hostClaim)

	m.Log.Info("Finished associating machine")

	return infrav1.AssociateHostClaimSuccessReason, nil
}

// Delete deletes a metal3 machine and is invoked by the Machine Controller.
// All the burden (deprovisioning) is on the HostClaim controller and fields
// are left as they are in the hostclaim.
// The finalizer will be removed in the controller only when the HostClaim
// disappears.
func (m *MachineHostClaimManager) Delete(ctx context.Context) error {
	m.Log.Info("Deleting metal3 machine")
	hostClaim, helper, err := m.getHostClaim(ctx)
	if err != nil {
		return err
	}
	if hostClaim == nil {
		m.Log.Info("hostclaim not found for metal3machine")
		if m.remoteClient != nil {
			m.remoteClient.Release(m.Metal3Machine)
		}
		return nil
	}

	// If owned by somebody else, stop here and release the remoteClient.
	if hostClaim.Spec.ConsumerRef != nil && !consumerRefMatches(hostClaim.Spec.ConsumerRef, m.Metal3Machine) {
		m.Log.Info("hostclaim no longer references this metal3machine; releasing finalizer",
			LogFieldHostClaim, hostClaim.Name, LogFieldHostClaimNamespace, hostClaim.Namespace)
		if m.remoteClient != nil {
			m.remoteClient.Release(m.Metal3Machine)
		}
		return nil
	}

	// Proceeds like standard metalmachine_manager:
	// * if the cluster is being deleted, perform standard deletion
	// * otherwise if nodeReuse is enabled, get the owner of the machine (Control-plane
	//   or MachineDeployment) and use it as a ConsumerRef.
	// * Keep the HostClaim but change its ConsumerRef.
	if m.Cluster != nil && m.Cluster.DeletionTimestamp.IsZero() {
		nodeReuserRef, nrErr := m.getNodeReuserRef(ctx)
		if nrErr != nil {
			return fmt.Errorf("cannot get node reuser reference: %w", nrErr)
		}
		if nodeReuserRef != nil {
			// Only act once: if the HostClaim's ConsumerRef still points at us,
			// clear its provisioning content and retarget it at the owner.
			if consumerRefMatches(hostClaim.Spec.ConsumerRef, m.Metal3Machine) {
				m.Log.Info("Clearing hostClaim (node reuse mode)",
					"nodeReuserKind", nodeReuserRef.Kind,
					"nodeReuserName", nodeReuserRef.Name)
				// We clear the spec. The metadata (label and owner reference to the cluster) stays untouched.
				// The hostclaim is still part of the cluster as the control-plane or the machine deployment
				// are subpart of it.
				hostClaim.Spec.Image = nil
				hostClaim.Spec.UserData = nil
				hostClaim.Spec.NetworkData = nil
				hostClaim.Spec.MetaData = nil
				hostClaim.Spec.PoweredOn = false
				if hostClaim.Annotations == nil {
					hostClaim.Annotations = map[string]string{}
				}
				// Annotation can be used to clear hostclaim never reused (scale down)
				hostClaim.Annotations[nodeReuseCleared] = time.Now().UTC().Format(time.RFC3339)
				hostClaim.Spec.ConsumerRef = nodeReuserRef
				if err = helper.Patch(ctx, hostClaim); err != nil {
					m.Log.Error(err, "Failed to retarget hostClaim ConsumerRef to node reuser")
					return err
				}
				// Requeue until the HostClaim is no longer bound to the Metal3Machine
				// the finalizer can then be released.
				return WithTransientError(errors.New("waiting for reusable hostClaim release"), requeueAfter)
			}
			// Already retargeted (or never ours); nothing more to do here.
			return nil
		}
	}

	hostClient := m.client
	if m.remoteClient != nil {
		hostClient = m.remoteClient
	}
	if hostClaim.DeletionTimestamp.IsZero() {
		err = hostClient.Delete(ctx, hostClaim)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Somebody removed the claim in the meantime
				return nil
			}
			m.Log.Error(err, "cannot delete associated hostClaim",
				LogFieldHostClaim, hostClaim.Name, LogFieldHostClaimNamespace, hostClaim.Namespace)
			return err
		}
	}
	err = errors.New("waiting for associated HostClaim deletion")
	return WithTransientError(err, requeueAfter)
}

// Update updates a machine and is invoked by the Machine Controller.
func (m *MachineHostClaimManager) Update(ctx context.Context) error {
	m.Log.Info("Updating machine")

	hostClaim, helper, err := m.getHostClaim(ctx)
	if err != nil {
		return err
	}
	if hostClaim == nil {
		err = fmt.Errorf("HostClaim not found for machine %s", m.Machine.Name)
		return WithTransientError(err, requeueAfter)
	}

	if err = m.WaitForM3Metadata(ctx); err != nil {
		return err
	}

	err = m.setHostClaimConsumerRef(ctx, hostClaim)
	if err != nil {
		return err
	}

	// ensures that the BMH specs are correctly set.
	err = m.setHostClaimSpec(ctx, hostClaim)
	if err != nil {
		return err
	}

	err = helper.Patch(ctx, hostClaim)
	if err != nil {
		return err
	}

	m.ensureAnnotation(ctx, hostClaim)

	if err := m.updateMachineStatus(ctx, hostClaim); err != nil {
		return err
	}

	m.Log.Info("Finished updating machine")
	return nil
}

// EnsureBlockMove prevents moving a BMH before its hardwareDetails are copied
// to status. This has no meaning for the HostClaim resource.
func (m *MachineHostClaimManager) EnsureBlockMoveAnnotation(_ context.Context) error {
	return nil
}

// SetPauseAnnotation sets the pause annotations on associated HostClaim as long as it is
// local.
func (m *MachineHostClaimManager) SetPauseAnnotation(ctx context.Context) error {
	if m.remoteClient != nil {
		return nil
	}
	// look for associated HostClaim
	hostclaim, helper, err := m.getHostClaim(ctx)
	if err != nil {
		m.Log.Info("Failed to get HostClaim for Metal3Machine, requeuing",
			LogFieldMetal3Machine, m.Metal3Machine.Name,
			LogFieldNamespace, m.Metal3Machine.Namespace,
			LogFieldError, err.Error())
		return WithTransientError(err, requeueAfter)
	}
	if hostclaim == nil {
		return nil
	}

	annotations := hostclaim.GetAnnotations()

	if annotations != nil {
		if _, ok := annotations[bmov1alpha1.PausedAnnotation]; ok {
			m.Log.Info("HostClaim is already paused")
			return nil
		}
	} else {
		hostclaim.Annotations = make(map[string]string)
	}
	m.Log.Info("Adding PausedAnnotation in HostClaim")
	hostclaim.Annotations[bmov1alpha1.PausedAnnotation] = PausedAnnotationKey

	// We should not need to keep the status of host for pivot.
	// Proxy should maintain the synchronisation over status.
	return helper.Patch(ctx, hostclaim)
}

// RemovePauseAnnotation checks and/or Removes the pause annotations on associated HostClaim.
func (m *MachineHostClaimManager) RemovePauseAnnotation(ctx context.Context) error {
	if m.remoteClient != nil {
		return nil
	}
	hostclaim, helper, err := m.getHostClaim(ctx)
	if err != nil {
		m.Log.Info("Failed to get HostClaim for Metal3Machine, requeuing",
			LogFieldMetal3Machine, m.Metal3Machine.Name,
			LogFieldNamespace, m.Metal3Machine.Namespace,
			LogFieldError, err.Error())
		return WithTransientError(errors.New("failed to get a HostClaim for the Metal3Machine, requeuing"), requeueAfter)
	}

	if hostclaim == nil || m.Cluster == nil {
		return nil
	}

	annotations := hostclaim.GetAnnotations()

	if annotations != nil {
		if _, ok := annotations[bmov1alpha1.PausedAnnotation]; ok {
			if m.Cluster.Name == hostclaim.Labels[clusterv1.ClusterNameLabel] && annotations[bmov1alpha1.PausedAnnotation] == PausedAnnotationKey {
				// Removing HostClaim Paused Annotation Since Owner Cluster is not paused.
				delete(hostclaim.Annotations, bmov1alpha1.PausedAnnotation)
			} else if m.Cluster.Name == hostclaim.Labels[clusterv1.ClusterNameLabel] && annotations[bmov1alpha1.PausedAnnotation] != PausedAnnotationKey {
				m.Log.Info("HostClaim is paused by user. Not removing Pause Annotation")
				return nil
			}
		}
	}
	return helper.Patch(ctx, hostclaim)
}

// getHostClaim gets the associated host by looking for an annotation on the machine
// that contains a reference to the hostclaim. Returns nil if not found. Assumes the
// hostclaim is in the same namespace as the machine if local.
func (m *MachineHostClaimManager) getHostClaim(ctx context.Context) (*bmov1alpha1.HostClaim, *patch.Helper, error) {
	hostClient := m.client
	namespace := m.Metal3Machine.Namespace
	if m.remoteClient != nil {
		namespace = m.remoteClient.Namespace
		hostClient = m.remoteClient
	}
	hostclaim, err := getHostClaim(ctx, m.Metal3Machine, hostClient, namespace, m.Log)
	if err != nil || hostclaim == nil {
		return hostclaim, nil, err
	}
	helper, err := patch.NewHelper(hostclaim, hostClient)
	return hostclaim, helper, err
}

func getHostClaim(
	ctx context.Context, m3Machine *infrav1.Metal3Machine, cl client.Client,
	namespace string, mLog logr.Logger,
) (*bmov1alpha1.HostClaim, error) {
	annotations := m3Machine.ObjectMeta.GetAnnotations()
	if annotations == nil {
		return nil, nil //nolint:nilnil
	}
	hostClaimName, ok := annotations[HostClaimAnnotation]
	if !ok {
		return nil, nil //nolint:nilnil
	}
	hostclaim := bmov1alpha1.HostClaim{}
	key := client.ObjectKey{
		Name:      hostClaimName,
		Namespace: namespace,
	}
	err := cl.Get(ctx, key, &hostclaim)
	if apierrors.IsNotFound(err) {
		mLog.Info("Referenced hostclaim does not exist anymore", LogFieldHostClaim, hostClaimName, LogFieldHostClaimNamespace, namespace)
		delete(annotations, HostClaimAnnotation)
		return nil, nil //nolint:nilnil
	} else if err != nil {
		return nil, err
	}
	return &hostclaim, nil
}

// Synchronize the secrets from the m3machine namespace to the hostclaim
// namespace. Original secrets are defined by a reference.
// Names are derived from the hostClaim name with a suffix (purpose).
// the function gives back a reference to the new secret or an error.
func (m *MachineHostClaimManager) syncSecretData(
	ctx context.Context, source *corev1.SecretReference,
	hostclaim *bmov1alpha1.HostClaim, secretRole string,
) (*corev1.SecretReference, error) {
	log := m.Log.WithValues(LogFieldSecretRole, secretRole)
	log.Info("Setting secret")
	if source == nil {
		return nil, nil //nolint:nilnil
	}
	target := corev1.SecretReference{}
	// local HostClaim, we just push the reference and fix the namespace if needed.
	if m.remoteClient == nil {
		target.Name = source.Name
	} else {
		namespace := m.remoteClient.Namespace
		target.Name = hostclaim.Name + "-" + secretRole
		localSecretKey := client.ObjectKey{
			Name:      source.Name,
			Namespace: m.Metal3Machine.Namespace,
		}
		localSecret := corev1.Secret{}
		err := m.client.Get(ctx, localSecretKey, &localSecret)
		if err != nil {
			log.Error(err, "missing data secret - requeueing", LogFieldSecretName, source.Name, LogFieldNamespace, m.Metal3Machine.Namespace)
			if apierrors.IsNotFound(err) {
				return nil, WithTransientError(err, requeueAfter)
			}
			log.Error(err, "Transient error while fetching local data secret", LogFieldSecretName, source.Name, LogFieldNamespace, m.Metal3Machine.Namespace)
			return nil, err
		}
		remoteSecret := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      target.Name,
				Namespace: namespace,
			},
		}
		_, err = controllerutil.CreateOrUpdate(
			ctx, m.remoteClient, &remoteSecret,
			func() error {
				remoteSecret.Data = localSecret.Data
				remoteSecret.Type = localSecret.Type
				return controllerutil.SetOwnerReference(hostclaim, &remoteSecret, m.remoteClient.Scheme())
			})
		if err != nil {
			log.Error(err, "cannot update remote secret", LogFieldSecretName, target.Name, LogFieldHostClaimNamespace, namespace)
			return nil, err
		}
	}
	return &target, nil
}

// setHostClaimSpec will ensure the HostClaim Spec is set according to the machine's
// details. It will then update the HostClaim via the kube API. If UserData does not
// include a Namespace, it will default to the Metal3Machine's namespace.
func (m *MachineHostClaimManager) setHostClaimSpec(ctx context.Context, hostClaim *bmov1alpha1.HostClaim) error {
	// We only want to update the image setting if the host does not
	// already have an image.
	//
	// A host with an existing image or customDeploy is already provisioned
	// and upgrades are not supported at this time. To re-provision a
	// host, we must fully deprovision it and then provision it again.
	// Not provisioning while we do not have the UserData.
	if hostClaim.Spec.Image == nil && hostClaim.Spec.CustomDeploy == nil && m.Metal3Machine.Status.UserData != nil {
		// Ensure either Image or CustomDeploy is configured
		imageConfigured := m.Metal3Machine.Spec.Image.URL != ""
		customDeployConfigured := m.Metal3Machine.Spec.CustomDeploy.Method != ""

		if !imageConfigured && !customDeployConfigured {
			logger := m.Log.WithValues("Metal3Machine", m.Metal3Machine.Name)
			logger.Error(nil, "Neither Image nor CustomDeploy is configured on Metal3Machine, unable to provision")
			return fmt.Errorf("Metal3Machine %s has neither Image.URL nor CustomDeploy.Method configured", m.Metal3Machine.Name)
		}

		if imageConfigured {
			hostClaim.Spec.Image = &bmov1alpha1.Image{
				URL:          m.Metal3Machine.Spec.Image.URL,
				Checksum:     ptr.Deref(m.Metal3Machine.Spec.Image.Checksum, ""),
				ChecksumType: bmov1alpha1.ChecksumType(m.Metal3Machine.Spec.Image.ChecksumType),
				DiskFormat:   &m.Metal3Machine.Spec.Image.DiskFormat,
			}
		}
		if customDeployConfigured {
			hostClaim.Spec.CustomDeploy = &bmov1alpha1.CustomDeploy{
				Method: m.Metal3Machine.Spec.CustomDeploy.Method,
			}
		}

		ref, err := m.syncSecretData(ctx, m.Metal3Machine.Status.UserData, hostClaim, "userdata")
		if err != nil {
			return err
		}
		hostClaim.Spec.UserData = ref

		ref, err = m.syncSecretData(ctx, m.Metal3Machine.Status.MetaData, hostClaim, "metadata")
		if err != nil {
			return err
		}
		hostClaim.Spec.MetaData = ref

		ref, err = m.syncSecretData(ctx, m.Metal3Machine.Status.NetworkData, hostClaim, "networkdata")
		if err != nil {
			return err
		}
		hostClaim.Spec.NetworkData = ref

		hostClaim.Spec.AutomatedCleaningMode = bmov1alpha1.AutomatedCleaningMode(m.Metal3Machine.Spec.AutomatedCleaningMode)

		hostClaim.Spec.PoweredOn = true
	}
	return nil
}

// ensureAnnotation makes sure the machine has an annotation that references the
// hostclaim.
func (m *MachineHostClaimManager) ensureAnnotation(_ context.Context, hostClaim *bmov1alpha1.HostClaim) {
	annotations := m.Metal3Machine.Annotations
	if annotations == nil {
		annotations = make(map[string]string)
	}
	hostKey := hostClaim.Name
	existing, ok := annotations[HostClaimAnnotation]
	if ok {
		if existing == hostKey {
			return
		}
		m.Log.Info("Warning: found stray annotation for hostclaim on machine. Overwriting.", LogFieldHostClaim, existing)
	}
	annotations[HostClaimAnnotation] = hostKey
	m.Metal3Machine.Annotations = annotations
}

func translateHostRequirements(meList []infrav1.HostSelectorRequirement) []bmov1alpha1.HostSelectorRequirement {
	matchExpressions := make([]bmov1alpha1.HostSelectorRequirement, len(meList))
	for i, me := range meList {
		matchExpressions[i] = bmov1alpha1.HostSelectorRequirement{
			Key:      me.Key,
			Operator: me.Operator,
			Values:   me.Values,
		}
	}
	return matchExpressions
}

// provideHostClaim creates a new HostClaim or gives back an existing one if the metal3Machine is in
// nodeReuse mode and a free HostClaim in the same group exists.
func (m *MachineHostClaimManager) provideHostClaim(ctx context.Context) (*bmov1alpha1.HostClaim, *patch.Helper, error) {
	namespace := m.Metal3Machine.Namespace
	hostClient := m.client
	if m.remoteClient != nil {
		hostClient = m.remoteClient
		namespace = m.remoteClient.Namespace
	}

	hostSelector := m.Metal3Machine.Spec.HostSelector
	var selector bmov1alpha1.HostSelector
	// Should not occur as long as InNamespace is used to distinguish hostclaim case.
	if hostSelector != nil {
		selector.MatchLabels = maps.Clone(hostSelector.MatchLabels)
		selector.MatchExpressions = translateHostRequirements(hostSelector.MatchExpressions)
		selector.InNamespace = hostSelector.InNamespace
	}
	nodeReuserRef, err := m.getNodeReuserRef(ctx)
	if err != nil {
		m.Log.Error(err, "Error resolving node reuser reference")
		return nil, nil, err
	}
	var hostclaim *bmov1alpha1.HostClaim

	if nodeReuserRef != nil {
		hostclaim, err = m.findReusableHostClaim(ctx, hostClient, namespace, nodeReuserRef)
		if err != nil {
			return nil, nil, err
		}
	}
	if hostclaim != nil {
		m.Log.Info("Reusing host", "host", hostclaim.Name, "namespace", hostclaim.Namespace)
		if err = m.setHostClaimConsumerRef(ctx, hostclaim); err != nil {
			return nil, nil, err
		}
		if hostclaim.Annotations != nil {
			delete(hostclaim.Annotations, nodeReuseCleared)
		}
		if hostclaim.Labels == nil {
			hostclaim.Labels = map[string]string{}
		}
		hostclaim.Labels[clusterv1.ClusterNameLabel] = m.Machine.Spec.ClusterName
		hostclaim.Spec.HostSelector = selector
		hostclaim.Spec.FailureDomain = m.Metal3Machine.Spec.FailureDomain
		if err = hostClient.Update(ctx, hostclaim); err != nil {
			m.Log.Error(err, "Cannot bind to a reusable host")
			return nil, nil, err
		}
		helper, errPatch := patch.NewHelper(hostclaim, hostClient)
		return hostclaim, helper, errPatch
	}

	// We must create a new hostClaim either because we are not in node
	// reuse mode or we did not find a node to reuse.
	m.Log.Info("Creating a new host", "host", m.Metal3Machine.Name, "namespace", namespace)
	hostclaim = &bmov1alpha1.HostClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Metal3Machine.Name,
			Namespace: namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(
		ctx, hostClient, hostclaim,
		func() error {
			if hostclaim.Labels == nil {
				hostclaim.Labels = map[string]string{}
			}
			hostclaim.Labels[clusterv1.ClusterNameLabel] = m.Machine.Spec.ClusterName
			hostclaim.Spec.HostSelector = selector
			hostclaim.Spec.PoweredOn = false
			hostclaim.Spec.FailureDomain = m.Metal3Machine.Spec.FailureDomain
			return m.setHostClaimConsumerRef(ctx, hostclaim)
		})
	if err != nil {
		return nil, nil, err
	}
	helper, err := patch.NewHelper(hostclaim, hostClient)
	return hostclaim, helper, err
}

// findReusableHostClaim looks for a HostClaim in the target namespace with
// a ConsumerRef set to nodeReuserRef (the owner control-plane or machine deployment
// of the machine). If none of the HostClaims found is available for provisioning,
// a requeue error is raised to wait until one is deprovisioned.
// If a claim's ConsumerRef already points at the current Metal3Machine it
// is immediately selected (recovery from a transient error).
func (m *MachineHostClaimManager) findReusableHostClaim(
	ctx context.Context, hostClient client.Client, namespace string,
	nodeReuserRef *corev1.ObjectReference,
) (*bmov1alpha1.HostClaim, error) {
	ownerGV, err := schema.ParseGroupVersion(nodeReuserRef.APIVersion)
	if err != nil {
		m.Log.Error(err, "Found an unparsable APIVersion on nodeReuser", "apiVersion", nodeReuserRef.APIVersion)
		return nil, err
	}
	allHostclaims := bmov1alpha1.HostClaimList{}
	if err := hostClient.List(ctx, &allHostclaims, client.InNamespace(namespace)); err != nil {
		m.Log.Error(err, "Error listing hostclaims for node reuse")
		return nil, err
	}
	var pending bool
	for i := range allHostclaims.Items {
		candidate := &allHostclaims.Items[i]
		ref := candidate.Spec.ConsumerRef
		if ref == nil {
			continue
		}
		if consumerRefMatches(ref, m.Metal3Machine) {
			// Already bound to this Metal3Machine
			return candidate, nil
		}
		if ref.Kind != nodeReuserRef.Kind || ref.Name != nodeReuserRef.Name {
			continue
		}
		gv, gvErr := schema.ParseGroupVersion(ref.APIVersion)
		if gvErr != nil {
			m.Log.Error(gvErr, "Unparsable APIVersion on candidate hostclaim consumerRef",
				"apiVersion", ref.APIVersion, "hostClaim", candidate.Name)
			continue
		}
		if gv.Group != ownerGV.Group {
			continue
		}
		if candidate.DeletionTimestamp != nil {
			continue
		}
		if conditions.IsTrue(candidate, bmov1alpha1.AvailableForProvisioningCondition) {
			return candidate, nil
		}
		pending = true
	}
	if pending {
		m.Log.Info("Waiting for a candidate reusable hostclaim to become available")
		return nil, WithTransientError(errors.New("waiting for reusable hostclaim"), requeueAfter)
	}
	return nil, nil //nolint:nilnil
}

// hardwareDataAddresses returns a slice of corev1.NodeAddress objects from the HardwareData.
func hardwareDataAddresses(hwdata *bmov1alpha1.HardwareData) []clusterv1.MachineAddress {
	addrs := []clusterv1.MachineAddress{}

	// If the hardwareData is nil or we have no hw details, return an empty address array.
	if hwdata == nil || hwdata.Spec.HardwareDetails == nil {
		return addrs
	}

	for _, nic := range hwdata.Spec.HardwareDetails.NIC {
		if nic.IP == "" {
			continue
		}
		address := clusterv1.MachineAddress{
			Type:    clusterv1.MachineInternalIP,
			Address: nic.IP,
		}
		addrs = append(addrs, address)
	}

	if hwdata.Spec.HardwareDetails.Hostname != "" {
		addrs = append(addrs, clusterv1.MachineAddress{
			Type:    clusterv1.MachineHostName,
			Address: hwdata.Spec.HardwareDetails.Hostname,
		})
		addrs = append(addrs, clusterv1.MachineAddress{
			Type:    clusterv1.MachineInternalDNS,
			Address: hwdata.Spec.HardwareDetails.Hostname,
		})
	}

	return addrs
}

// updateMachineStatus updates a Metal3Machine object's status.
func (m *MachineHostClaimManager) updateMachineStatus(ctx context.Context, hostclaim *bmov1alpha1.HostClaim) error {
	oldM3mStatus := m.Metal3Machine.Status.DeepCopy()

	cl := m.client
	if m.remoteClient != nil {
		cl = m.remoteClient
	}

	hardwareDataRef := hostclaim.Status.HardwareData
	if hardwareDataRef == nil {
		// Will requeue when hostClaim status modified.
		m.Log.Info("No hardware data linked yet", LogFieldHostClaim, hostclaim.Name, LogFieldHostClaimNamespace, hostclaim.Namespace)
		return nil
	}

	hardwareData := &bmov1alpha1.HardwareData{}
	key := client.ObjectKey{Namespace: hardwareDataRef.Namespace, Name: hardwareDataRef.Name}
	if err := cl.Get(ctx, key, hardwareData); err != nil {
		m.Log.Error(err, "cannot access hardwareData",
			LogFieldHardwareData, hardwareDataRef.Name, LogFieldHardwareDataNamespace, hardwareDataRef.Namespace)
		return err
	}

	overrides, err := m.addressOverridesFromMetaData(ctx)
	if err != nil {
		return err
	}
	if len(overrides) > 0 {
		m.Metal3Machine.Status.Addresses = overrides
	} else {
		m.Metal3Machine.Status.Addresses = hardwareDataAddresses(hardwareData)
	}

	if equality.Semantic.DeepEqual(&m.Metal3Machine.Status, oldM3mStatus) {
		// Status did not change
		return nil
	}

	now := metav1.Now()
	m.Metal3Machine.Status.LastUpdated = &now
	return nil
}

// setHostClaimConsumerRef will ensure the host's Spec is set to link to the
// Metal3Machine that created it. Name and namespace can only be interpreted on the cluster
// hosting the Metal3Machine, so we use a special kind for remote claims to prevent a local
// controller on the remote cluster from resolving those consumers.
func (m *MachineHostClaimManager) setHostClaimConsumerRef(_ context.Context, hostclaim *bmov1alpha1.HostClaim) error {
	kind := metal3MachineKind
	cl := m.client
	if m.remoteClient != nil {
		kind = remoteMetal3MachineKind
		cl = m.remoteClient
	}
	hostclaim.Spec.ConsumerRef = &corev1.ObjectReference{
		Kind:       kind,
		Name:       m.Metal3Machine.Name,
		Namespace:  m.Metal3Machine.Namespace,
		APIVersion: m.Metal3Machine.APIVersion,
	}

	// Set OwnerReferences if not remote. Mainly used for pivoting.
	// We put it on the cluster rather than the metal3machine.
	// Deletion is handled separately especially if node-reuse is enabled.
	if m.remoteClient == nil {
		m.Log.Info("Setting local owner ref for pivot")
		if err := controllerutil.SetOwnerReference(m.Cluster, hostclaim, cl.Scheme()); err != nil {
			return err
		}
	}
	return nil
}

// IsBaremetalHostProvisioned returns true if the HostClaim Ready condition is true.
// This occurs only if the underlying Provisioned mirroring the baremetalhost condition is true.
func (m *MachineHostClaimManager) IsBaremetalHostProvisioned(ctx context.Context) bool {
	m.Log.V(VerbosityLevelDebug).Info("checking through hostclaim if underlying baremetalhost is provisioned")
	host, _, err := m.getHostClaim(ctx)
	if err != nil {
		m.Log.V(VerbosityLevelDebug).Info("failed to get host", "err", err)
		return false
	}
	if host == nil {
		m.Log.V(VerbosityLevelDebug).Info("getHost returned nil")
		return false
	}
	return conditions.IsTrue(host, clusterv1.ReadyCondition)
}

// getBmhUIDFromM3Machine retrieves bmhUID from m3m.
func (m *MachineHostClaimManager) getBmhUIDFromM3Machine(ctx context.Context) (string, error) {
	hostclaim, _, err := m.getHostClaim(ctx)
	if err != nil || hostclaim == nil {
		errMessage := "Failed to get a HostClaim for the metal3machine: " + m.Metal3Machine.GetName()
		return "", errors.New(errMessage)
	}
	if hostclaim.Status.BareMetalHost == nil {
		errMessage := "BareMetalHost not linked to HostClaim for Metal3Machine " + m.Metal3Machine.GetName()
		return "", errors.New(errMessage)
	}
	if hostclaim.Status.BareMetalHost.UID == "" {
		errMessage := "BareMetalHost UID not available in HostClaim for Metal3Machine " + m.Metal3Machine.GetName()
		return "", errors.New(errMessage)
	}
	return string(hostclaim.Status.BareMetalHost.UID), nil
}

// getBmhNameFromM3Machine retrieves bmhName from m3m annotations.
func (m *MachineHostClaimManager) getBmhNameFromM3Machine(ctx context.Context) (string, error) {
	hostclaim, _, err := m.getHostClaim(ctx)
	if err != nil || hostclaim == nil {
		errMessage := "Failed to get a HostClaim for the metal3machine: " + m.Metal3Machine.GetName()
		return "", errors.New(errMessage)
	}
	if hostclaim.Status.BareMetalHost == nil {
		errMessage := "BareMetalHost not linked to HostClaim for Metal3Machine " + m.Metal3Machine.GetName()
		return "", errors.New(errMessage)
	}
	return hostclaim.Status.BareMetalHost.Name, nil
}
