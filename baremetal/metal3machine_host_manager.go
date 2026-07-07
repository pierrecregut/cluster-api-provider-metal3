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
	"maps"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	HostClaimAnnotation = "metal3.io/hostclaim"
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
// * IsProvisioned checks if the metal3machine is provisioned.
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

// getHostClaim gets the associated hostclaim by looking for an annotation on the machine
// that contains a reference to the host. Returns nil if not found. Assumes the
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
	var hostclaim *bmov1alpha1.HostClaim

	// We must create a new hostClaim either because we are not in node
	// reuse mode or we did not find a node to reuse.
	m.Log.Info("Creating a new host", "host", m.Metal3Machine.Name, "namespace", namespace)
	hostclaim = &bmov1alpha1.HostClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Metal3Machine.Name,
			Namespace: namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(
		ctx, hostClient, hostclaim,
		func() error {
			if hostclaim.Labels == nil {
				hostclaim.Labels = make(map[string]string)
			}
			hostclaim.Labels[clusterv1.ClusterNameLabel] = m.Machine.Spec.ClusterName
			hostclaim.Spec.HostSelector = selector
			hostclaim.Spec.PoweredOn = false
			hostclaim.Spec.FailureDomain = m.Metal3Machine.Spec.FailureDomain
			return m.setHostConsumerRef(ctx, hostclaim)
		})
	if err != nil {
		return nil, nil, err
	}
	helper, err := patch.NewHelper(hostclaim, hostClient)
	return hostclaim, helper, err
}

// setHostConsumerRef will ensure the host's Spec is set to link to the
// Metal3Machine that created it. Name and namespace can only be interpreted on the cluster
// hosting the Metal3Machine, so we use a special kind for remote claims to prevent a local
// controller on the remote cluster from resolving those consumers.
func (m *MachineHostClaimManager) setHostConsumerRef(_ context.Context, hostclaim *bmov1alpha1.HostClaim) error {
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
