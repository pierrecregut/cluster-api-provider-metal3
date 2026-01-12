/*
Copyright 2024 Orange.

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
	"fmt"
	"time"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	clusterv1beta1 "sigs.k8s.io/cluster-api/api/core/v1beta1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	capierrors "sigs.k8s.io/cluster-api/errors"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	HostClaimAnnotation = "metal3.io/hostclaim"
	nodeReuseCleared    = "infrastructure.cluster.x-k8s.io/node-reuse-cleared-at"
)

// MachineHostManager is responsible for performing machine reconciliation.
// It is the variant for cluster targeting Host instead of BareMetalHost
// It has exactly the same structure but will implement methods of the
// MachineManagerInterface differently.
type MachineHostManager struct {
	MachineManager
	remoteClient    *RemoteClient
	remoteNamespace string
}

var _ MachineManagerInterface = &MachineHostManager{}

func NewMachineHostManager(localClient client.Client, clientCache RemoteClientCacheInterface,
	cluster *clusterv1.Cluster, metal3Cluster *infrav1.Metal3Cluster,
	machine *clusterv1.Machine, metal3machine *infrav1.Metal3Machine,
	machineLog logr.Logger) (*MachineHostManager, error) {
	remoteClient, err := clientCache.GetRemoteClient(machineLog, metal3machine)
	if err != nil {
		return nil, err
	}
	var remoteNamespace string
	if remoteClient != nil {
		remoteNamespace = remoteClient.Namespace
	}
	return &MachineHostManager{
		remoteClient:    remoteClient,
		remoteNamespace: remoteNamespace,
		MachineManager: MachineManager{
			client:        localClient,
			Cluster:       cluster,
			Metal3Cluster: metal3Cluster,
			Machine:       machine,
			Metal3Machine: metal3machine,
			Log:           machineLog,
		},
	}, nil
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
// * SetConditionMetal3MachineToFalse sets Metal3Machine condition status to False.
// * SetConditionMetal3MachineToTrue sets Metal3Machine condition status to True.

// Associate associates a machine and is invoked by the Machine Controller.
func (m *MachineHostManager) Associate(ctx context.Context) error {
	// No need to lock this is an object creation
	m.Log.Info("Associating machine to host", "machine", m.Machine.Name)

	// load and validate the config
	if m.Metal3Machine == nil {
		// Should have been picked earlier. Do not requeue
		return nil
	}

	// look for associated BMH
	host, helper, err := m.getHost(ctx)
	if err != nil {
		return err
	}

	// no BMH found, trying to choose from available ones
	if host == nil {
		host, helper, err = m.createHost(ctx)
		if err != nil {
			return err
		}
		m.Log.Info("Created host for association", "host", host.Name)
	} else {
		m.Log.Info("Machine already associated with host", "host", host.Name)
	}
	// A machine bootstrap not ready case is caught in the controller
	// ReconcileNormal function
	m.getUserDataSecretName(ctx)

	err = m.setHostLabel(ctx, host)
	if err != nil {
		return err
	}

	err = m.setHostConsumerRef(ctx, host)
	if err != nil {
		return err
	}

	// If the user did not provide a DataTemplate, we can directly set the host
	// specs, nothing to wait for.
	if m.Metal3Machine.Spec.DataTemplate == nil {
		if err = m.setHostSpec(ctx, host); err != nil {
			return err
		}
	}

	err = helper.Patch(ctx, host)
	if err != nil {
		var aggr kerrors.Aggregate
		if ok := errors.As(err, &aggr); ok {
			for _, kerr := range aggr.Errors() {
				if apierrors.IsConflict(kerr) {
					return WithTransientError(nil, requeueAfter)
				}
			}
		}
		return err
	}

	err = m.ensureAnnotation(ctx, host)
	if err != nil {
		return err
	}

	m.Log.Info("Finished associating machine")

	return nil
}

// Delete deletes a metal3 machine and is invoked by the Machine Controller.
// All the burden is on the Host implementation.
func (m *MachineHostManager) Delete(ctx context.Context) error {
	m.Log.Info("Deleting metal3 machine")
	host, helper, err := m.getHost(ctx)
	if err != nil {
		return err
	}
	if host == nil {
		m.Log.Info("host not found for metal3machine")
		if m.remoteClient != nil {
			m.remoteClient.Release(m.Metal3Machine)
		}
		return nil
	}
	var nodeReuse bool
	if host.Labels != nil {
		// The cluster being deleted, do not take nodeReuse into account.
		if m.Cluster == nil || !m.Cluster.DeletionTimestamp.IsZero() {
			delete(host.Labels, nodeReuseLabelName)
		}
		_, nodeReuse = host.Labels[nodeReuseLabelName]
	}
	if nodeReuse {
		// In nodeReuse mode, we do not delete the hostClaim but deprovision it
		// and let it available to other nodes. We only perform this once and
		// we check that The M3Machine still owns the hostClaim
		if consumerRefMatches(host.Spec.ConsumerRef, m.Metal3Machine) {
			m.Log.Info("Clearing hostClaim (node reuse mode)")
			host.Spec.Image = nil
			host.Spec.UserData = nil
			host.Spec.NetworkData = nil
			host.Spec.MetaData = nil
			host.Spec.Online = false
			if host.Annotations == nil {
				host.Annotations = map[string]string{}
			}
			host.Annotations[nodeReuseCleared] = time.Now().Format(time.RFC822)
			host.Spec.ConsumerRef = nil
			if err := helper.Patch(ctx, host); err != nil {
				m.Log.Error(err, "Failed to unbind hostClaim from Metal3Machine")
				return err
			}
			return nil
		}
		return nil
	}
	// The consumerRef is used as a barrier to check before hostclaim deletion.
	if host.Spec.ConsumerRef != nil {

		host.Spec.ConsumerRef = nil

		// Delete created secret, if data was set without DataSecretName
		if m.Machine.Spec.Bootstrap.DataSecretName == nil {
			m.Log.Info("Deleting User data secret for machine")
			if m.Metal3Machine.Status.UserData != nil {
				err = deleteSecret(ctx, m.client, m.Metal3Machine.Status.UserData.Name,
					m.Metal3Machine.Namespace,
				)
				if err != nil {
					return err
				}
			}
		}

		if err = helper.Patch(ctx, host); err != nil {
			return err
		}
	}

	hostClient := m.client
	if m.remoteClient != nil {
		hostClient = m.remoteClient
	}
	err = hostClient.Delete(ctx, host)
	if err != nil {
		m.Log.Error(err, "cannot delete associated host")
		return err
	}

	errMessage := fmt.Sprintf("Waiting for deletion of HostClaim associated to machine %s", m.Machine.Name)
	return WithTransientError(errors.New(errMessage), requeueAfter)
}

// Update updates a machine and is invoked by the Machine Controller.
func (m *MachineHostManager) Update(ctx context.Context) error {
	m.Log.Info("Updating machine")

	host, helper, err := m.getHost(ctx)
	if err != nil {
		return err
	}
	if host == nil {
		errMessage := fmt.Sprintf("HostClaim not found for machine %s", m.Machine.Name)
		return WithTransientError(errors.New(errMessage), requeueAfter)
	}

	if err := m.WaitForM3Metadata(ctx); err != nil {
		return err
	}

	err = m.setHostConsumerRef(ctx, host)
	if err != nil {
		return err
	}

	// ensure that the BMH specs are correctly set.
	err = m.setHostSpec(ctx, host)
	if err != nil {
		return err
	}

	err = helper.Patch(ctx, host)
	if err != nil {
		return err
	}

	err = m.ensureAnnotation(ctx, host)
	if err != nil {
		return err
	}

	if err := m.updateMachineStatus(ctx, host); err != nil {
		return err
	}

	m.Log.Info("Finished updating machine")
	return nil
}

// HasAnnotation makes sure the machine has an annotation that references a host.
func (m *MachineHostManager) HasAnnotation() bool {
	annotations := m.Metal3Machine.ObjectMeta.GetAnnotations()
	if annotations == nil {
		return false
	}
	_, ok := annotations[HostClaimAnnotation]
	return ok
}

// SetPauseAnnotation sets the pause annotations on associated bmh.
func (m *MachineHostManager) SetPauseAnnotation(ctx context.Context) error {
	// look for associated HostClaim
	host, helper, err := m.getHost(ctx)
	if err != nil {
		m.SetError("Failed to get a BaremetalHost for the Metal3Machine",
			capierrors.UpdateMachineError,
		)
		return err
	}
	if host == nil {
		return nil
	}

	annotations := host.GetAnnotations()

	if annotations != nil {
		if _, ok := annotations[bmov1alpha1.PausedAnnotation]; ok {
			m.Log.Info("Host is already paused")
			return nil
		}
	} else {
		host.Annotations = make(map[string]string)
	}
	m.Log.Info("Adding PausedAnnotation in Host")
	host.Annotations[bmov1alpha1.PausedAnnotation] = PausedAnnotationKey

	// We should not need to keep the status of host for pivot.
	// Proxy should maintain the synchronisation over status.
	return helper.Patch(ctx, host)
}

// RemovePauseAnnotation checks and/or Removes the pause annotations on associated bmh.
func (m *MachineHostManager) RemovePauseAnnotation(ctx context.Context) error {
	// Warning: same code as MachineManager.RemovePauseAnnotation but getHost is different !
	// do not use promotion even if pauseannotation was shared.

	// look for associated BMH
	host, helper, err := m.getHost(ctx)
	if err != nil {
		m.SetError("Failed to get a BaremetalHost for the Metal3Machine",
			capierrors.CreateMachineError,
		)
		return err
	}

	if host == nil {
		return nil
	}

	annotations := host.GetAnnotations()

	if annotations != nil {
		if _, ok := annotations[bmov1alpha1.PausedAnnotation]; ok {
			if m.Cluster.Name == host.Labels[clusterv1.ClusterNameLabel] && annotations[bmov1alpha1.PausedAnnotation] == PausedAnnotationKey {
				// Removing BMH Paused Annotation Since Owner Cluster is not paused.
				delete(host.Annotations, bmov1alpha1.PausedAnnotation)
			} else if m.Cluster.Name == host.Labels[clusterv1.ClusterNameLabel] && annotations[bmov1alpha1.PausedAnnotation] != PausedAnnotationKey {
				m.Log.Info("BMH is paused by user. Not removing Pause Annotation")
				return nil
			}
		}
	}
	return helper.Patch(ctx, host)
}

// getHost gets the associated host by looking for an annotation on the machine
// that contains a reference to the host. Returns nil if not found. Assumes the
// host is in the same namespace as the machine.
func (m *MachineHostManager) getHost(ctx context.Context) (*bmov1alpha1.HostClaim, *patch.Helper, error) {
	hostClient := m.client
	namespace := m.Metal3Machine.Namespace
	if m.remoteNamespace != "" {
		namespace = m.remoteNamespace
	}
	if m.remoteClient != nil {
		hostClient = m.remoteClient
	}
	host, err := getHostClaim(ctx, m.Metal3Machine, hostClient, namespace, m.Log)
	if err != nil || host == nil {
		return host, nil, err
	}
	helper, err := patch.NewHelper(host, hostClient)
	return host, helper, err
}

// Simplified annotation: we do not store the namespace as everything is done
// to keep Host and m3machine in a controlled namespace.
func getHostClaim(
	ctx context.Context, m3Machine *infrav1.Metal3Machine, cl client.Client,
	namespace string, mLog logr.Logger,
) (*bmov1alpha1.HostClaim, error) {
	annotations := m3Machine.ObjectMeta.GetAnnotations()
	if annotations == nil {
		return nil, nil
	}
	hostName, ok := annotations[HostClaimAnnotation]
	if !ok {
		return nil, nil
	}
	host := bmov1alpha1.HostClaim{}
	key := client.ObjectKey{
		Name:      hostName,
		Namespace: namespace,
	}
	err := cl.Get(ctx, key, &host)
	if apierrors.IsNotFound(err) {
		mLog.Info("Annotated host not found", "hostName", hostName, "hostNamespace", namespace)
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &host, nil
}

// setHostLabel will set the set cluster.x-k8s.io/cluster-name to bmh.
func (m *MachineHostManager) setHostLabel(_ context.Context, host *bmov1alpha1.HostClaim) error {
	if host.Labels == nil {
		host.Labels = make(map[string]string)
	}
	host.Labels[clusterv1.ClusterNameLabel] = m.Machine.Spec.ClusterName

	return nil
}

// Synchronize the secrets from the m3machine namespace to the hostclaim
// namespace. Original secrets are defined by a reference.
// Names are derived from the hostClaim name with a suffix (purpose).
// the function gives back a reference to the new secret or an error.
func (m *MachineHostManager) syncSecretData(
	ctx context.Context, source *corev1.SecretReference,
	host *bmov1alpha1.HostClaim, purpose string,
) (*corev1.SecretReference, error) {
	log := m.Log.WithValues("purpose", purpose)
	if source == nil {
		return nil, nil
	}
	target := corev1.SecretReference{}
	// local HostClaim, we just push the reference and fix the namespace if needed.
	if m.remoteClient == nil {
		target = *source
		if target.Namespace == "" {
			target.Namespace = m.Metal3Machine.Namespace
		}
	} else {
		target.Namespace = m.remoteNamespace
		target.Name = host.Name + "-" + purpose
		localNamespace := source.Namespace
		if localNamespace == "" {
			localNamespace = m.Metal3Machine.Namespace
		}
		localSecretKey := client.ObjectKey{
			Name:      source.Name,
			Namespace: localNamespace,
		}
		localSecret := corev1.Secret{}
		err := m.client.Get(ctx, localSecretKey, &localSecret)
		if err != nil {
			log.Error(err, "cannot get local secret", "secretName", source.Name, "secretNamespace", localNamespace)
			return nil, err
		}
		remoteSecret := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      target.Name,
				Namespace: target.Namespace,
			},
		}
		_, err = controllerutil.CreateOrUpdate(ctx, m.remoteClient, &remoteSecret, func() error {
			if remoteSecret.Data == nil {
				remoteSecret.Data = map[string][]byte{}
			}
			maps.Copy(remoteSecret.Data, localSecret.Data)
			return nil
		})
		if err != nil {
			log.Error(err, "cannot update remote secret", "secretName", target.Name, "secretNamespace", target.Namespace)
			return nil, err
		}
	}
	return &target, nil
}

// setHostSpec will ensure the host's Spec is set according to the machine's
// details. It will then update the host via the kube API. If UserData does not
// include a Namespace, it will default to the Metal3Machine's namespace.
func (m *MachineHostManager) setHostSpec(ctx context.Context, host *bmov1alpha1.HostClaim) error {
	host.Spec.Online = false

	if m.Metal3Machine.Status.UserData == nil || m.Metal3Machine.Status.MetaData == nil {
		return nil
	}

	ref, err := m.syncSecretData(ctx, m.Metal3Machine.Status.UserData, host, "userdata")
	if err != nil {
		return err
	}
	host.Spec.UserData = ref

	ref, err = m.syncSecretData(ctx, m.Metal3Machine.Status.MetaData, host, "metadata")
	if err != nil {
		return err
	}
	host.Spec.MetaData = ref

	ref, err = m.syncSecretData(ctx, m.Metal3Machine.Status.NetworkData, host, "networkdata")
	if err != nil {
		return err
	}
	host.Spec.NetworkData = ref

	if m.Metal3Machine.Spec.AutomatedCleaningMode != nil {
		cleaningMode := bmov1alpha1.AutomatedCleaningMode(*m.Metal3Machine.Spec.AutomatedCleaningMode)
		host.Spec.AutomatedCleaningMode = &cleaningMode
	} else {
		host.Spec.AutomatedCleaningMode = nil
	}
	host.Spec.Online = true

	return nil
}

// ensureAnnotation makes sure the machine has an annotation that references the
// host and uses the API to update the machine if necessary.
func (m *MachineHostManager) ensureAnnotation(_ context.Context, host *bmov1alpha1.HostClaim) error {
	annotations := m.Metal3Machine.ObjectMeta.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	hostKey := host.Name
	existing, ok := annotations[HostClaimAnnotation]
	if ok {
		if existing == hostKey {
			return nil
		}
		m.Log.Info("Warning: found stray annotation for host on machine. Overwriting.", "host", existing)
	}
	annotations[HostClaimAnnotation] = hostKey
	m.Metal3Machine.ObjectMeta.SetAnnotations(annotations)

	return nil
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

// chooseHost iterates through known hosts and returns one that can be
// associated with the metal3 machine.
//
// It searches all hosts in case one already has an association with this
// metal3 machine. createHost finds/creates a suitable HostClaim for the metal3
// machine. If we are in the nodeReuse case, we first check there is not an
// available unbound hostClaim with the right label. If it fails or if it is
// not in node reuse mode, a new hostClaim is created with the name of the
// metal3 machine.
func (m *MachineHostManager) createHost(ctx context.Context) (*bmov1alpha1.HostClaim, *patch.Helper, error) {
	namespace := m.Metal3Machine.Namespace
	hostClient := m.client
	if m.remoteClient != nil {
		hostClient = m.remoteClient
		namespace = m.remoteNamespace
	}

	checksumType := ""
	image := bmov1alpha1.Image{
		URL:          m.Metal3Machine.Spec.Image.URL,
		Checksum:     m.Metal3Machine.Spec.Image.Checksum,
		ChecksumType: bmov1alpha1.ChecksumType(checksumType),
		DiskFormat:   m.Metal3Machine.Spec.Image.DiskFormat,
	}
	if m.Metal3Machine.Spec.Image.ChecksumType != nil {
		checksumType = *m.Metal3Machine.Spec.Image.ChecksumType
	}

	matchLabels := maps.Clone(m.Metal3Machine.Spec.HostSelector.MatchLabels)
	selector := bmov1alpha1.HostSelector{
		MatchLabels:      matchLabels,
		MatchExpressions: translateHostRequirements(m.Metal3Machine.Spec.HostSelector.MatchExpressions),
		InNamespace:      *m.Metal3Machine.Spec.HostSelector.InNamespace,
	}
	nodeReuseValue, err := m.nodeReuseValue(ctx)
	if err != nil {
		m.Log.Error(err, "Error during computation of nodeReuseLabel value")
		return nil, nil, err
	}
	var host *bmov1alpha1.HostClaim

	if nodeReuseValue != nil {
		var futureCandidate bool
		reusableHostclaims := bmov1alpha1.HostClaimList{}
		if err := hostClient.List(ctx, &reusableHostclaims, client.InNamespace(namespace), client.MatchingLabels{nodeReuseLabelName: *nodeReuseValue}); err != nil {
			m.Log.Error(err, "Error listing reusable nodes")
			return nil, nil, err
		}
		for _, candidate := range reusableHostclaims.Items {
			if candidate.Spec.ConsumerRef == nil {
				if conditions.IsTrue(&candidate, bmov1alpha1.AvailableCondition) {
					// This may be a reasonable choice
					host = &candidate
				} else {
					futureCandidate = true
				}
			} else {
				if consumerRefMatches(candidate.Spec.ConsumerRef, m.Metal3Machine) {
					// Already bound to us (conflict on update). This is the choice to use
					host = &candidate
					break
				}
			}
		}
		if host == nil && futureCandidate {
			m.Log.Info("Waiting for a candidate reusable node")
			return nil, nil, WithTransientError(nil, requeueAfter)
		}
	}
	if host != nil {
		// We found a node to reuse and we update it
		m.setHostConsumerRef(ctx, host)
		if host.Annotations != nil {
			delete(host.Annotations, nodeReuseCleared)
		}
		host.Spec.Image = &image
		host.Spec.HostSelector = selector
		err = hostClient.Update(ctx, host)
		if err != nil {
			m.Log.Error(err, "Cannot bind to a reusable host")
			return nil, nil, err
		}
	} else {
		// We must create a new hostClaim either because we are not in node
		// reuse mode or we did not find a node to reuse.
		host = &bmov1alpha1.HostClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      m.Metal3Machine.Name,
				Namespace: namespace,
			},
			Spec: bmov1alpha1.HostClaimSpec{
				HostSelector: selector,
				Image:        &image,
				Online:       false,
			},
		}
		m.setHostConsumerRef(ctx, host)
		if nodeReuseValue != nil {
			host.Labels = map[string]string{
				nodeReuseLabelName: *nodeReuseValue,
			}
		}

		err = hostClient.Create(ctx, host)
		if err != nil {
			m.Log.Error(err, "Cannot create an associated host")
			return nil, nil, err
		}
	}
	helper, err := patch.NewHelper(host, hostClient)
	return host, helper, err
}

// NodeAddresses returns a slice of corev1.NodeAddress objects for a
// given Metal3 machine.
func hardwareDataAddresses(hwdata *bmov1alpha1.HardwareData) []clusterv1beta1.MachineAddress {
	addrs := []clusterv1beta1.MachineAddress{}

	// If the host is nil or we have no hw details, return an empty address array.
	if hwdata == nil || hwdata.Spec.HardwareDetails == nil {
		return addrs
	}

	for _, nic := range hwdata.Spec.HardwareDetails.NIC {
		if nic.IP == "" {
			continue
		}
		address := clusterv1beta1.MachineAddress{
			Type:    clusterv1beta1.MachineInternalIP,
			Address: nic.IP,
		}
		addrs = append(addrs, address)
	}

	if hwdata.Spec.HardwareDetails.Hostname != "" {
		addrs = append(addrs, clusterv1beta1.MachineAddress{
			Type:    clusterv1beta1.MachineHostName,
			Address: hwdata.Spec.HardwareDetails.Hostname,
		})
		addrs = append(addrs, clusterv1beta1.MachineAddress{
			Type:    clusterv1beta1.MachineInternalDNS,
			Address: hwdata.Spec.HardwareDetails.Hostname,
		})
	}

	return addrs
}

// updateMachineStatus updates a Metal3Machine object's status.
func (m *MachineHostManager) updateMachineStatus(ctx context.Context, host *bmov1alpha1.HostClaim) error {
	if host.Status.HardwareData == nil {
		err := fmt.Errorf("no hardware data")
		m.Log.Error(err, "No hardware data linked on host (should not occur)")
		return err
	}
	cl := m.client
	remoteClient, _, err := getRemoteClient(m.Log, m.client, m.Metal3Machine)
	if err != nil {
		m.Log.Error(err, "Cannot get client on remote host provisionner cluster")
		return err
	}
	if remoteClient != nil {
		cl = remoteClient
	}

	hardwareData := &bmov1alpha1.HardwareData{}
	key := client.ObjectKey{Namespace: host.Status.HardwareData.Namespace, Name: host.Status.HardwareData.Name}
	if err := cl.Get(ctx, key, hardwareData); err != nil {
		m.Log.Error(err, "cannot access hardwareData")
	}

	addrs := hardwareDataAddresses(hardwareData)

	metal3MachineOld := m.Metal3Machine.DeepCopy()

	m.Metal3Machine.Status.Addresses = addrs
	m.SetConditionMetal3MachineToTrue(infrav1.AssociateBMHCondition)

	if equality.Semantic.DeepEqual(m.Metal3Machine.Status, metal3MachineOld.Status) {
		// Status did not change
		return nil
	}

	now := metav1.Now()
	m.Metal3Machine.Status.LastUpdated = &now
	return nil
}

// setHostConsumerRef will ensure the host's Spec is set to link to this
// Metal3Machine.
func (m *MachineHostManager) setHostConsumerRef(_ context.Context, host *bmov1alpha1.HostClaim) error {
	kind := metal3MachineKind
	if m.remoteClient != nil {
		kind = remoteMetal3MachineKind
	}
	host.Spec.ConsumerRef = &corev1.ObjectReference{
		Kind:       kind,
		Name:       m.Metal3Machine.Name,
		Namespace:  m.Metal3Machine.Namespace,
		APIVersion: m.Metal3Machine.APIVersion,
	}

	// Set OwnerReferences if not remote. Mainly used for pivoting the hostclaims.
	// We put it on the cluster rather than the metal3machine.
	// Deletion is handled separately especially if node-reuse is enabled.
	if m.remoteClient == nil {
		controllerutil.SetOwnerReference(m.Cluster, host, m.client.Scheme())
	}

	return nil
}

func (m *MachineHostManager) nodeReuseValue(ctx context.Context) (*string, error) {
	// If cluster has DeletionTimestamp set, skip checking if nodeReuse
	// feature is enabled.
	if m.Cluster == nil || !m.Cluster.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	// Fetch corresponding Metal3MachineTemplate, to see if nodeReuse
	// feature is enabled. If set to true, check the machine role. In case
	// machine role is ControlPlane, set nodeReuseLabelName to ControlPlane
	// name, otherwise to MachineDeployment name.
	m.Log.Info("Getting Metal3MachineTemplate")
	m3mt := &infrav1.Metal3MachineTemplate{}
	if m.Metal3Machine == nil {
		return nil, errors.New("Metal3Machine associated with Metal3MachineTemplate is not found")
	}
	if m.hasTemplateAnnotation() {
		m3mtKey := client.ObjectKey{
			Name:      m.Metal3Machine.Annotations[clusterv1.TemplateClonedFromNameAnnotation],
			Namespace: m.Metal3Machine.Namespace,
		}
		if err := m.client.Get(ctx, m3mtKey, m3mt); err != nil {
			// we are here, because while normal deprovisioning, Metal3MachineTemplate will be deleted first
			// and we can't get it even though Metal3Machine has reference to it. We consider it nil and move
			// forward with normal deprovisioning.
			m3mt = nil
			m.Log.Info("Metal3MachineTemplate associated with Metal3Machine is deleted")
		} else {
			// in case of upgrading, Metal3MachineTemplate will not be deleted and we can fetch it,
			// in order to check for node reuse feature in the next step.
			m.Log.Info("Found Metal3machineTemplate", "metal3machineTemplate", m3mtKey.Name)
		}
	}
	if m3mt != nil && m3mt.Spec.NodeReuse {
		// Check if machine is ControlPlane
		if m.isControlPlane() {
			// Fetch ControlPlane name for controlplane machine
			cpName, err := m.getControlPlaneName(ctx)
			if err != nil {
				return nil, err
			}
			// Set nodeReuseLabelName on the host to ControlPlane name
			return &cpName, nil
		}
		// Fetch MachineDeployment name for worker machine
		mdName, err := m.getMachineDeploymentName(ctx)
		if err != nil {
			return nil, err
		}
		// Set nodeReuseLabelName on the host to MachineDeployment name
		return &mdName, nil
	}
	return nil, nil
}

func (m *MachineHostManager) IsBaremetalHostProvisioned(ctx context.Context) bool {
	m.Log.Info("checking if baremetalhost is provisioned")
	host, _, err := m.getHost(ctx)
	if err != nil {
		m.Log.Info("failed to get host", "err", err)
		return false
	}
	if host == nil {
		m.Log.Info("getHost returned nil")
		return false
	}
	cond := conditions.Get(host, clusterv1beta1.ReadyV1Beta2Condition)
	m.Log.V(1).Info("Condition Ready on Host", "condition", cond)
	return cond != nil && cond.Status == metav1.ConditionTrue
}

// getBmhUIDFromM3Machine retrieves bmhUID from m3m.
func (m *MachineHostManager) GetBmhUIDFromM3Machine(ctx context.Context) (string, error) {
	host, _, err := m.getHost(ctx)
	if err != nil || host == nil {
		errMessage := "Failed to get a HostClaim for the metal3machine: " + m.Metal3Machine.GetName()
		return "", errors.New(errMessage)
	}
	if host.Status.HostUID == "" {
		return "", errors.New("Missing BaremetalHost UID in HostClaim")
	}
	return string(host.Status.HostUID), nil
}

// getBmhNameFromM3Machine retrieves bmhName from m3m annotations.
func (m *MachineHostManager) GetBmhNameFromM3Machine(ctx context.Context) (string, error) {
	host, _, err := m.getHost(ctx)
	if err != nil || host == nil {
		errMessage := "Failed to get a HostClaim for the metal3machine: " + m.Metal3Machine.GetName()
		return "", errors.New(errMessage)
	}
	if host.Status.HardwareData == nil {
		errMessage := "BareMetalHost not linked to HostClaim for Metal3Machine " + m.Metal3Machine.GetName()
		return "", errors.New(errMessage)
	}
	return host.Status.HardwareData.Name, nil
}
