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

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// getHostClaim gets the associated host by looking for an annotation on the machine
// that contains a reference to the host. Returns nil if not found. Assumes the
// host is in the same namespace as the machine.
func (m *MachineHostClaimManager) getHostClaim(ctx context.Context) (*bmov1alpha1.HostClaim, *patch.Helper, error) { //nolint:unparam
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
	hostName, ok := annotations[HostClaimAnnotation]
	if !ok {
		return nil, nil //nolint:nilnil
	}
	hostclaim := bmov1alpha1.HostClaim{}
	key := client.ObjectKey{
		Name:      hostName,
		Namespace: namespace,
	}
	err := cl.Get(ctx, key, &hostclaim)
	if apierrors.IsNotFound(err) {
		mLog.Info("Annotated hostclaim not found", LogFieldHostClaim, hostName, LogFieldHostClaimNamespace, namespace)
		return nil, nil //nolint:nilnil
	} else if err != nil {
		return nil, err
	}
	return &hostclaim, nil
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
