/*
Copyright 2026 The Kubernetes Authors.

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

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The DataGetter interface abstracts a BareMetalHost or a HostClaim so that they
// can both be used as source for rendering Metal3DataTemplates.
type DataGetter interface {
	// Gives back the name of the underlying BareMetalHost.
	GetName() (string, error)
	// Gives back the name of the HostClaim (or BareMetalHost if not HostClaim mode).
	GetHostName() (string, error)
	// Returns the value of a label on either the BareMetalHost or the HardwareData when used
	// with a HostClaim.
	GetLabel(label string) (string, error)
	// Returns the value of an annotation on either the BareMetalHost or the HardwareData when used
	// with a HostClaim.
	GetAnnotation(annot string) (string, error)
	// Returns the value of a label on the HostClaim. If used without HostClaim, returns
	// an error.
	GetHostClaimLabel(label string) (string, error)
	// Returns the value of an annotation on the HostClaim. If used without HostClaim, returns
	// an error.
	GetHostClaimAnnotation(annot string) (string, error)
	// Underlying configuration of Network cards.
	GetNICs() map[string]string
	// GetBootMACAddress returns the BootMacAddress on a directly bound BareMetalHost, "" otherwise
	GetBootMACAddress() string
}

// BMHDataGetter implements DataGetter for an underlying BareMetalHost.
type BMHDataGetter struct {
	bmh *bmov1alpha1.BareMetalHost
}

// HostClaimDataGetter implements DataGetter for an underlying HostClaim.
type HostClaimDataGetter struct {
	hostclaim    *bmov1alpha1.HostClaim
	hardwareData *bmov1alpha1.HardwareData
}

// NewDataGetter creates a DataGetter for the Metal3Machine. It needs a client to
// access the BareMetalHost or the HostClaim. It supports remote HardwareData.
func NewDataGetter(
	ctx context.Context, rcc RemoteClientCacheInterface,
	m3Machine *infrav1.Metal3Machine,
	cl client.Client, mLog logr.Logger,
) (DataGetter, error) {
	annotations := m3Machine.ObjectMeta.GetAnnotations()
	if annotations == nil {
		return nil, nil //nolint:nilnil
	}
	bmhKey, ok := annotations[HostAnnotation]
	if ok {
		_, hostName, err := cache.SplitMetaNamespaceKey(bmhKey)
		if err != nil {
			mLog.Error(err, "Error parsing annotation value", "annotation key", bmhKey)
			return nil, err
		}
		bmh := &bmov1alpha1.BareMetalHost{}
		dataGetter := BMHDataGetter{bmh}
		key := client.ObjectKey{
			Name:      hostName,
			Namespace: m3Machine.Namespace, // security: ignore namespace in annotation.
		}
		err = cl.Get(ctx, key, dataGetter.bmh)
		if apierrors.IsNotFound(err) {
			mLog.Info("Annotated host not found", "host", bmhKey)
			return nil, nil //nolint:nilnil
		} else if err != nil {
			return nil, err
		}
		return &dataGetter, nil
	}
	hostName, ok := annotations[HostClaimAnnotation]

	if !ok {
		return nil, nil //nolint:nilnil
	}
	namespace := m3Machine.Namespace
	if rcc == nil {
		return nil, errors.New("remote client cache necessary for HostClaim mode")
	}
	remoteClient, err := rcc.GetRemoteClient(ctx, mLog, m3Machine)
	if err != nil {
		mLog.Error(err, "Cannot get client on remote cluster")
		return nil, err
	}
	if remoteClient != nil {
		cl = remoteClient
		namespace = remoteClient.Namespace
	}

	hostclaim := &bmov1alpha1.HostClaim{}
	dataGetter := HostClaimDataGetter{}
	key := client.ObjectKey{
		Name:      hostName,
		Namespace: namespace,
	}
	err = cl.Get(ctx, key, hostclaim)
	if apierrors.IsNotFound(err) {
		mLog.Info("Annotated host not found", "host", hostName)
		return nil, nil //nolint:nilnil
	} else if err != nil {
		mLog.Error(err, "Error while getting host", "host", hostName)
		return nil, err
	}
	hwDataRef := hostclaim.Status.HardwareData
	if hwDataRef == nil {
		mLog.Info("Host found but not synced", "host", hostName)
		return nil, nil //nolint:nilnil
	}
	dataGetter.hostclaim = hostclaim
	key = client.ObjectKey{Namespace: hwDataRef.Namespace, Name: hwDataRef.Name}
	dataGetter.hardwareData = &bmov1alpha1.HardwareData{}
	if err = cl.Get(ctx, key, dataGetter.hardwareData); err != nil {
		mLog.Error(err, "Referenced hardware data not accessible")
		return nil, err
	}
	return &dataGetter, nil
}

// GetName gives back the name of the BMH.
func (dg *BMHDataGetter) GetName() (string, error) {
	if dg.bmh == nil {
		return "", errors.New("baremetalhost is nil")
	}
	return dg.bmh.Name, nil
}

// GetHostName gives back the name of the HostClaim (falls back to BMH name in direct BMH mode).
func (dg *BMHDataGetter) GetHostName() (string, error) {
	return dg.GetName()
}

// GetHostClaimLabel gives back an error: there is no HostClaim in direct BMH mode.
func (dg *BMHDataGetter) GetHostClaimLabel(_ string) (string, error) {
	return "", errors.New("no hostclaim")
}

// GetLabel gives back a label on the BareMetalHost.
func (dg *BMHDataGetter) GetLabel(key string) (string, error) {
	if dg.bmh == nil {
		return "", fmt.Errorf("baremetalhost is nil but referenced in label %s", key)
	}
	return dg.bmh.Labels[key], nil
}

// GetHostClaimAnnotation gives back an error: there is no HostClaim in direct BMH mode.
func (dg *BMHDataGetter) GetHostClaimAnnotation(_ string) (string, error) {
	return "", errors.New("no hostclaim")
}

// GetAnnotation gives back an annotation on the BareMetalHost.
func (dg *BMHDataGetter) GetAnnotation(key string) (string, error) {
	if dg.bmh == nil {
		return "", fmt.Errorf("baremetalhost is nil but referenced in annotation %s", key)
	}
	return dg.bmh.Annotations[key], nil
}

// GetNICs gives back the names and mac of NICs associated to a BMH.
// Note: It expects HardwareDetails in BMH and should transition to using HardwareData at some point.
func (dg *BMHDataGetter) GetNICs() map[string]string {
	if dg.bmh == nil || dg.bmh.Status.HardwareDetails == nil || dg.bmh.Status.HardwareDetails.NIC == nil {
		return nil
	}
	result := map[string]string{}
	for _, nic := range dg.bmh.Status.HardwareDetails.NIC {
		result[nic.Name] = nic.MAC
	}
	return result
}

// GetBootMACAddress gets the boot mac address of the underlying BMH.
func (dg *BMHDataGetter) GetBootMACAddress() string {
	if dg.bmh == nil {
		return ""
	}
	return dg.bmh.Spec.BootMACAddress
}

// GetName gets the name of the BMH associated to the underlying hostclaim.
func (dg *HostClaimDataGetter) GetName() (string, error) {
	if dg.hostclaim == nil {
		return "", errors.New("bmh name not accessible: hostclaim is nil")
	}
	if dg.hostclaim.Status.BareMetalHost == nil {
		return "", WithTransientError(errors.New("bmh name not accessible: hostclaim not associated"), requeueAfter)
	}
	return dg.hostclaim.Status.BareMetalHost.Name, nil
}

// GetHostName gets the name of the underlying hostclaim.
func (dg *HostClaimDataGetter) GetHostName() (string, error) {
	if dg.hostclaim == nil {
		return "", errors.New("hostclaim is nil")
	}
	return dg.hostclaim.Name, nil
}

// GetLabel gets a label from the hardwaredata associated to the underlying hostclaim.
// HardwareData expose public BMH metadata.
func (dg *HostClaimDataGetter) GetLabel(key string) (string, error) {
	if dg.hardwareData == nil {
		return "", fmt.Errorf("hardwaredata is nil but necessary to get label %s", key)
	}
	v, ok := dg.hardwareData.Labels[key]
	if ok {
		return v, nil
	}
	return "", fmt.Errorf("unknown label %s", key)
}

// GetHostClaimLabel gets a label from the underlying hostclaim.
func (dg *HostClaimDataGetter) GetHostClaimLabel(key string) (string, error) {
	if dg.hostclaim == nil {
		return "", fmt.Errorf("hostclaim is nil but necessary to get label %s", key)
	}
	if dg.hostclaim.Labels != nil {
		v, ok := dg.hostclaim.Labels[key]
		if ok {
			return v, nil
		}
	}
	return "", fmt.Errorf("unknown label %s", key)
}

// GetAnnotation gets an annotation from the hardwaredata associated to the underlying hostclaim.
// HardwareData expose public BMH metadata.
func (dg *HostClaimDataGetter) GetAnnotation(key string) (string, error) {
	if dg.hardwareData == nil {
		return "", fmt.Errorf("hardwaredata is nil but necessary to get annotation %s", key)
	}
	v, ok := dg.hardwareData.Annotations[key]
	if ok {
		return v, nil
	}
	return "", fmt.Errorf("unknown annotation %s", key)
}

// GetHostClaimAnnotation gets an annotation from the underlying hostclaim.
func (dg *HostClaimDataGetter) GetHostClaimAnnotation(key string) (string, error) {
	if dg.hostclaim == nil {
		return "", fmt.Errorf("hostclaim is nil but necessary to get annotation %s", key)
	}
	if dg.hostclaim.Annotations != nil {
		v, ok := dg.hostclaim.Annotations[key]
		if ok {
			return v, nil
		}
	}
	return "", fmt.Errorf("unknown annotation %s", key)
}

// GetNICs gives back the names and mac of NICs described in underlying hardwaredata.
func (dg *HostClaimDataGetter) GetNICs() map[string]string {
	result := map[string]string{}
	if dg.hardwareData != nil && dg.hardwareData.Spec.HardwareDetails != nil {
		for _, nic := range dg.hardwareData.Spec.HardwareDetails.NIC {
			result[nic.Name] = nic.MAC
		}
	}
	return result
}

// GetBootMacAddress does not give a relevant mac address in HostClaim mode.
func (dg *HostClaimDataGetter) GetBootMACAddress() string {
	return ""
}
