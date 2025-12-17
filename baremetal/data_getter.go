package baremetal

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The DataGetter interface abstracts a BareMetalHost or a HostClaim so that they
// can both be used as source for rendering Metal3DataTemplates.
type DataGetter interface {
	// Gives back the name of the underlying BareMetalHost.
	GetName() string
	// Gives back the name of the HostClaim (or BareMetalHost if no HostClaim).
	GetHostName() string
	// Returns the value of a label on either the BareMetalHost or the HardwareData when used
	// with a HostClaim.
	GetLabel(label string) (string, error)
	// Returns the value of an annotation on either the BareMetalHost or the HardwareData when used
	// with a HostClaim.
	GetAnnotation(label string) (string, error)
	// Underlying configuration of Network cards.
	GetNICs() map[string]string
}

// BMHDataGetter implements DataGetter for an underlying BareMetalHost.
type BMHDataGetter struct {
	bmh *bmov1alpha1.BareMetalHost
}

// BMHDataGetter implements DataGetter for an underlying HostClaim.
type HostDataGetter struct {
	hostclaimName string
	hardwareData  bmov1alpha1.HardwareData
}

// getDataHost creates a DataGetter for the Metal3Machine. It needs a client to
// access the BareMetalHost or the HostClaim. It supports remote HardwareData.
func getDataHost(
	ctx context.Context, m3Machine *infrav1.Metal3Machine,
	cl client.Client, mLog logr.Logger,
) (DataGetter, error) {
	annotations := m3Machine.ObjectMeta.GetAnnotations()
	if annotations == nil {
		return nil, nil
	}
	bmhKey, ok := annotations[HostAnnotation]
	if ok {
		hostNamespace, hostName, err := cache.SplitMetaNamespaceKey(bmhKey)
		if err != nil {
			mLog.Error(err, "Error parsing annotation value", "annotation key", bmhKey)
			return nil, err
		}
		bmh := &bmov1alpha1.BareMetalHost{}
		dataGetter := BMHDataGetter{bmh}
		key := client.ObjectKey{
			Name:      hostName,
			Namespace: hostNamespace,
		}
		err = cl.Get(ctx, key, dataGetter.bmh)
		if apierrors.IsNotFound(err) {
			mLog.Info("Annotated host not found", "host", bmhKey)
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		return &dataGetter, nil
	}
	hostName, ok := annotations[HostClaimAnnotation]

	if !ok {
		return nil, nil
	}
	namespace := m3Machine.Namespace
	remoteClient, remoteNamespace, err := getRemoteClient(mLog, cl, m3Machine)
	if err != nil {
		mLog.Error(err, "Cannot get client on remote host provisionner cluster")
		return nil, err
	}
	if remoteClient != nil {
		cl = remoteClient
		namespace = remoteNamespace
	}

	hostclaim := &bmov1alpha1.HostClaim{}
	dataGetter := HostDataGetter{}
	key := client.ObjectKey{
		Name:      hostName,
		Namespace: namespace,
	}
	err = cl.Get(ctx, key, hostclaim)
	if apierrors.IsNotFound(err) {
		mLog.Info("Annotated host not found", "host", hostName)
		return nil, nil
	} else if err != nil {
		mLog.Error(err, "Error while getting host", "host", hostName)
		return nil, err
	}
	hwDataRef := hostclaim.Status.HardwareData
	if hwDataRef == nil {
		mLog.Info("Host found but not synced", "host", hostName)
		return nil, nil
	}
	dataGetter.hostclaimName = hostclaim.Name
	key = client.ObjectKey{Namespace: hwDataRef.Namespace, Name: hwDataRef.Name}
	if err = cl.Get(ctx, key, &dataGetter.hardwareData); err != nil {
		mLog.Error(err, "Referenced hardware data not accessible")
		return nil, err
	}
	return &dataGetter, nil
}

func (dg *BMHDataGetter) GetName() string {
	return dg.bmh.Name
}

func (dg *BMHDataGetter) GetHostName() string {
	return dg.bmh.Name
}

func (dg *BMHDataGetter) GetLabel(key string) (string, error) {
	if dg.bmh == nil {
		return "", fmt.Errorf("baremetalhost is nil but referenced in label %s", key)
	}
	v, ok := dg.bmh.Labels[key]
	if ok {
		return v, nil
	}
	return "", fmt.Errorf("label %s not found or empty", key)
}

func (dg *BMHDataGetter) GetAnnotation(key string) (string, error) {
	if dg.bmh == nil {
		return "", fmt.Errorf("baremetalhost is nil but referenced in annotation %s", key)
	}
	v, ok := dg.bmh.Annotations[key]
	if ok {
		return v, nil
	}
	return "", fmt.Errorf("annotation %s not found or empty", key)
}

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

func (dg *HostDataGetter) GetName() string {
	return dg.hardwareData.Name
}

func (dg *HostDataGetter) GetHostName() string {
	return dg.hostclaimName
}

func (dg *HostDataGetter) GetLabel(key string) (string, error) {
	v, ok := dg.hardwareData.Labels[key]
	if ok {
		return v, nil
	}
	return "", fmt.Errorf("unknown label %s", key)
}

func (dg *HostDataGetter) GetAnnotation(key string) (string, error) {
	v, ok := dg.hardwareData.Annotations[key]
	if ok {
		return v, nil
	}
	return "", fmt.Errorf("unknown annotation %s", key)
}

func (dg *HostDataGetter) GetNICs() map[string]string {
	result := map[string]string{}
	if dg.hardwareData.Spec.HardwareDetails != nil {
		for _, nic := range dg.hardwareData.Spec.HardwareDetails.NIC {
			result[nic.Name] = nic.MAC
		}
	}
	return result
}
