/*
Copyright 2025 The Kubernetes Authors.

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
	"log"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1beta1 "sigs.k8s.io/cluster-api/api/core/v1beta1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

type MockRemoteClientCache struct{}

func (m MockRemoteClientCache) GetRemoteClient(log logr.Logger, metal3Machine *infrav1.Metal3Machine) (*RemoteClient, error) {
	return nil, nil
}
func (m MockRemoteClientCache) GetWatchChannel() chan event.GenericEvent {
	return nil
}

var mockRemoteClientCache RemoteClientCacheInterface = MockRemoteClientCache{}

func emptyM3MSpec() *infrav1.Metal3MachineSpec {
	return &infrav1.Metal3MachineSpec{}
}

func withInNamespace(spec *infrav1.Metal3MachineSpec) *infrav1.Metal3MachineSpec {
	var from = ""
	spec.HostSelector.InNamespace = &from
	return spec
}

func withDataTemplate(spec *infrav1.Metal3MachineSpec) *infrav1.Metal3MachineSpec {
	spec.DataTemplate = &corev1.ObjectReference{
		Name:      "abcd",
		Namespace: namespaceName,
	}
	return spec
}

var _ = Describe("Metal3Machine HostClaim HostClaim manager", func() {
	type testCaseAssociate struct {
		Machine            *clusterv1.Machine
		Host               *bmov1alpha1.HostClaim
		M3Machine          *infrav1.Metal3Machine
		DataTemplate       *infrav1.Metal3DataTemplate
		Data               *infrav1.Metal3Data
		ExpectRequeue      bool
		ExpectClusterLabel bool
		ExpectOwnerRef     bool
	}

	DescribeTable("Test Associate function",
		func(tc testCaseAssociate) {
			objects := []client.Object{
				tc.M3Machine,
				tc.Machine,
			}
			if tc.Host != nil {
				objects = append(objects, tc.Host)
			}
			if tc.DataTemplate != nil {
				objects = append(objects, tc.DataTemplate)
			}
			if tc.Data != nil {
				objects = append(objects, tc.Data)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupSchemeMm()).WithObjects(objects...).Build()

			machineMgr, err := NewMachineHostManager(fakeClient, mockRemoteClientCache, nil, nil, tc.Machine,
				tc.M3Machine, GinkgoLogr,
			)
			Expect(err).NotTo(HaveOccurred())

			err = machineMgr.Associate(context.TODO())
			if tc.ExpectRequeue {
				var reconcileError ReconcileError
				ok := errors.As(err, &reconcileError)
				log.Println(errors.Cause(err))
				Expect(ok).To(BeTrue())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}

			// get the saved host
			hostClaim := bmov1alpha1.HostClaim{}
			err = fakeClient.Get(context.TODO(),
				client.ObjectKey{
					Name:      tc.M3Machine.Name,
					Namespace: tc.M3Machine.Namespace,
				},
				&hostClaim,
			)
			Expect(err).NotTo(HaveOccurred())
			_, err = machineMgr.FindOwnerRef(hostClaim.OwnerReferences)
			if tc.ExpectOwnerRef {
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
			}
			if tc.ExpectClusterLabel {
				Expect(err).NotTo(HaveOccurred())
				Expect(hostClaim.Labels[clusterv1beta1.ClusterNameLabel]).To(Equal(tc.Machine.Spec.ClusterName))
			}
		},
		Entry("Associate empty machine, Metal3 machine spec limited to InNamespace",
			testCaseAssociate{
				Machine: newMachine("", nil),
				M3Machine: newMetal3Machine(metal3machineName, withInNamespace(emptyM3MSpec()), nil,
					m3mObjectMetaWithValidAnnotations(),
				),
				ExpectRequeue:  false,
				ExpectOwnerRef: true,
			},
		),
		Entry("Associate empty machine, Metal3 machine spec set",
			testCaseAssociate{
				Machine: newMachine("", nil),
				M3Machine: newMetal3Machine(metal3machineName, withInNamespace(m3mSpecAll()), nil,
					m3mObjectMetaWithValidAnnotations(),
				),
				ExpectRequeue:  false,
				ExpectOwnerRef: true,
			},
		),
		Entry("Associate machine with DataTemplate missing",
			testCaseAssociate{
				Machine: newMachine(machineName, nil),
				M3Machine: newMetal3Machine(metal3machineName,
					withDataTemplate(withInNamespace(m3mSpecAll())),
					nil, nil,
				),
				ExpectClusterLabel: true,
				ExpectRequeue:      false,
				ExpectOwnerRef:     true,
			},
		),
		Entry("Associate machine with DataTemplate and Data ready",
			testCaseAssociate{
				Machine: newMachine(machineName, nil),
				M3Machine: newMetal3Machine(metal3machineName,
					withDataTemplate(withInNamespace(m3mSpecAll())),
					&infrav1.Metal3MachineStatus{
						RenderedData: &corev1.ObjectReference{Name: "abcd-0", Namespace: namespaceName},
					}, nil,
				),
				Data: &infrav1.Metal3Data{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "abcd-0",
						Namespace: namespaceName,
					},
					Spec: infrav1.Metal3DataSpec{
						MetaData: &corev1.SecretReference{
							Name: "metadata",
						},
						NetworkData: &corev1.SecretReference{
							Name: "networkdata",
						},
					},
					Status: infrav1.Metal3DataStatus{
						Ready: true,
					},
				},
				ExpectClusterLabel: true,
				ExpectRequeue:      false,
				ExpectOwnerRef:     true,
			},
		),
	)

	type testCaseDelete struct {
		HostClaim           *bmov1alpha1.HostClaim
		Secret              *corev1.Secret
		Machine             *clusterv1.Machine
		M3Machine           *infrav1.Metal3Machine
		ExpectedConsumerRef *corev1.ObjectReference
		ExpectedError       bool
		ExpectSecretDeleted bool
		Cluster             *clusterv1.Cluster
	}

	DescribeTable("Test Delete function",
		func(tc testCaseDelete) {
			objects := []client.Object{tc.M3Machine}
			if tc.HostClaim != nil {
				objects = append(objects, tc.HostClaim)
			}
			if tc.Secret != nil {
				objects = append(objects, tc.Secret)
			}

			fakeClient := fake.NewClientBuilder().WithScheme(setupSchemeMm()).WithObjects(objects...).Build()

			machineMgr, err := NewMachineHostManager(fakeClient, mockRemoteClientCache, tc.Cluster, nil, tc.Machine,
				tc.M3Machine, logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			err = machineMgr.Delete(context.TODO())

			if tc.ExpectedError {
				Expect(err).NotTo(HaveOccurred())
			} else {
				var reconcileError ReconcileError
				Expect(errors.As(err, &reconcileError)).To(BeTrue())
				Expect(reconcileError.IsTransient()).To(BeTrue())
			}
		},
	)

})
