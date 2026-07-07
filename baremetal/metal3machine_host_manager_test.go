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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const (
	localNs       = "local"
	remoteNs      = "remote"
	hostClaimName = "hostclaim"
)

var _ = Describe("Metal3Machine Manager targeting HostClaims", func() {
	type testCaseGetHostClaim struct {
		m3machine           *infrav1.Metal3Machine
		expectError         bool
		expectedHostClaimNS string
	}
	DescribeTable("getHostClaim",
		func(tc testCaseGetHostClaim) {
			ctx := context.TODO()
			localHostClaim := NewHostClaimBuilder(hostClaimName, localNs).Build()
			remoteHostClaim := NewHostClaimBuilder(hostClaimName, remoteNs).Build()
			fakeClient := fake.NewClientBuilder().WithObjects(localHostClaim, tc.m3machine).WithScheme(setupScheme()).Build()
			m3manager, err := NewMachineHostClaimManager(
				ctx, fakeClient, MockRemoteContext(remoteHostClaim),
				nil, nil, nil, tc.m3machine, GinkgoLogr)
			Expect(err).NotTo(HaveOccurred())
			hostClaim, _, err := m3manager.getHostClaim(context.TODO())
			if tc.expectError {
				Expect(err).To(HaveOccurred())
				return
			}
			Expect(err).NotTo(HaveOccurred())
			if tc.expectedHostClaimNS == "" {
				Expect(hostClaim).To(BeNil())
				return
			}
			Expect(hostClaim).NotTo(BeNil())
			Expect(hostClaim.Namespace).To(Equal(tc.expectedHostClaimNS))
		},
		Entry("Not bound m3machine", testCaseGetHostClaim{
			m3machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      metal3machineName,
					Namespace: namespaceName,
				},
			},
		}),
		Entry("local hostClaim", testCaseGetHostClaim{
			m3machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:        metal3machineName,
					Namespace:   localNs,
					Annotations: map[string]string{HostClaimAnnotation: hostClaimName},
				},
			},
			expectedHostClaimNS: localNs,
		}),
		Entry("remote hostClaim", testCaseGetHostClaim{
			m3machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:        metal3machineName,
					Namespace:   namespaceName,
					Annotations: map[string]string{HostClaimAnnotation: hostClaimName},
				},
				Spec: infrav1.Metal3MachineSpec{
					IdentityRef: infrav1.IdentityRef{Name: "kb"},
				},
			},
			expectedHostClaimNS: remoteNs,
		}),
	)

	type testCaseBmhResolver struct {
		hostclaim *bmov1alpha1.HostClaim
		bmhName   string
		uid       string
	}

	DescribeTable("BMH Resolver for HostClaim",
		func(tc testCaseBmhResolver) {
			m3machine := &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:        metal3machineName,
					Namespace:   localNs,
					Annotations: map[string]string{HostClaimAnnotation: hostClaimName},
				},
			}

			fakeClient := fake.NewClientBuilder().WithObjects(m3machine, tc.hostclaim).WithScheme(setupScheme()).Build()
			m3manager, err := NewMachineHostClaimManager(
				context.TODO(), fakeClient, MockRemoteContext(),
				nil, nil, nil, m3machine, GinkgoLogr)
			Expect(err).NotTo(HaveOccurred())
			ctx := context.TODO()
			bmhName, err := m3manager.getBmhNameFromM3Machine(ctx)
			if tc.bmhName != "" {
				Expect(err).NotTo(HaveOccurred())
				Expect(bmhName).To(Equal(tc.bmhName))
			} else {
				Expect(err).To(HaveOccurred())
			}
			uid, err := m3manager.getBmhUIDFromM3Machine(ctx)
			if tc.uid == "" {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(uid).To(Equal(tc.uid))
			}
		},
		Entry("hostClaim not bound yet", testCaseBmhResolver{
			hostclaim: NewHostClaimBuilder(hostClaimName, localNs).WithUID("hc-uid").Build(),
			bmhName:   "",
			uid:       "",
		}),
		Entry("hostClaim bound", testCaseBmhResolver{
			hostclaim: NewHostClaimBuilder(hostClaimName, localNs).WithUID("hc-uid").WithBareMetalHost(baremetalhostName, "ns", "bmh-uid").Build(),
			bmhName:   baremetalhostName,
			uid:       "bmh-uid",
		}),
	)

	It("Getting the remote client cache fails", func() {
		m3machine := &infrav1.Metal3Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:        metal3machineName,
				Namespace:   localNs,
				Annotations: map[string]string{HostClaimAnnotation: hostClaimName},
			},
			Spec: infrav1.Metal3MachineSpec{
				IdentityRef: infrav1.IdentityRef{Name: "kb"},
			},
		}
		fakeClient := fake.NewClientBuilder().WithObjects(m3machine).WithScheme(setupScheme()).Build()

		wantErr := errors.New("error placeholder")
		cache := &MockRemote{err: wantErr}

		_, err := NewMachineHostClaimManager(
			context.TODO(), fakeClient, cache,
			nil, nil, nil, m3machine, GinkgoLogr)
		Expect(err).To(MatchError(wantErr))

		cond := filterCondition(m3machine.Status.Conditions, infrav1.AssociateBareMetalHostCondition)
		Expect(cond).To(HaveLen(1))
		Expect(cond[0].Status).To(Equal(metav1.ConditionFalse))
		Expect(cond[0].Reason).To(Equal(infrav1.RemoteClusterUnavailableReason))
	})
})

// MockRemote implements the RemoteClientCache interface.
type MockRemote struct {
	err    error
	client client.WithWatch
}

var _ RemoteClientCacheInterface = &MockRemote{}

func (mr *MockRemote) Start(_ context.Context) error { return nil }

func (mr *MockRemote) GetRemoteClient(_ context.Context, _ logr.Logger, m3machine *infrav1.Metal3Machine) (*RemoteClient, error) {
	if mr.err != nil {
		return nil, mr.err
	}
	if m3machine == nil || m3machine.Spec.IdentityRef.Name == "" {
		return nil, nil //nolint:nilnil
	}
	return &RemoteClient{
		WithWatch: mr.client,
		Namespace: remoteNs,
	}, nil
}

func (mr *MockRemote) GetWatchChannel() chan event.GenericEvent {
	return make(chan event.GenericEvent)
}

// For tests a remote cllient initialized with objects.
func MockRemoteContext(objects ...client.Object) RemoteClientCacheInterface {
	remoteClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
	return &MockRemote{client: remoteClient}
}

type HostClaimBuilder struct {
	hostclaim bmov1alpha1.HostClaim
}

func NewHostClaimBuilder(name string, namespace string) *HostClaimBuilder {
	return &HostClaimBuilder{
		hostclaim: bmov1alpha1.HostClaim{
			TypeMeta: metav1.TypeMeta{
				Kind:       "HostClaim",
				APIVersion: bmov1alpha1.GroupVersion.String(),
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		},
	}
}

func (hb *HostClaimBuilder) Build() *bmov1alpha1.HostClaim {
	return &hb.hostclaim
}

func (hb *HostClaimBuilder) WithUID(uid string) *HostClaimBuilder {
	hb.hostclaim.UID = types.UID(uid)
	return hb
}

func (hb *HostClaimBuilder) WithBareMetalHost(name string, namespace string, uid types.UID) *HostClaimBuilder {
	hb.hostclaim.Status.BareMetalHost = &bmov1alpha1.ObjectReference{
		Name:      name,
		Namespace: namespace,
		UID:       uid,
	}
	return hb
}
