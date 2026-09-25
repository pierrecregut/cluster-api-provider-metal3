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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

	type testCaseSyncSecretData struct {
		source           *corev1.SecretReference
		localSecret      *corev1.Secret
		useRemoteClient  bool
		expectError      bool
		expectedRef      *corev1.SecretReference
		expectRemoteData map[string][]byte
	}

	DescribeTable("syncSecretData",
		func(tc testCaseSyncSecretData) {
			var hostClaim *bmov1alpha1.HostClaim
			m3machine := NewM3MBuilder(metal3machineName, localNs).Build()
			remoteClientCache := MockRemoteContext()
			if tc.useRemoteClient {
				hostClaim = NewHostClaimBuilder(hostClaimName, remoteNs).Build()
				m3machine.Spec.IdentityRef = infrav1.IdentityRef{Name: "kb"}
			} else {
				hostClaim = NewHostClaimBuilder(hostClaimName, localNs).Build()
				m3machine.Spec.IdentityRef = infrav1.IdentityRef{}
			}
			localObjects := []client.Object{m3machine}
			if tc.localSecret != nil {
				localObjects = append(localObjects, tc.localSecret)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(localObjects...).Build()

			m3manager, err := NewMachineHostClaimManager(
				context.TODO(), fakeClient, remoteClientCache,
				nil, nil, nil, m3machine, GinkgoLogr)
			Expect(err).NotTo(HaveOccurred())

			ref, err := m3manager.syncSecretData(context.TODO(), tc.source, hostClaim, "userdata")
			if tc.expectError {
				Expect(err).To(HaveOccurred())
				return
			}
			Expect(err).NotTo(HaveOccurred())
			if tc.expectedRef == nil {
				Expect(ref).To(BeNil())
			} else {
				Expect(ref).NotTo(BeNil())
				Expect(ref.Name).To(Equal(tc.expectedRef.Name))
			}
			if tc.expectRemoteData != nil {
				remoteSecret := corev1.Secret{}
				err = m3manager.remoteClient.Get(context.TODO(),
					client.ObjectKey{Name: hostClaimName + "-userdata", Namespace: remoteNs},
					&remoteSecret)
				Expect(err).NotTo(HaveOccurred())
				Expect(remoteSecret.Data).To(Equal(tc.expectRemoteData))
			}
		},
		Entry("nil source returns nil", testCaseSyncSecretData{
			source:      nil,
			expectedRef: nil,
		}),
		Entry("local client copies source name", testCaseSyncSecretData{
			source:          &corev1.SecretReference{Name: "datasecret", Namespace: localNs},
			useRemoteClient: false,
			expectedRef:     &corev1.SecretReference{Name: "datasecret"},
		}),
		Entry("remote client syncs secret data", testCaseSyncSecretData{
			source:          &corev1.SecretReference{Name: "datasecret", Namespace: localNs},
			useRemoteClient: true,
			localSecret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "datasecret", Namespace: localNs},
				Data:       map[string][]byte{"key": []byte("value")},
			},
			expectedRef:      &corev1.SecretReference{Name: hostClaimName + "-userdata"},
			expectRemoteData: map[string][]byte{"key": []byte("value")},
		}),
		Entry("remote client errors when local secret missing", testCaseSyncSecretData{
			source:          &corev1.SecretReference{Name: "missing-secret", Namespace: localNs},
			useRemoteClient: true,
			expectError:     true,
		}),
	)

	// setHostSpec
	// ensureAnnotation
	// translateHostRequirements

	var (
		defaultImage = &infrav1.Image{
			URL:          testImageURL,
			Checksum:     ptr.To(testImageChecksumURL),
			ChecksumType: "sha512",
			DiskFormat:   testImageDiskFormat,
		}
	)

	type testCaseProvideHostClaim struct {
		m3machine          *infrav1.Metal3Machine
		useRemoteClient    bool
		expectError        bool
		expectNamespace    string
		expectConsumerKind string
		expectOwnerRef     bool
		expectMatchLabels  map[string]string
		expectFailureDom   string
	}

	DescribeTable("provideHostClaim",
		func(tc testCaseProvideHostClaim) {
			if tc.useRemoteClient {
				tc.m3machine.Spec.IdentityRef = infrav1.IdentityRef{Name: "kb"}
			}
			remoteClientCache := MockRemoteContext()
			fakeClient := fake.NewClientBuilder().WithScheme(setupSchemeMm()).WithObjects(tc.m3machine).Build()
			cluster := newCluster(clusterName)
			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      machineName,
					Namespace: namespaceName,
				},
				Spec: clusterv1.MachineSpec{
					ClusterName: clusterName,
				},
			}

			m3manager, err := NewMachineHostClaimManager(
				context.TODO(), fakeClient, remoteClientCache,
				cluster, nil, machine, tc.m3machine, GinkgoLogr)
			Expect(err).NotTo(HaveOccurred())

			hostClaim, helper, err := m3manager.provideHostClaim(context.TODO())
			if tc.expectError {
				Expect(err).To(HaveOccurred())
				return
			}
			Expect(err).NotTo(HaveOccurred())
			Expect(hostClaim).NotTo(BeNil())
			Expect(helper).NotTo(BeNil())

			Expect(hostClaim.Name).To(Equal(tc.m3machine.Name))
			Expect(hostClaim.Namespace).To(Equal(tc.expectNamespace))

			Expect(hostClaim.Spec.ConsumerRef).NotTo(BeNil())
			Expect(hostClaim.Spec.ConsumerRef.Name).To(Equal(tc.m3machine.Name))
			Expect(hostClaim.Spec.ConsumerRef.Kind).To(Equal(tc.expectConsumerKind))

			Expect(hostClaim.Spec.PoweredOn).To(BeFalse())
			Expect(hostClaim.Spec.FailureDomain).To(Equal(tc.expectFailureDom))

			Expect(hostClaim.Labels).NotTo(BeNil())
			Expect(hostClaim.Labels[clusterv1.ClusterNameLabel]).To(Equal(clusterName))

			if tc.expectMatchLabels != nil {
				Expect(hostClaim.Spec.HostSelector.MatchLabels).To(Equal(tc.expectMatchLabels))
			}

			if tc.expectOwnerRef {
				hasOwnerRef, err := controllerutil.HasOwnerReference(
					hostClaim.OwnerReferences, cluster, fakeClient.Scheme())
				Expect(err).NotTo(HaveOccurred())
				Expect(hasOwnerRef).To(BeTrue())
			} else {
				Expect(hostClaim.OwnerReferences).To(BeEmpty())
			}
		},
		Entry("local client creates hostclaim with owner ref", testCaseProvideHostClaim{
			m3machine: NewM3MBuilder(metal3machineName, namespaceName).
				InNamespace("").Build(),
			useRemoteClient:    false,
			expectNamespace:    namespaceName,
			expectConsumerKind: metal3MachineKind,
			expectOwnerRef:     true,
		}),
		Entry("remote client creates hostclaim without owner ref", testCaseProvideHostClaim{
			m3machine: NewM3MBuilder(metal3machineName, namespaceName).
				InNamespace("").Build(),
			useRemoteClient:    true,
			expectNamespace:    remoteNs,
			expectConsumerKind: remoteMetal3MachineKind,
			expectOwnerRef:     false,
		}),
		Entry("hostclaim with match labels and failure domain", testCaseProvideHostClaim{
			m3machine: NewM3MBuilder(metal3machineName, namespaceName).
				InNamespace("").FailureDomain("zone").HostSelector(&infrav1.HostSelector{MatchLabels: map[string]string{"k": "v"}}).Build(),
			useRemoteClient:    false,
			expectNamespace:    namespaceName,
			expectConsumerKind: metal3MachineKind,
			expectOwnerRef:     true,
			expectMatchLabels:  map[string]string{"k": "v"},
			expectFailureDom:   "zone",
		}),
		Entry("hostclaim with match expressions", testCaseProvideHostClaim{
			m3machine: NewM3MBuilder(metal3machineName, namespaceName).
				InNamespace("").
				HostSelector(&infrav1.HostSelector{MatchExpressions: []infrav1.HostSelectorRequirement{
					{Key: "l", Operator: "In", Values: []string{"v"}},
				}}).Build(),
			useRemoteClient:    false,
			expectNamespace:    namespaceName,
			expectConsumerKind: metal3MachineKind,
			expectOwnerRef:     true,
		}),
	)

	// setHostConsumerRef
	// Associate
	type testCaseAssociate struct {
		Machine            *clusterv1.Machine
		Host               *bmov1alpha1.HostClaim
		M3Machine          *infrav1.Metal3Machine
		ExpectRequeue      bool
		ExpectClusterLabel bool
		ExpectOwnerRef     bool
	}

	DescribeTable("Test Associate function",
		func(tc testCaseAssociate) {
			mset := &clusterv1.MachineSet{
				ObjectMeta: metav1.ObjectMeta{Name: "machineset", Namespace: tc.Machine.Namespace},
				Spec: clusterv1.MachineSetSpec{
					Template: clusterv1.MachineTemplateSpec{
						Spec: clusterv1.MachineSpec{
							InfrastructureRef: clusterv1.ContractVersionedObjectReference{
								Name:     "template",
								Kind:     "Metal3Machine",
								APIGroup: "metal3.io",
							},
						},
					},
				},
			}
			template := &infrav1.Metal3MachineTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: tc.Machine.Namespace},
			}
			err := controllerutil.SetOwnerReference(mset, tc.Machine, setupSchemeMm())
			Expect(err).NotTo(HaveOccurred())
			objects := []client.Object{
				tc.M3Machine,
				tc.Machine,
				mset,
				template,
			}
			if tc.Host != nil {
				objects = append(objects, tc.Host)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupSchemeMm()).WithObjects(objects...).Build()

			mockRemoteClientCache := MockRemoteContext()
			machineMgr, err := NewMachineHostClaimManager(context.TODO(), fakeClient, mockRemoteClientCache, newCluster(clusterName), nil, tc.Machine,
				tc.M3Machine, GinkgoLogr,
			)
			Expect(err).NotTo(HaveOccurred())

			_, err = machineMgr.Associate(context.TODO())
			if tc.ExpectRequeue {
				var reconcileError ReconcileError
				ok := errors.As(err, &reconcileError)
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
			hasOwnerRef, err := controllerutil.HasOwnerReference(hostClaim.OwnerReferences, machineMgr.Cluster, machineMgr.client.Scheme())
			Expect(err).NotTo(HaveOccurred())
			Expect(hasOwnerRef).To(Equal(tc.ExpectOwnerRef))
			if tc.ExpectClusterLabel {
				Expect(err).NotTo(HaveOccurred())
				Expect(hostClaim.Labels).NotTo(BeNil())
				Expect(hostClaim.Labels[clusterv1.ClusterNameLabel]).To(Equal(tc.Machine.Spec.ClusterName))
			}
		},
		Entry("Associate empty machine, Metal3 machine spec limited to InNamespace",
			testCaseAssociate{
				Machine:        newMachine("", nil),
				M3Machine:      NewM3MBuilder(metal3machineName, namespaceName).InNamespace("").WithClaimAnnotation(hostClaimName).Build(),
				ExpectRequeue:  false,
				ExpectOwnerRef: true,
			},
		),

		Entry("Associate empty machine, Metal3 machine spec set",
			testCaseAssociate{
				Machine: newMachine("", nil),
				M3Machine: NewM3MBuilder(metal3machineName, namespaceName).InNamespace("").
					WithClaimAnnotation(hostClaimName).Build(),
				ExpectRequeue:  false,
				ExpectOwnerRef: true,
			},
		),
	)

	type testCaseUpdate struct {
		Machine       *clusterv1.Machine
		HostClaim     *bmov1alpha1.HostClaim
		M3Machine     *infrav1.Metal3Machine
		Secrets       []client.Object
		Remote        bool
		RemoteSecrets int
		ExpectError   bool
		ExpectRequeue bool
	}

	DescribeTable("Test Update function",
		func(tc testCaseUpdate) {
			ctx := context.TODO()
			objects := []client.Object{
				tc.M3Machine,
				tc.Machine,
			}
			objects = append(objects, tc.Secrets...)
			remoteObjs := []client.Object{}
			if tc.HostClaim != nil {
				if tc.Remote {
					remoteObjs = append(remoteObjs, tc.HostClaim)
				} else {
					objects = append(objects, tc.HostClaim)
				}
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupSchemeMm()).WithObjects(objects...).Build()

			mockRemoteClientCache := MockRemoteContext(remoteObjs...)
			machineMgr, err := NewMachineHostClaimManager(ctx, fakeClient, mockRemoteClientCache, newCluster(clusterName), nil, tc.Machine,
				tc.M3Machine, GinkgoLogr,
			)
			Expect(err).NotTo(HaveOccurred())
			// Needed to get a status.UserData (done during Associate)
			Expect(machineMgr.getUserDataSecretName(ctx)).To(Succeed())
			// AssociateM3Metadata must be run before Update (done by controller)
			Expect(machineMgr.AssociateM3Metadata(ctx)).To(Succeed())
			err = machineMgr.Update(ctx)
			if tc.ExpectRequeue {
				var reconcileError ReconcileError
				Expect(errors.As(err, &reconcileError)).To(BeTrue())
			} else if tc.ExpectError {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			secretList := corev1.SecretList{}
			mr, ok := mockRemoteClientCache.(*MockRemote)
			Expect(ok).To(BeTrue())
			remoteClient := mr.client
			Expect(remoteClient.List(ctx, &secretList)).To(Succeed())
			Expect(secretList.Items).To(HaveLen(tc.RemoteSecrets))
		},
		Entry("Regular case - no image yet", testCaseUpdate{
			Machine:     newMachine("", nil),
			M3Machine:   NewM3MBuilder(metal3machineName, namespaceName).InNamespace("").UserData("user-data").MetaData("meta-data").WithClaimAnnotation(hostClaimName).Build(),
			HostClaim:   NewHostClaimBuilder(hostClaimName, namespaceName).Build(),
			ExpectError: true,
		}),
		Entry("Regular case - with image - local", testCaseUpdate{
			Machine:   newMachine("", nil),
			M3Machine: NewM3MBuilder(metal3machineName, namespaceName).InNamespace("").Image(defaultImage).UserData("user-data").MetaData("meta-data").WithClaimAnnotation(hostClaimName).Build(),
			Secrets:   []client.Object{newHostClaimSecret("user-data", map[string][]byte{}), newHostClaimSecret("meta-data", map[string][]byte{})},
			HostClaim: NewHostClaimBuilder(hostClaimName, namespaceName).Build(),
		}),
		Entry("Regular case - with image - remote", testCaseUpdate{
			Machine:       newMachine("", nil),
			M3Machine:     NewM3MBuilder(metal3machineName, namespaceName).InNamespace("").IdentityRef("kb").Image(defaultImage).UserData("user-data").MetaData("meta-data").WithClaimAnnotation(hostClaimName).Build(),
			Secrets:       []client.Object{newHostClaimSecret("user-data", map[string][]byte{}), newHostClaimSecret("meta-data", map[string][]byte{})},
			HostClaim:     NewHostClaimBuilder(hostClaimName, remoteNs).Build(),
			Remote:        true,
			RemoteSecrets: 2,
		}),
		Entry("Regular case - with image - remote - no image", testCaseUpdate{
			Machine:       newMachine("", nil),
			M3Machine:     NewM3MBuilder(metal3machineName, namespaceName).InNamespace("").IdentityRef("kb").Image(defaultImage).UserData("user-data").MetaData("meta-data").WithClaimAnnotation(hostClaimName).Build(),
			HostClaim:     NewHostClaimBuilder(hostClaimName, remoteNs).Build(),
			Remote:        true,
			ExpectRequeue: true,
		}),
	)
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

type M3MBuilder struct {
	metal3machine infrav1.Metal3Machine
}

func NewM3MBuilder(name string, namespace string) *M3MBuilder {
	return &M3MBuilder{
		metal3machine: infrav1.Metal3Machine{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Metal3Machine",
				APIVersion: infrav1.GroupVersion.String(),
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		},
	}
}

func (mb *M3MBuilder) Build() *infrav1.Metal3Machine {
	return &mb.metal3machine
}

func (mb *M3MBuilder) InNamespace(ns string) *M3MBuilder {
	if mb.metal3machine.Spec.HostSelector == nil {
		mb.metal3machine.Spec.HostSelector = &infrav1.HostSelector{}
	}
	mb.metal3machine.Spec.HostSelector.InNamespace = ns
	return mb
}

func (mb *M3MBuilder) IdentityRef(secretName string) *M3MBuilder {
	mb.metal3machine.Spec.IdentityRef = infrav1.IdentityRef{Name: secretName}
	return mb
}

func (mb *M3MBuilder) WithClaimAnnotation(name string) *M3MBuilder {
	if mb.metal3machine.Annotations == nil {
		mb.metal3machine.Annotations = map[string]string{}
	}
	mb.metal3machine.Annotations[HostClaimAnnotation] = name
	return mb
}

func (mb *M3MBuilder) ProviderID(pid string) *M3MBuilder {
	mb.metal3machine.Spec.ProviderID = pid
	return mb
}

func (mb *M3MBuilder) Image(img *infrav1.Image) *M3MBuilder {
	mb.metal3machine.Spec.Image = *img
	return mb
}

func (mb *M3MBuilder) FailureDomain(zone string) *M3MBuilder {
	mb.metal3machine.Spec.FailureDomain = zone
	return mb
}

func (mb *M3MBuilder) HostSelector(hs *infrav1.HostSelector) *M3MBuilder {
	mb.metal3machine.Spec.HostSelector = hs
	return mb
}

func (mb *M3MBuilder) UserData(name string) *M3MBuilder {
	mb.metal3machine.Spec.UserData = &corev1.SecretReference{
		Name: name,
	}
	return mb
}

func (mb *M3MBuilder) MetaData(name string) *M3MBuilder {
	mb.metal3machine.Spec.MetaData = &corev1.SecretReference{
		Name: name,
	}
	return mb
}

func (mb *M3MBuilder) RenderedData(name string) *M3MBuilder {
	mb.metal3machine.Status.RenderedData = &infrav1.Metal3ObjectRef{
		Name:      name,
		Namespace: mb.metal3machine.Namespace,
	}
	return mb
}

func (mb *M3MBuilder) DataTemplate(name string) *M3MBuilder {
	mb.metal3machine.Spec.DataTemplate = &infrav1.Metal3ObjectRef{
		Name:      name,
		Namespace: mb.metal3machine.Namespace,
	}
	return mb
}

func newHostClaimSecret(name string, content map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespaceName,
		},
		Data: content,
		Type: "Opaque",
	}
}
