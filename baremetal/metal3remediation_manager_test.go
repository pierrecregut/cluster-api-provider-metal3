/*
Copyright 2019 The Kubernetes Authors.
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
	"time"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	_ "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "k8s.io/client-go/kubernetes/fake"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	storagev1client "k8s.io/client-go/kubernetes/typed/storage/v1"
	k8stesting "k8s.io/client-go/testing"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type testCaseRemediationManager struct {
	Metal3Remediation *infrav1.Metal3Remediation
	Metal3Machine     *infrav1.Metal3Machine
	Machine           *clusterv1.Machine
	ExpectSuccess     bool
}

var _ = Describe("Metal3Remediation manager", func() {

	var fakeClient client.Client
	var mockRemoteClientCache RemoteClientCacheInterface

	BeforeEach(func() {
		fakeClient = fake.NewClientBuilder().WithScheme(setupScheme()).Build()
		mockRemoteClientCache = MockRemoteContext()
	})

	Describe("Test New Remediation Manager", func() {

		DescribeTable("Test NewRemediationManager",
			func(tc testCaseRemediationManager) {
				_, err := NewRemediationManager(
					context.TODO(), fakeClient,
					mockRemoteClientCache,
					nil, nil,
					tc.Metal3Remediation,
					tc.Metal3Machine,
					tc.Machine,
					logr.Discard(),
				)
				if tc.ExpectSuccess {
					Expect(err).NotTo(HaveOccurred())
				} else {
					Expect(err).To(HaveOccurred())
				}
			},
			Entry("All fields defined", testCaseRemediationManager{
				Metal3Remediation: &infrav1.Metal3Remediation{},
				Metal3Machine:     &infrav1.Metal3Machine{},
				Machine:           &clusterv1.Machine{},
				ExpectSuccess:     true,
			}),
			Entry("None of the fields defined", testCaseRemediationManager{
				Metal3Remediation: nil,
				Metal3Machine:     nil,
				Machine:           nil,
				ExpectSuccess:     true,
			}),
		)
	})

	DescribeTable("Test Finalizers",
		func(tc testCaseRemediationManager) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			Expect(remediationMgr.HasFinalizer()).To(BeFalse())
			remediationMgr.SetFinalizer()
			Expect(remediationMgr.HasFinalizer()).To(BeTrue())

			Expect(tc.Metal3Remediation.ObjectMeta.Finalizers).To(ContainElement(
				infrav1.RemediationFinalizer,
			))

			remediationMgr.UnsetFinalizer()

			Expect(tc.Metal3Remediation.ObjectMeta.Finalizers).NotTo(ContainElement(
				infrav1.RemediationFinalizer,
			))
		},
		Entry("No finalizers", testCaseRemediationManager{
			Metal3Remediation: &infrav1.Metal3Remediation{},
		}),
		Entry("Additional finalizers", testCaseRemediationManager{
			Metal3Remediation: &infrav1.Metal3Remediation{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{"foo"},
				},
			},
		}),
	)

	type testCaseRetryLimitSet struct {
		Metal3Remediation *infrav1.Metal3Remediation
		ExpectTrue        bool
	}

	DescribeTable("Test if Retry Limit is set",
		func(tc testCaseRetryLimitSet) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			retryLimitIsSet := remediationMgr.RetryLimitIsSet()

			Expect(retryLimitIsSet).To(Equal(tc.ExpectTrue))
		},
		Entry("retry limit is set", testCaseRetryLimitSet{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:       "",
						RetryLimit: 1,
					},
				},
			},
			ExpectTrue: true,
		}),
		Entry("retry limit is set to 0", testCaseRetryLimitSet{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:       "",
						RetryLimit: 0,
					},
				},
			},
			ExpectTrue: false,
		}),
		Entry("retry limit is set to less than 0", testCaseRetryLimitSet{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:       "",
						RetryLimit: -1,
					},
				},
			},
			ExpectTrue: false,
		}),
		Entry("retry limit is not set", testCaseRetryLimitSet{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type: "",
					},
				},
			},
			ExpectTrue: false,
		}),
	)

	DescribeTable("Test if Retry Limit is reached",
		func(tc testCaseRetryLimitSet) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			retryLimitIsSet := remediationMgr.HasReachRetryLimit()

			Expect(retryLimitIsSet).To(Equal(tc.ExpectTrue))
		},
		Entry("retry limit is reached", testCaseRetryLimitSet{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:       "",
						RetryLimit: 1,
					},
				},
				Status: infrav1.Metal3RemediationStatus{
					Phase:          "",
					RetryCount:     1,
					LastRemediated: &metav1.Time{},
				},
			},
			ExpectTrue: true,
		}),
		Entry("retry limit is not reached", testCaseRetryLimitSet{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:       "",
						RetryLimit: 1,
					},
				},
				Status: infrav1.Metal3RemediationStatus{
					Phase:          "",
					RetryCount:     0,
					LastRemediated: &metav1.Time{},
				},
			},
			ExpectTrue: false,
		}),
		Entry("retry limit is not set so limit is reached", testCaseRetryLimitSet{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:       "",
						RetryLimit: 0,
					},
				},
				Status: infrav1.Metal3RemediationStatus{
					Phase:          "",
					RetryCount:     0,
					LastRemediated: &metav1.Time{},
				},
			},
			ExpectTrue: true,
		}),
	)

	type testCaseEnsureOnlineStatus struct {
		Host              *bmov1alpha1.BareMetalHost
		Metal3Remediation *infrav1.Metal3Remediation
		ExpectTrue        bool
	}

	type testCaseGetUnhealthyHost struct {
		M3Machine         *infrav1.Metal3Machine
		Metal3Remediation *infrav1.Metal3Remediation
		ExpectPresent     bool
	}

	DescribeTable("Test GetUnhealthyHost",
		func(tc testCaseGetUnhealthyHost) {
			host := bmov1alpha1.BareMetalHost{
				ObjectMeta: testObjectMeta(baremetalhostName, namespaceName, ""),
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(&host).Build()

			remediationMgr, err := NewRemediationManager(context.TODO(), fakeClient, mockRemoteClientCache, nil, nil, nil, tc.M3Machine, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			remediatedHost, helper, err := remediationMgr.GetUnhealthyHost(context.TODO())
			result := remediatedHost.GetObject()
			if tc.ExpectPresent {
				Expect(result).NotTo(BeNil())
				Expect(helper).NotTo(BeNil())
				Expect(err).ToNot(HaveOccurred())
			} else {
				Expect(result).To(BeNil())
				Expect(helper).To(BeNil())
				Expect(err).To(HaveOccurred())
			}
		},
		Entry("Should find the unhealthy host", testCaseGetUnhealthyHost{
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       namespaceName,
					OwnerReferences: []metav1.OwnerReference{},
					Annotations: map[string]string{
						HostAnnotation: namespaceName + "/" + baremetalhostName,
					},
				},
			},
			ExpectPresent: true,
		}),
		Entry("Should not find the unhealthy host", testCaseGetUnhealthyHost{
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       namespaceName,
					OwnerReferences: []metav1.OwnerReference{},
					Annotations: map[string]string{
						HostAnnotation: namespaceName + "/wronghostname",
					},
				},
			},
			ExpectPresent: false,
		}),
		Entry("Should not find the host, annotation is empty", testCaseGetUnhealthyHost{
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       namespaceName,
					OwnerReferences: []metav1.OwnerReference{},
					Annotations:     map[string]string{},
				},
			},
			ExpectPresent: false,
		}),
		Entry("Should not find the host, annotation is nil", testCaseGetUnhealthyHost{
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       "myns",
					OwnerReferences: []metav1.OwnerReference{},
					Annotations:     nil,
				},
			},
			ExpectPresent: false,
		}),
		Entry("Should not find the host, could not parse annotation value", testCaseGetUnhealthyHost{
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       "myns",
					OwnerReferences: []metav1.OwnerReference{},
					Annotations: map[string]string{
						HostAnnotation: "myns/wronghostname/wronghostname",
					},
				},
			},
			ExpectPresent: false,
		}),
		Entry("Should find host in own namespace when annotation contains a foreign namespace prefix", testCaseGetUnhealthyHost{
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       namespaceName,
					OwnerReferences: []metav1.OwnerReference{},
					Annotations: map[string]string{
						HostAnnotation: "other-namespace/" + baremetalhostName,
					},
				},
			},
			ExpectPresent: true,
		}),
		Entry("Should not find host in other namespace", testCaseGetUnhealthyHost{
			// The BMH is in namespaceName for all these tests.
			// We should not be able to find any BMH in the other-namespace,
			// where the M3M is located in this test.
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       "other-namespace",
					OwnerReferences: []metav1.OwnerReference{},
					Annotations: map[string]string{
						HostAnnotation: namespaceName + "/" + baremetalhostName,
					},
				},
			},
			ExpectPresent: false,
		}),
	)

	DescribeTable("Test GetUnhealthyHost - HostClaim branch",
		func(annotation string, seedClaim bool, expectPresent bool) {
			m3machine := &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      metal3machineName,
					Namespace: namespaceName,
					Annotations: map[string]string{
						HostClaimAnnotation: annotation,
					},
				},
				Spec: infrav1.Metal3MachineSpec{
					HostSelector: &infrav1.HostSelector{HostClaim: infrav1.HostClaimEnabled},
					IdentityRef:  infrav1.IdentityRef{Name: "kb"},
				},
			}
			remoteObjects := []client.Object{}
			if seedClaim {
				remoteObjects = append(remoteObjects, NewHostClaimBuilder(hostClaimName, remoteNs).Build())
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(m3machine).Build()
			mgr, err := NewRemediationManager(context.TODO(), fakeClient, MockRemoteContext(remoteObjects...),
				nil, nil, nil, m3machine, nil, logr.Discard())
			Expect(err).NotTo(HaveOccurred())

			remediatedHost, helper, err := mgr.GetUnhealthyHost(context.TODO())
			result := remediatedHost.GetObject()
			if expectPresent {
				Expect(err).NotTo(HaveOccurred())
				Expect(result).NotTo(BeNil())
				Expect(helper).NotTo(BeNil())
			} else {
				Expect(err).To(HaveOccurred())
				Expect(result).To(BeNil())
				Expect(helper).To(BeNil())
			}
		},
		Entry("finds the HostClaim in the remote namespace", hostClaimName, true, true),
		Entry("errors when the HostClaim is missing", hostClaimName, false, false),
	)

	type testCaseSetAnnotation struct {
		Host       *bmov1alpha1.BareMetalHost
		M3Machine  *infrav1.Metal3Machine
		ExpectTrue bool
	}

	DescribeTable("Test SetUnhealthyAnnotation",
		func(tc testCaseSetAnnotation) {
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(tc.Host).Build()
			remediationMgr, err := NewRemediationManager(context.TODO(), fakeClient, mockRemoteClientCache, nil, nil, nil, tc.M3Machine, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			patchError := remediationMgr.SetUnhealthyAnnotation(context.TODO())

			if tc.ExpectTrue {
				Expect(patchError).ToNot(HaveOccurred())
			} else {
				Expect(patchError).To(HaveOccurred())
			}
		},
		Entry("Should set the unhealthy annotation", testCaseSetAnnotation{
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       "myns",
					OwnerReferences: []metav1.OwnerReference{},
					Annotations: map[string]string{
						HostAnnotation: "myns/" + baremetalhostName,
					},
				},
			},
			Host: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Name:        baremetalhostName,
					Namespace:   "myns",
					Annotations: map[string]string{infrav1.UnhealthyAnnotation: ""},
				},
			},
			ExpectTrue: true,
		}),
		Entry("Should not set the unhealthy annotation, annotation is empty", testCaseSetAnnotation{
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       "myns",
					OwnerReferences: []metav1.OwnerReference{},
					Annotations:     map[string]string{},
				},
			},
			Host: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Name:        baremetalhostName,
					Namespace:   "myns",
					Annotations: map[string]string{infrav1.UnhealthyAnnotation: ""},
				},
			},
			ExpectTrue: false,
		}),
		Entry("Should not set the unhealthy annotation because of wrong HostAnnotation", testCaseSetAnnotation{
			M3Machine: &infrav1.Metal3Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       "myns",
					OwnerReferences: []metav1.OwnerReference{},
					Annotations: map[string]string{
						HostAnnotation: "myns/wronghostname",
					},
				},
			},
			Host: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Name:        baremetalhostName,
					Namespace:   "myns",
					Annotations: map[string]string{infrav1.UnhealthyAnnotation: ""},
				},
			},
			ExpectTrue: false,
		}),
	)

	It("Test SetUnhealthyAnnotation - HostClaim backend quarantines the claim", func() {
		rc := newRemediationCase(hostKindHostClaim, true)

		Expect(rc.mgr.SetUnhealthyAnnotation(context.TODO())).To(Succeed())

		saved := &bmov1alpha1.HostClaim{}
		Expect(rc.hostClient.Get(context.TODO(), client.ObjectKeyFromObject(rc.host), saved)).To(Succeed())

		By("marking the claim as unhealthy and quarantined")
		Expect(saved.Annotations).To(HaveKey(infrav1.UnhealthyAnnotation))
		Expect(saved.Annotations).To(HaveKey(QuarantinedAnnotation))

		By("clearing the workload spec and powering off")
		Expect(saved.Spec.Image).To(BeNil())
		Expect(saved.Spec.UserData).To(BeNil())
		Expect(saved.Spec.NetworkData).To(BeNil())
		Expect(saved.Spec.MetaData).To(BeNil())
		Expect(saved.Spec.PoweredOn).To(BeFalse())

		By("retargeting ConsumerRef to the quarantine sentinel so no Metal3Machine adopts it")
		Expect(saved.Spec.ConsumerRef).NotTo(BeNil())
		Expect(saved.Spec.ConsumerRef.Kind).To(Equal("Metal3Remediation"))
		Expect(saved.Spec.ConsumerRef.APIVersion).To(Equal(infrav1.GroupVersion.String()))

		By("dropping the cluster-name label so cluster teardown ignores it")
		Expect(saved.Labels).NotTo(HaveKey(clusterv1.ClusterNameLabel))
	})

	type testCaseGetRemediationType struct {
		Metal3Remediation  *infrav1.Metal3Remediation
		RemediationType    *infrav1.RemediationType
		RemediationTypeSet bool
	}

	DescribeTable("Test GetRemediationType",
		func(tc testCaseGetRemediationType) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			remediationType := remediationMgr.GetRemediationType()
			if tc.RemediationTypeSet {
				Expect(remediationType).To(Equal(infrav1.RebootRemediationStrategy))
			} else {
				Expect(remediationType).To(Equal(infrav1.RemediationType("")))
			}
		},
		Entry("Remediation strategy type is set to Reboot, should return strategy", testCaseGetRemediationType{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:       "Reboot",
						RetryLimit: 0,
					},
				},
			},
			RemediationTypeSet: true,
		}),
		Entry("Remediation strategy type is not set should return empty string", testCaseGetRemediationType{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:       "",
						RetryLimit: 0,
					},
				},
			},
			RemediationTypeSet: false,
		}),
		Entry("Remediation strategy is nil should return empty string", testCaseGetRemediationType{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{},
			},
			RemediationTypeSet: false,
		}),
	)

	type testCaseGetRemediatedTime struct {
		Metal3Remediation *infrav1.Metal3Remediation
		Remediated        bool
	}

	DescribeTable("Test GetLastRemediatedTime",
		func(tc testCaseGetRemediatedTime) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			remediationTime := remediationMgr.GetLastRemediatedTime()
			if tc.Remediated {
				Expect(remediationTime).NotTo(BeNil())
			} else {
				Expect(remediationTime).To(BeNil())
			}
		},
		Entry("Host is not yet remediated and last remediation timestamp is not set yet", testCaseGetRemediatedTime{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{},
			},
			Remediated: false,
		}),
		Entry("Host is remediated and controller has set the last remediation timestamp", testCaseGetRemediatedTime{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{
					LastRemediated: &metav1.Time{},
				},
			},
			Remediated: true,
		}),
	)

	type testTimeToRemediate struct {
		Metal3Remediation *infrav1.Metal3Remediation
		ExpectTrue        bool
	}

	DescribeTable("Test TimeToRemediate",
		func(tc testTimeToRemediate) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())
			okToRemediate, nextRemediation := remediationMgr.TimeToRemediate(remediationMgr.GetTimeoutSeconds())
			if tc.ExpectTrue {
				Expect(okToRemediate).To(BeTrue())
				Expect(nextRemediation).To(Equal(time.Duration(0)))

			} else {
				Expect(okToRemediate).To(BeFalse())
				Expect(nextRemediation).NotTo(Equal(time.Duration(0)))
			}

		},
		Entry("Time to remediate reached", testTimeToRemediate{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:           "",
						RetryLimit:     1,
						TimeoutSeconds: int32(600),
					},
				},
				Status: infrav1.Metal3RemediationStatus{
					Phase:          "",
					RetryCount:     1,
					LastRemediated: &metav1.Time{Time: time.Now().Add(time.Duration(-700) * time.Second)},
				},
			},
			ExpectTrue: true,
		}),
		Entry("Time to remediate is not reached because of LastRemediated is nil", testTimeToRemediate{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:           "",
						RetryLimit:     1,
						TimeoutSeconds: int32(600),
					},
				},
				Status: infrav1.Metal3RemediationStatus{
					Phase:          "",
					RetryCount:     1,
					LastRemediated: nil,
				},
			},
			ExpectTrue: false,
		}),
		Entry("Time to remediate is not reached, LastRemediated + Timeout is greater than current time", testTimeToRemediate{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:           "",
						RetryLimit:     1,
						TimeoutSeconds: int32(600),
					},
				},
				Status: infrav1.Metal3RemediationStatus{
					Phase:          "",
					RetryCount:     1,
					LastRemediated: &metav1.Time{Time: time.Now()},
				},
			},
			ExpectTrue: false,
		}),
	)

	type testCaseGetTimeoutSeconds struct {
		Metal3Remediation *infrav1.Metal3Remediation
		TimeoutSet        bool
	}

	DescribeTable("Test GetTimeoutSeconds",
		func(tc testCaseGetTimeoutSeconds) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			timeoutSeconds := remediationMgr.GetTimeoutSeconds()
			if tc.TimeoutSet {
				Expect(timeoutSeconds).NotTo(BeNil())
			} else {
				Expect(timeoutSeconds).To(Equal(defaultTimeout))
			}
		},
		Entry("Timeout is not set", testCaseGetTimeoutSeconds{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{},
				},
			},
			TimeoutSet: false,
		}),
		Entry("Timeout is set", testCaseGetTimeoutSeconds{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Spec: infrav1.Metal3RemediationSpec{
					Strategy: &infrav1.RemediationStrategy{
						Type:           "",
						RetryLimit:     0,
						TimeoutSeconds: 300,
					},
				},
			},
			TimeoutSet: true,
		}),
	)

	DescribeTable("Test SetRemediationPhase",
		func(tc testCaseRemediationManager) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			remediationMgr.SetRemediationPhase(infrav1.PhaseRunning)

			Expect(tc.Metal3Remediation.Status.Phase).To(Equal("Running"))
		},
		Entry("No phase", testCaseRemediationManager{
			Metal3Remediation: &infrav1.Metal3Remediation{},
		}),
		Entry("Overwride excisting phase", testCaseRemediationManager{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{
					Phase: "Waiting",
				},
			},
		}),
	)

	DescribeTable("Test SetLastRemediationTime",
		func(tc testCaseRemediationManager) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())
			now := metav1.Now()

			remediationMgr.SetLastRemediationTime(&now)

			Expect(tc.Metal3Remediation.Status.LastRemediated).ShouldNot(BeNil())
		},
		Entry("No timestamp set", testCaseRemediationManager{
			Metal3Remediation: &infrav1.Metal3Remediation{},
		}),
		Entry("Overwride excisting time", testCaseRemediationManager{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{
					LastRemediated: &metav1.Time{},
				},
			},
		}),
	)

	DescribeTable("Test IncreaseRetryCount",
		func(tc testCaseRemediationManager) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())
			oldCount := tc.Metal3Remediation.Status.RetryCount

			remediationMgr.IncreaseRetryCount()
			newCount := oldCount + 1
			Expect(tc.Metal3Remediation.Status.RetryCount).To(Equal(newCount))
		},
		Entry("RetryCount is not set", testCaseRemediationManager{
			Metal3Remediation: &infrav1.Metal3Remediation{},
		}),
		Entry("RetryCount is 0", testCaseRemediationManager{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{
					RetryCount: 0,
				},
			},
		}),
		Entry("RetryCount is 2", testCaseRemediationManager{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{
					RetryCount: 2,
				},
			},
		}),
	)

	type testCaseGetRemediationPhase struct {
		Metal3Remediation *infrav1.Metal3Remediation
		Succeed           bool
	}

	DescribeTable("Test GetRemediationPhase",
		func(tc testCaseGetRemediationPhase) {
			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil, tc.Metal3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			phase := remediationMgr.GetRemediationPhase()

			if tc.Succeed {
				if phase == infrav1.PhaseRunning {
					Expect(tc.Metal3Remediation.Status.Phase).Should(ContainSubstring("Running"))
				}
				if phase == infrav1.PhaseWaiting {
					Expect(tc.Metal3Remediation.Status.Phase).Should(ContainSubstring("Waiting"))
				}
				if phase == infrav1.PhaseDeleting {
					Expect(tc.Metal3Remediation.Status.Phase).Should(ContainSubstring("Deleting machine"))
				}
			} else {
				Expect(phase).ShouldNot(ContainSubstring("Deleting machine"))
				Expect(phase).ShouldNot(ContainSubstring("Waiting"))
				Expect(phase).ShouldNot(ContainSubstring("Running"))
			}

		},
		Entry("No phase set", testCaseGetRemediationPhase{
			Metal3Remediation: &infrav1.Metal3Remediation{},
			Succeed:           false,
		}),
		Entry("Phase is set to Running", testCaseGetRemediationPhase{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{
					Phase: "Running",
				},
			},
			Succeed: true,
		}),
		Entry("Phase is set to Waiting", testCaseGetRemediationPhase{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{
					Phase: "Waiting",
				},
			},
			Succeed: true,
		}),
		Entry("Phase is set to Deleting", testCaseGetRemediationPhase{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{
					Phase: "Deleting machine",
				},
			},
			Succeed: true,
		}),
		Entry("Phase is set to something else", testCaseGetRemediationPhase{
			Metal3Remediation: &infrav1.Metal3Remediation{
				Status: infrav1.Metal3RemediationStatus{
					Phase: "test",
				},
			},
			Succeed: false,
		}),
	)

	DescribeTable("Test OnlineStatus",
		func(kind hostKind, powered bool) {
			rc := newRemediationCase(kind, powered)

			status, err := rc.mgr.PowerSpec(context.TODO())
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(Equal(powered))
		},
		Entry("BMH Online=false returns false", hostKindBMH, false),
		Entry("BMH Online=true returns true", hostKindBMH, true),
		Entry("HostClaim PoweredOn=false returns false", hostKindHostClaim, false),
		Entry("HostClaim PoweredOn=true returns true", hostKindHostClaim, true),
	)

	DescribeTable("Test PowerOffAnnotation",
		func(kind hostKind) {
			rc := newRemediationCase(kind, false)

			ensureNotExists := func() {
				Expect(rc.mgr.IsPowerOffRequested(context.TODO())).To(BeFalse(), "IsPowerOffrequested should return false")
				Expect(rc.hostClient.Get(context.TODO(), client.ObjectKeyFromObject(rc.host), rc.host)).To(Succeed())
				Expect(rc.host.GetAnnotations()).ToNot(HaveKey(rc.mgr.getPowerOffAnnotationKey()), "host should not have power off annotation")
			}

			ensureExists := func() {
				Expect(rc.mgr.IsPowerOffRequested(context.TODO())).To(BeTrue(), "IsPowerOffrequested should return true")
				Expect(rc.hostClient.Get(context.TODO(), client.ObjectKeyFromObject(rc.host), rc.host)).To(Succeed())
				Expect(rc.host.GetAnnotations()).To(HaveKeyWithValue(
					And(
						Equal(rc.mgr.getPowerOffAnnotationKey()),
						HaveSuffix(string(rc.mgr.Metal3Remediation.UID)), // to ensure that powerOffAnnotation can be formatted with the UID
					),
					ContainSubstring(string(bmov1alpha1.RebootModeHard)),
				), "host should have power off annotation")
			}

			ensureNotExists()

			By("Setting annotation")
			Expect(rc.mgr.SetPowerOffAnnotation(context.TODO())).To(Succeed(), "SetPowerOffAnnotation should succeed")
			ensureExists()

			By("Removing annotation")
			Expect(rc.mgr.RemovePowerOffAnnotation(context.TODO())).To(Succeed(), "RemovePowerOffAnnotation should succeed")
			ensureNotExists()
		},
		Entry("BareMetalHost backend", hostKindBMH),
		Entry("HostClaim backend", hostKindHostClaim),
	)

	Describe("Test NodeBackupAnnotation", func() {
		It("should set and remove the node backup annotation as requested", func() {
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).Build()
			m3Remediation := &infrav1.Metal3Remediation{}

			remediationMgr, err := NewRemediationManager(context.TODO(), fakeClient, mockRemoteClientCache, nil, nil, m3Remediation, nil, nil,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			ensureNotExists := func() {
				ann, lbl := remediationMgr.GetNodeBackupAnnotations()
				Expect(ann).To(BeEmpty(), "there shouldn't be any node backup annotations")
				Expect(lbl).To(BeEmpty(), "there shouldn't be any node backup labels")
				Expect(m3Remediation.ObjectMeta.Annotations).ToNot(HaveKey(nodeAnnotationsBackupAnnotation), "bmh should not have node annotations backup annotation")
				Expect(m3Remediation.ObjectMeta.Annotations).ToNot(HaveKey(nodeLabelsBackupAnnotation), "bmh should not have node labels backup annotation")
			}

			ensureExists := func() {
				ann, lbl := remediationMgr.GetNodeBackupAnnotations()
				Expect(ann).To(Equal("foo"), "there should be node backup annotations")
				Expect(lbl).To(Equal("bar"), "there should be node backup labels")
				Expect(m3Remediation.ObjectMeta.Annotations).To(HaveKeyWithValue(
					nodeAnnotationsBackupAnnotation,
					Equal("foo"),
				), "remediation should have correct node annotations backup annotation")
				Expect(m3Remediation.ObjectMeta.Annotations).To(HaveKeyWithValue(
					nodeLabelsBackupAnnotation,
					Equal("bar"),
				), "remediation should have correct node labels backup annotation")
			}

			ensureNotExists()

			By("Setting annotations")
			Expect(remediationMgr.SetNodeBackupAnnotations("foo", "bar")).To(BeTrue(), "SetNodeBackupAnnotations should return true")
			ensureExists()

			By("Removing annotations")
			remediationMgr.RemoveNodeBackupAnnotations()
			ensureNotExists()
		})
	})

	Describe("Test Nodes", func() {
		cluster := &clusterv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "mycluster",
			},
		}
		m3Remediation := &infrav1.Metal3Remediation{
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: clusterv1.GroupVersion.String(),
						Kind:       "Machine",
						Name:       "mymachine",
					},
				},
			},
		}
		capiMachine := &clusterv1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name: "mymachine",
				Labels: map[string]string{
					"cluster.x-k8s.io/cluster-name": "mycluster",
				},
			},
			Status: clusterv1.MachineStatus{
				NodeRef: clusterv1.MachineNodeReference{
					Name: "mynode",
				},
			},
		}
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "mynode",
			},
		}

		It("Should find, update and delete node", func() {
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(cluster, m3Remediation, capiMachine).Build()
			corev1Client := clientfake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: node.Name,
			}}).CoreV1()
			clientGetter := func(_ context.Context, _ client.Client, _ *clusterv1.Cluster) (clientcorev1.CoreV1Interface, error) {
				return corev1Client, nil
			}
			remediationMgr, err := NewRemediationManager(context.TODO(), fakeClient, mockRemoteClientCache, clientGetter, nil, m3Remediation, nil, capiMachine,
				logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			By("getting cluster client")
			clusterClient, err := remediationMgr.GetClusterClient(context.TODO())
			Expect(err).ToNot(HaveOccurred(), "should get cluster client")

			By("Getting node")
			node, err := remediationMgr.GetNode(context.TODO(), clusterClient)
			Expect(err).ToNot(HaveOccurred(), "should find node without error")
			Expect(node).ToNot(BeNil(), "node should not be nil")
			Expect(node.GetName()).To(Equal("mynode"), "node should have correct name")

			By("Updating node")
			node.Annotations = make(map[string]string)
			node.Annotations["foo"] = "bar"
			Expect(remediationMgr.UpdateNode(context.TODO(), clusterClient, node)).To(Succeed(), "should update node without error")

			newNode, err := corev1Client.Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
			Expect(err).ToNot(HaveOccurred(), "should get updated node")
			Expect(newNode.Annotations).To(HaveLen(1))
			Expect(newNode.Annotations).To(HaveKeyWithValue("foo", "bar"))

			By("Deleting node")
			Expect(remediationMgr.DeleteNode(context.TODO(), clusterClient, node)).To(Succeed(), "should delete node without error")
			_, err = corev1Client.Nodes().Get(context.TODO(), node.Name, metav1.GetOptions{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected NotFound error")
		})

	})

	Describe("Test IsNodeDrained", func() {
		It("should list pods using spec.nodeName field selector", func() {
			nodeName := "drain-node"
			clusterClientset := clientfake.NewSimpleClientset()

			usedFieldSelector := ""
			clusterClientset.Fake.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				listAction, ok := action.(k8stesting.ListAction)
				Expect(ok).To(BeTrue())
				usedFieldSelector = listAction.GetListRestrictions().Fields.String()

				now := metav1.Now()
				return true, &corev1.PodList{Items: []corev1.Pod{{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "terminating-pod",
						DeletionTimestamp: &now,
					},
					Spec: corev1.PodSpec{NodeName: nodeName},
				}}}, nil
			})

			remediationMgr, err := NewRemediationManager(context.TODO(), nil, mockRemoteClientCache, nil, nil,
				&infrav1.Metal3Remediation{}, nil, nil, logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			drained := remediationMgr.IsNodeDrained(context.TODO(), clusterClientset.CoreV1(),
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}},
			)

			Expect(drained).To(BeFalse())
			Expect(usedFieldSelector).To(Equal("spec.nodeName=" + nodeName))
		})

		It("should use injected storage client getter and block drain on matching volume attachment", func() {
			nodeName := "mynode"
			clusterName := "mycluster"
			machineName := "mymachine"
			remediationName := "myremediation"

			workloadCluster := &clusterv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespaceName,
				},
			}

			machine := &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      machineName,
					Namespace: namespaceName,
					Labels: map[string]string{
						"cluster.x-k8s.io/cluster-name": clusterName,
					},
				},
			}

			remediation := &infrav1.Metal3Remediation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      remediationName,
					Namespace: namespaceName,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: clusterv1.GroupVersion.String(),
						Kind:       "Machine",
						Name:       machineName,
					}},
				},
			}

			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(
				workloadCluster,
				machine,
				remediation,
			).Build()

			clusterClientset := clientfake.NewSimpleClientset(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "running-pod"},
				Spec:       corev1.PodSpec{NodeName: nodeName},
			})

			storageClientset := clientfake.NewSimpleClientset(&storagev1.VolumeAttachment{
				ObjectMeta: metav1.ObjectMeta{Name: "va-match"},
				Spec: storagev1.VolumeAttachmentSpec{
					Attacher: "metal3.io",
					NodeName: nodeName,
				},
			})

			storageClientGetterCalled := false
			listedVolumeAttachments := false
			storageClientset.Fake.PrependReactor("list", "volumeattachments", func(k8stesting.Action) (bool, runtime.Object, error) {
				listedVolumeAttachments = true
				return false, nil, nil
			})

			storageClientGetter := func(_ context.Context, _ client.Client, cluster *clusterv1.Cluster) (storagev1client.StorageV1Interface, error) {
				storageClientGetterCalled = true
				Expect(cluster.Name).To(Equal(clusterName))
				Expect(cluster.Namespace).To(Equal(namespaceName))
				return storageClientset.StorageV1(), nil
			}

			remediationMgr, err := NewRemediationManager(context.TODO(), fakeClient, mockRemoteClientCache, nil, storageClientGetter,
				remediation, nil, nil, logr.Discard(),
			)
			Expect(err).NotTo(HaveOccurred())

			drained := remediationMgr.IsNodeDrained(context.TODO(), clusterClientset.CoreV1(),
				&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}},
			)

			Expect(storageClientGetterCalled).To(BeTrue())
			Expect(listedVolumeAttachments).To(BeTrue())
			Expect(drained).To(BeFalse())
		})
	})
})

type hostKind int

const (
	hostKindBMH hostKind = iota
	hostKindHostClaim
)

// remediationCase bundles the manager under test with the underlying host
// object and whichever fake client stores it, so a spec can exercise either
// backend through the same assertions.
type remediationCase struct {
	mgr        *RemediationManager
	host       client.Object
	hostClient client.Client
}

// newRemediationCase builds a RemediationManager seeded with a single host of
// the requested kind, powered on or off per `powered`. For hostKindBMH the
// host lives on the manager's local client; for hostKindHostClaim it lives on
// a mock remote client in `remoteNs`.
func newRemediationCase(kind hostKind, powered bool) remediationCase {
	const hostName = "myhost"
	remediation := &infrav1.Metal3Remediation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "myremediation",
			Namespace: namespaceName,
			UID:       "123",
		},
	}
	m3machine := &infrav1.Metal3Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mym3machine",
			Namespace: namespaceName,
		},
	}

	switch kind {
	case hostKindBMH:
		m3machine.Annotations = map[string]string{HostAnnotation: namespaceName + "/" + hostName}
		bmh := &bmov1alpha1.BareMetalHost{
			ObjectMeta: metav1.ObjectMeta{Name: hostName, Namespace: namespaceName},
			Spec:       bmov1alpha1.BareMetalHostSpec{Online: powered},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(bmh, m3machine, remediation).Build()
		mgr, err := NewRemediationManager(context.TODO(), fakeClient, MockRemoteContext(),
			nil, nil, remediation, m3machine, nil, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		return remediationCase{mgr: mgr, host: bmh, hostClient: fakeClient}

	case hostKindHostClaim:
		m3machine.Annotations = map[string]string{HostClaimAnnotation: hostName}
		m3machine.Spec.HostSelector = &infrav1.HostSelector{HostClaim: infrav1.HostClaimEnabled}
		m3machine.Spec.IdentityRef = infrav1.IdentityRef{Name: "kb"}
		hc := NewHostClaimBuilder(hostName, remoteNs).WithClusterLabel(clusterName).Build()
		hc.Spec.PoweredOn = powered
		// Seed workload spec so quarantine assertions can observe it being cleared.
		hc.Spec.Image = &bmov1alpha1.Image{URL: "http://example/img"}
		hc.Spec.UserData = &corev1.SecretReference{Name: "ud"}
		fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(m3machine, remediation).Build()
		remoteCache := MockRemoteContext(hc)
		mgr, err := NewRemediationManager(context.TODO(), fakeClient, remoteCache,
			nil, nil, remediation, m3machine, nil, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		return remediationCase{mgr: mgr, host: hc, hostClient: mgr.RemoteClient}
	}
	Fail("unknown hostKind")
	return remediationCase{}
}
