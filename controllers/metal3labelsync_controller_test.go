/*
Copyright 2021 The Kubernetes Authors.

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

package controllers

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	"github.com/metal3-io/cluster-api-provider-metal3/baremetal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientfake "k8s.io/client-go/kubernetes/fake"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var hcuuid = types.UID("16cfa55a-495d-4048-8165-256d35166434")
var hwduuid = types.UID("fff22de7-2697-4785-935a-52d6a0b6179a")

var _ = Describe("Metal3LabelSync controller", func() {

	type TestCaseBuildLabelSyncSet struct {
		PrefixSet      map[string]struct{}
		Labels         map[string]string
		ExpectedResult map[string]string
	}

	DescribeTable("Build Label Sync Set",
		func(tc TestCaseBuildLabelSyncSet) {
			got := buildLabelSyncSet(tc.PrefixSet, tc.Labels)
			Expect(reflect.DeepEqual(got, tc.ExpectedResult)).To(BeTrue(),
				"Expected %v but got %v", tc.ExpectedResult, got)
		},
		Entry("Single label case", TestCaseBuildLabelSyncSet{
			PrefixSet: map[string]struct{}{
				"foo.metal3.io": {},
			},
			Labels: map[string]string{
				"foo.metal3.io/bar": "blue",
			},
			ExpectedResult: map[string]string{
				"foo.metal3.io/bar": "blue",
			},
		}),
		Entry("Multiple label case", TestCaseBuildLabelSyncSet{
			PrefixSet: map[string]struct{}{
				"foo.metal3.io": {},
				"boo.metal3.io": {},
			},
			Labels: map[string]string{
				"foo.metal3.io/bar":  "blue",
				"foo.metal3.io/car":  "red",
				"boo.metal3.io/bill": "green",
			},
			ExpectedResult: map[string]string{
				"foo.metal3.io/bar":  "blue",
				"foo.metal3.io/car":  "red",
				"boo.metal3.io/bill": "green",
			},
		}),
		Entry("Ignore labels not in prefix set", TestCaseBuildLabelSyncSet{
			PrefixSet: map[string]struct{}{
				"foo.metal3.io": {},
			},
			Labels: map[string]string{
				"boo.metal3.io/blah":     "red",
				"foo.metal4.io/blahblah": "green",
				"foo.metal3.io/bar":      "blue",
			},
			ExpectedResult: map[string]string{
				"foo.metal3.io/bar": "blue",
			},
		}),
		Entry("Empty prefix set", TestCaseBuildLabelSyncSet{
			PrefixSet: map[string]struct{}{},
			Labels: map[string]string{
				"foo.metal3.io/blah":     "red",
				"foo.metal3.io/blahblah": "green",
				"boo.metal3.io/bar":      "blue",
			},
			ExpectedResult: map[string]string{},
		}),
		Entry("Empty prefix set and empty labels", TestCaseBuildLabelSyncSet{
			PrefixSet:      map[string]struct{}{},
			Labels:         map[string]string{},
			ExpectedResult: map[string]string{},
		}),
		Entry("Empty labels", TestCaseBuildLabelSyncSet{
			PrefixSet: map[string]struct{}{
				"foo.metal3.io": {},
			},
			Labels:         map[string]string{},
			ExpectedResult: map[string]string{},
		}),
	)

	type TestCaseParsePrefixAnnotation struct {
		PrefixStr      string
		ExpectedErr    bool
		ExpectedResult map[string]struct{}
	}

	type TestCaseSynchronizeLabelSyncSetsOnNode struct {
		PrefixSet      map[string]struct{}
		Host           *bmov1alpha1.BareMetalHost
		Node           *corev1.Node
		ExpectedResult map[string]string
	}

	DescribeTable("Build Label Sync Set",
		func(tc TestCaseSynchronizeLabelSyncSetsOnNode) {
			hostLabelSyncSet := buildLabelSyncSet(tc.PrefixSet, tc.Host.Labels)
			nodeLabelSyncSet := buildLabelSyncSet(tc.PrefixSet, tc.Node.Labels)
			synchronizeLabelSyncSetsOnNode(hostLabelSyncSet, nodeLabelSyncSet, tc.Node)
			Expect(reflect.DeepEqual(tc.Node.Labels, tc.ExpectedResult)).To(BeTrue(),
				"Expected %v but got %v", tc.ExpectedResult, tc.Node.Labels)
		},
		Entry("Label exists, do nothing", TestCaseSynchronizeLabelSyncSetsOnNode{
			PrefixSet: map[string]struct{}{
				"foo.metal3.io": {},
			},
			Host: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo.metal3.io/bar": "blue",
					},
				},
			},
			Node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo.metal3.io/bar": "blue",
					},
				},
			},
			ExpectedResult: map[string]string{
				"foo.metal3.io/bar": "blue",
			},
		}),
		Entry("Add label on Node", TestCaseSynchronizeLabelSyncSetsOnNode{
			PrefixSet: map[string]struct{}{
				"foo.metal3.io": {},
			},
			Host: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo.metal3.io/bar": "blue",
					},
				},
			},
			Node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			ExpectedResult: map[string]string{
				"foo.metal3.io/bar": "blue",
			},
		}),
		Entry("Add multiple labels on Node, ignore existing label", TestCaseSynchronizeLabelSyncSetsOnNode{
			PrefixSet: map[string]struct{}{
				"foo.metal3.io": {},
				"boo.metal3.io": {},
			},
			Host: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo.metal3.io/bar":  "blue",
						"boo.metal3.io/ball": "red",
					},
				},
			},
			Node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"some.metal4.io/blah": "pink", // ignore
					},
				},
			},
			ExpectedResult: map[string]string{
				"foo.metal3.io/bar":   "blue",
				"boo.metal3.io/ball":  "red",
				"some.metal4.io/blah": "pink",
			},
		}),
		Entry("Remove and update labels from Node", TestCaseSynchronizeLabelSyncSetsOnNode{
			PrefixSet: map[string]struct{}{
				"foo.metal3.io": {},
				"boo.metal3.io": {},
			},
			Host: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo.metal3.io/bar": "blue",
					},
				},
			},
			Node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo.metal3.io/bar":  "XXXX", // update
						"foo.metal3.io/blah": "YYYY", // remove
					},
				},
			},
			ExpectedResult: map[string]string{
				"foo.metal3.io/bar": "blue",
			},
		}),
		Entry("Empty prefix set, do nothing", TestCaseSynchronizeLabelSyncSetsOnNode{
			PrefixSet: map[string]struct{}{},
			Host: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"some.bmh-label.io/blah": "gray", // ignore
					},
				},
			},
			Node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"some.node-label.io/bar": "orange", // ignore
					},
				},
			},
			ExpectedResult: map[string]string{
				"some.node-label.io/bar": "orange",
			},
		}),
	)
	type TestCaseMetal3ClusterToMetal3Machines struct {
		Cluster        *clusterv1.Cluster
		M3Cluster      *infrav1.Metal3Cluster
		Machine        *clusterv1.Machine
		M3Machine      *infrav1.Metal3Machine
		ExpectRequests []ctrl.Request
	}

	DescribeTable("Metal3Cluster To Metal3Machine tests",
		func(tc TestCaseMetal3ClusterToMetal3Machines) {
			objects := []client.Object{
				tc.Cluster,
				tc.M3Cluster,
				tc.Machine,
				tc.M3Machine,
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			r := Metal3LabelSyncReconciler{
				Client: fakeClient,
				Log:    logr.Discard(),
			}
			obj := client.Object(tc.M3Cluster)
			reqs := r.Metal3ClusterToMetal3Machine(context.Background(), obj)
			Expect(reflect.DeepEqual(reqs, tc.ExpectRequests)).To(BeTrue(),
				"Expected %v but got %v", tc.ExpectRequests, reqs)
		},
		Entry("Metal3Cluster To Metal3Machine",
			TestCaseMetal3ClusterToMetal3Machines{
				Cluster:   newCluster(clusterName, nil, nil),
				M3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, nil, false),
				Machine:   newMachine(clusterName, machineName, metal3machineName, ""),
				M3Machine: newMetal3Machine(metal3machineName, m3mObjectMeta(), nil, nil, false),
				ExpectRequests: []ctrl.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      metal3machineName,
							Namespace: namespaceName,
						},
					},
				},
			},
		),
		Entry("Host annotation with wrong namespace",
			// We should ignore the namespace in the annotation and only look in the M3M namespace
			TestCaseMetal3ClusterToMetal3Machines{
				Cluster:   newCluster(clusterName, nil, nil),
				M3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, nil, false),
				Machine:   newMachine(clusterName, machineName, metal3machineName, ""),
				M3Machine: newMetal3Machine(metal3machineName, &metav1.ObjectMeta{
					Name:            metal3machineName,
					Namespace:       namespaceName,
					OwnerReferences: m3mOwnerRefs(),
					Labels: map[string]string{
						clusterv1.ClusterNameLabel: clusterName,
					},
					Annotations: map[string]string{
						baremetal.HostAnnotation: "other-namespace/" + baremetalhostName,
					},
				}, nil, nil, false),
				ExpectRequests: []ctrl.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      metal3machineName,
							Namespace: namespaceName,
						},
					},
				},
			},
		),
	)

	type TestCaseHostClaimToMetal3Machines struct {
		HostClaim      *bmov1alpha1.HostClaim
		ExpectRequests []ctrl.Request
	}
	DescribeTable("HostClaim To Metal3Machine tests",
		func(tc TestCaseHostClaimToMetal3Machines) {
			objects := []client.Object{
				tc.HostClaim,
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			r := Metal3LabelSyncReconciler{
				Client: fakeClient,
				Log:    logr.Discard(),
			}
			obj := client.Object(tc.HostClaim)
			reqs := r.HostClaimToMetal3Machines(context.Background(), obj)
			Expect(reflect.DeepEqual(reqs, tc.ExpectRequests)).To(BeTrue(),
				"Expected %v but got %v", tc.ExpectRequests, reqs)
		},
		Entry("HostClaim To Metal3Machine",
			TestCaseHostClaimToMetal3Machines{
				HostClaim: newHostClaim(hostClaimName, nil, nil, false, ""),
				ExpectRequests: []ctrl.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      metal3machineName,
							Namespace: namespaceName,
						},
					},
				},
			},
		),
	)

	Describe("Test BareMetalHost To Metal3Machine function", func() {
		metal3MachineSpec := bmov1alpha1.BareMetalHostSpec{
			ConsumerRef: &corev1.ObjectReference{
				Name:       metal3machineName,
				Namespace:  namespaceName,
				Kind:       metal3MachineKind,
				APIVersion: infrav1.GroupVersion.String(),
			},
		}
		type TestCaseBareMetalHostToMetal3Machines struct {
			BareMetalHost  *bmov1alpha1.BareMetalHost
			ExpectRequests []ctrl.Request
		}
		DescribeTable("BareMetalHost To Metal3Machine tests",
			func(tc TestCaseBareMetalHostToMetal3Machines) {
				objects := []client.Object{
					tc.BareMetalHost,
				}
				fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
				r := Metal3LabelSyncReconciler{
					Client: fakeClient,
					Log:    logr.Discard(),
				}
				obj := client.Object(tc.BareMetalHost)
				reqs := r.BareMetalHostToMetal3Machines(context.Background(), obj)
				Expect(reflect.DeepEqual(reqs, tc.ExpectRequests)).To(BeTrue(),
					"Expected %v but got %v", tc.ExpectRequests, reqs)
			},
			Entry("BareMetalHost To Metal3Machine",
				TestCaseBareMetalHostToMetal3Machines{
					BareMetalHost: newBareMetalHost(baremetalhostName, &metal3MachineSpec, nil, nil, false),
					ExpectRequests: []ctrl.Request{
						{
							NamespacedName: types.NamespacedName{
								Name:      metal3machineName,
								Namespace: namespaceName,
							},
						},
					},
				},
			),
		)
	})

	Describe("Test labelsync Reconcile functions", func() {
		testLabels := map[string]string{
			"foo.metal3.io/bar": "blue",
		}
		metal3MachineSpec := bmov1alpha1.BareMetalHostSpec{
			ConsumerRef: &corev1.ObjectReference{
				Name:       metal3machineName,
				Namespace:  namespaceName,
				Kind:       metal3MachineKind,
				APIVersion: infrav1.GroupVersion.String(),
			},
		}
		annotation := map[string]string{
			"metal3.io/metal3-label-sync-prefixes": "foo.metal3.io",
		}
		incorrectAnnotation := map[string]string{
			"metal3.io/incorrect-metal3-label-sync-prefixes": "incorrect",
		}
		hostAnnotationValue := fmt.Sprintf("%s/%s", namespaceName, baremetalhostName)
		hostClaimAnnotationValue := hostClaimName
		m3mForHostClaimSpec := &infrav1.Metal3MachineSpec{
			HostSelector: &infrav1.HostSelector{HostClaim: infrav1.HostClaimEnabled},
		}
		nodeName := "testNode"
		cluserCapiSpec := clusterv1.ClusterSpec{
			Paused: ptr.To(true),
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				Name:     metal3ClusterName,
				Kind:     "Metal3Cluster",
				APIGroup: infrav1.GroupVersion.Group,
			},
		}
		type testCaseReconcile struct {
			hardwaredata    *bmov1alpha1.HardwareData
			baremetalhost   *bmov1alpha1.BareMetalHost
			hostclaim       *bmov1alpha1.HostClaim
			machine         *clusterv1.Machine
			metal3Machine   *infrav1.Metal3Machine
			cluster         *clusterv1.Cluster
			metal3Cluster   *infrav1.Metal3Cluster
			expectError     bool
			expectRequeue   bool
			expectLabelsync map[string]string
			debug           bool
		}
		DescribeTable("Test reconcile",

			func(tc testCaseReconcile) {

				objects := []client.Object{}
				if tc.hardwaredata != nil {
					objects = append(objects, tc.hardwaredata)
				}
				if tc.baremetalhost != nil {
					objects = append(objects, tc.baremetalhost)
				}
				if tc.hostclaim != nil {
					objects = append(objects, tc.hostclaim)
				}
				if tc.cluster != nil {
					objects = append(objects, tc.cluster)
				}
				if tc.metal3Cluster != nil {
					objects = append(objects, tc.metal3Cluster)
				}
				if tc.machine != nil {
					objects = append(objects, tc.machine)
				}
				if tc.metal3Machine != nil {
					objects = append(objects, tc.metal3Machine)
				}

				fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
				corev1Client := clientfake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name: nodeName,
				}}).CoreV1()
				r := &Metal3LabelSyncReconciler{
					Client:         fakeClient,
					ManagerFactory: baremetal.NewManagerFactory(fakeClient, nil),
					Log:            GinkgoLogr,
					CapiClientGetter: func(_ context.Context, _ client.Client, _ *clusterv1.Cluster) (
						clientcorev1.CoreV1Interface, error,
					) {
						return corev1Client, nil
					},
					RemoteClients:    baremetal.NewRemoteClientCache(fakeClient),
					WatchFilterValue: "",
				}
				req := reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name:      metal3machineName,
						Namespace: namespaceName,
					},
				}
				result, err := r.Reconcile(context.TODO(), req)

				if tc.expectError {
					Expect(err).To(HaveOccurred())
				} else {
					Expect(err).NotTo(HaveOccurred())
				}
				if tc.expectRequeue {
					Expect(result.RequeueAfter).To(BeNumerically(">", 0))
				} else {
					Expect(result.RequeueAfter).ToNot(BeNumerically(">", 0))
				}

				node, _ := corev1Client.Nodes().Get(context.TODO(), "testNode", metav1.GetOptions{})
				Expect(node.Labels).To(Equal(tc.expectLabelsync))
			},
			Entry("Metal3Machine not found", testCaseReconcile{
				expectError:   false,
				expectRequeue: true,
			}),
			Entry("Metal3Machine with no owner", testCaseReconcile{
				metal3Machine: newMetal3Machine(metal3machineName, nil, nil, nil, false),
				expectError:   false,
				expectRequeue: true,
			}),
			Entry("CapiMachine not found", testCaseReconcile{
				metal3Machine: newMetal3Machine(metal3machineName, m3mObjectMetaWithOwnerRef(), nil, nil, false),
				expectError:   true,
				expectRequeue: false,
			}),
			Entry("Error fetching cluster", testCaseReconcile{
				metal3Machine: newMetal3Machine(metal3machineName, m3mObjectMetaWithOwnerRef(), nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				expectError:   true,
				expectRequeue: true,
			}),

			Entry("Error fetching Metal3Cluster", testCaseReconcile{
				metal3Machine: newMetal3Machine(metal3machineName, m3mObjectMetaWithOwnerRef(), nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				expectError:   true,
				expectRequeue: true,
			}),

			Entry("Baremetal host not found", testCaseReconcile{
				metal3Machine: newMetal3Machine(metal3machineName,
					m3mObjectMetaWithOwnerRefWithAnnotations(baremetal.HostAnnotation, hostAnnotationValue),
					nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, nil, false),
			}),
			Entry("Paused Baremetal", testCaseReconcile{
				baremetalhost: newBareMetalHost(baremetalhostName, nil, nil, nil, true),
				metal3Machine: newMetal3Machine(metal3machineName,
					m3mObjectMetaWithOwnerRefWithAnnotations(baremetal.HostAnnotation, hostAnnotationValue),
					nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, annotation, false),
				expectRequeue: true,
			}),
			Entry("Metal3Machine with no host annotation", testCaseReconcile{
				baremetalhost: newBareMetalHost(baremetalhostName, nil, nil, nil, true),
				metal3Machine: newMetal3Machine(metal3machineName, m3mObjectMetaWithOwnerRef(), nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, annotation, false),
			}),

			Entry("Could not find Node Ref", testCaseReconcile{
				baremetalhost: newBareMetalHost(baremetalhostName, &metal3MachineSpec, nil, testLabels, false),
				metal3Machine: newMetal3Machine(metal3machineName, m3mObjectMetaWithOwnerRef(), nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, ""),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, annotation, false),
				expectRequeue: true,
			}),
			Entry("Cluster is paused", testCaseReconcile{
				baremetalhost: newBareMetalHost(baremetalhostName, &metal3MachineSpec, nil, testLabels, false),
				metal3Machine: newMetal3Machine(metal3machineName, m3mObjectMetaWithOwnerRef(), nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, &cluserCapiSpec, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, annotation, false),
				expectRequeue: true,
			}),
			Entry("No annotation found on Metal3Cluster", testCaseReconcile{
				baremetalhost: newBareMetalHost(baremetalhostName, &metal3MachineSpec, nil, testLabels, false),
				metal3Machine: newMetal3Machine(metal3machineName, m3mObjectMetaWithOwnerRef(), nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, nil, false),
			}),
			Entry("No annotation for the prefixes found on Metal3Cluster", testCaseReconcile{
				baremetalhost: newBareMetalHost(baremetalhostName, &metal3MachineSpec, nil, testLabels, false),
				metal3Machine: newMetal3Machine(metal3machineName, m3mObjectMetaWithOwnerRef(), nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, incorrectAnnotation, false),
			}),
			Entry("No errors for baremetalhost case", testCaseReconcile{
				baremetalhost: newBareMetalHost(baremetalhostName, &metal3MachineSpec, nil, testLabels, false),
				metal3Machine: newMetal3Machine(metal3machineName,
					m3mObjectMetaWithOwnerRefWithAnnotations(baremetal.HostAnnotation, hostAnnotationValue),
					nil, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, annotation, false),
				debug:         true,
				expectRequeue: true,
				expectLabelsync: map[string]string{
					"foo.metal3.io/bar": "blue",
				},
			}),
			Entry("No errors for hostclaim case", testCaseReconcile{
				hardwaredata: newHardwareData(baremetalhostName, testLabels),
				hostclaim:    newHostClaim(hostClaimName, nil, nil, false, baremetalhostName),
				metal3Machine: newMetal3Machine(metal3machineName,
					m3mObjectMetaWithOwnerRefWithAnnotations(baremetal.HostClaimAnnotation, hostClaimAnnotationValue),
					m3mForHostClaimSpec, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, annotation, false),
				debug:         true,
				expectRequeue: true,
				expectLabelsync: map[string]string{
					"foo.metal3.io/bar": "blue",
				},
			}),
			Entry("No hostclaim for hostclaim case", testCaseReconcile{
				metal3Machine: newMetal3Machine(metal3machineName,
					m3mObjectMetaWithOwnerRefWithAnnotations(baremetal.HostClaimAnnotation, hostClaimAnnotationValue),
					m3mForHostClaimSpec, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, annotation, false),
			}),
			Entry("paused annotation on hostclaim for hostclaim case", testCaseReconcile{
				hostclaim: newHostClaim(hostClaimName, nil, nil, true, baremetalhostName),
				metal3Machine: newMetal3Machine(metal3machineName,
					m3mObjectMetaWithOwnerRefWithAnnotations(baremetal.HostClaimAnnotation, hostClaimAnnotationValue),
					m3mForHostClaimSpec, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, annotation, false),
				expectRequeue: true,
			}),
			Entry("No hardwaredata for hostclaim case", testCaseReconcile{
				hostclaim: newHostClaim(hostClaimName, nil, nil, false, baremetalhostName),
				metal3Machine: newMetal3Machine(metal3machineName,
					m3mObjectMetaWithOwnerRefWithAnnotations(baremetal.HostClaimAnnotation, hostClaimAnnotationValue),
					m3mForHostClaimSpec, nil, false),
				machine:       newMachine(clusterName, machineName, metal3machineName, nodeName),
				cluster:       newCluster(clusterName, nil, nil),
				metal3Cluster: newMetal3Cluster(metal3ClusterName, bmcOwnerRef(), bmcSpec(), nil, annotation, false),
				expectRequeue: true,
			}),
		)
		type TestCaseReconcileBMHLabels struct {
			PrefixSet   map[string]struct{}
			Host        *bmov1alpha1.BareMetalHost
			Machine     *clusterv1.Machine
			Cluster     *clusterv1.Cluster
			ExpectError bool
		}

		DescribeTable("Test reconcileBMHLabels",
			func(tc TestCaseReconcileBMHLabels) {
				objects := []client.Object{
					tc.Host,
					tc.Cluster,
					tc.Machine,
				}
				fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
				corev1Client := clientfake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name: nodeName,
				}}).CoreV1()
				r := &Metal3LabelSyncReconciler{
					Client:         fakeClient,
					ManagerFactory: baremetal.NewManagerFactory(fakeClient, nil),
					Log:            logr.Discard(),
					CapiClientGetter: func(_ context.Context, _ client.Client, _ *clusterv1.Cluster) (
						clientcorev1.CoreV1Interface, error,
					) {
						return corev1Client, nil
					},
					WatchFilterValue: "",
				}
				err := r.reconcileBMHLabels(context.TODO(),
					tc.Host.Labels, tc.Machine, tc.Cluster, tc.PrefixSet, logr.Discard())

				if tc.ExpectError {
					Expect(err).To(HaveOccurred())
				} else {
					Expect(err).NotTo(HaveOccurred())
				}
			},
			Entry("No errors", TestCaseReconcileBMHLabels{
				PrefixSet: map[string]struct{}{
					"foo.metal3.io": {},
				},
				Host:    newBareMetalHost(baremetalhostName, nil, nil, testLabels, false),
				Machine: newMachine(clusterName, machineName, metal3machineName, nodeName),
				Cluster: newCluster(clusterName, nil, nil),
			}),
		)
	})
})

func m3mObjectMeta() *metav1.ObjectMeta {
	return &metav1.ObjectMeta{
		Name:            metal3machineName,
		Namespace:       namespaceName,
		OwnerReferences: m3mOwnerRefs(),
		Labels: map[string]string{
			clusterv1.ClusterNameLabel: clusterName,
		},
		Annotations: map[string]string{
			baremetal.HostAnnotation: namespaceName + "/" + baremetalhostName,
		},
	}
}
