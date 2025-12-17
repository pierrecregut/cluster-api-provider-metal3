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

	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type dataGetterTestCase struct {
	Metal3Machine *infrav1.Metal3Machine
	BareMetalHost *bmov1alpha1.BareMetalHost
	HardwareData  *bmov1alpha1.HardwareData
	HostClaim     *bmov1alpha1.HostClaim
	ExpectError   bool
	ExpectNil     bool
}

func metal3Machine(annotations map[string]string) *infrav1.Metal3Machine {
	return &infrav1.Metal3Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "m3m",
			Namespace:   "ns",
			Annotations: annotations,
		},
	}
}

var _ = Describe("Data Getter", func() {
	DescribeTable("Builder tests",
		func(tc dataGetterTestCase) {
			var objects []client.Object
			if tc.Metal3Machine != nil {
				objects = append(objects, tc.Metal3Machine)
			}
			if tc.BareMetalHost != nil {
				objects = append(objects, tc.BareMetalHost)
			}
			if tc.HostClaim != nil {
				objects = append(objects, tc.HostClaim)
			}
			if tc.HardwareData != nil {
				objects = append(objects, tc.HardwareData)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(objects...).Build()
			getter, err := getDataHost(context.TODO(), tc.Metal3Machine, fakeClient, GinkgoLogr)
			if tc.ExpectError {
				Expect(err).To(HaveOccurred())
				return
			}
			Expect(err).NotTo(HaveOccurred())
			if tc.ExpectNil {
				Expect(getter).To(BeNil())
				return
			}
			if tc.HostClaim != nil {
				hcDg, ok := getter.(*HostDataGetter)
				Expect(ok).To(BeTrue())
				Expect(hcDg.hostclaimName).To(Equal(tc.HostClaim.Name))
				Expect(hcDg.hardwareData).To(Equal(*tc.HardwareData))
				return
			}
			bmhDg, ok := getter.(*BMHDataGetter)
			Expect(ok).To(BeTrue())
			Expect(bmhDg.bmh).To(Equal(tc.BareMetalHost))
		},
		Entry("No annotation", dataGetterTestCase{
			Metal3Machine: metal3Machine(nil),
			ExpectNil:     true,
		}),
		Entry("No expected annotation", dataGetterTestCase{
			Metal3Machine: metal3Machine(map[string]string{"a": "b"}),
			ExpectNil:     true,
		}),
		Entry("Standard BareMetalHost", dataGetterTestCase{
			Metal3Machine: metal3Machine(map[string]string{HostAnnotation: "ns/bmh"}),
			BareMetalHost: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bmh",
					Namespace: "ns",
				},
			},
		}),
		Entry("BareMetalHost case without BMH", dataGetterTestCase{
			Metal3Machine: metal3Machine(map[string]string{HostAnnotation: "ns/bmh"}),
			ExpectNil:     true,
		}),
		Entry("Standard HostClaim (local)", dataGetterTestCase{
			Metal3Machine: metal3Machine(map[string]string{HostClaimAnnotation: "hc"}),
			HostClaim: &bmov1alpha1.HostClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hc",
					Namespace: "ns",
				},
				Status: bmov1alpha1.HostClaimStatus{
					HardwareData: &bmov1alpha1.HardwareReference{
						Name:      "hwd",
						Namespace: "ns2",
					},
				},
			},
			HardwareData: &bmov1alpha1.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hwd",
					Namespace: "ns2",
				},
			},
		}),
		Entry("HostClaim case without hostclaim", dataGetterTestCase{
			Metal3Machine: metal3Machine(map[string]string{HostClaimAnnotation: "hc"}),
			ExpectNil:     true,
		}),
		Entry("HostClaim case without hardware data", dataGetterTestCase{
			Metal3Machine: metal3Machine(map[string]string{HostClaimAnnotation: "hc"}),
			HostClaim: &bmov1alpha1.HostClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hc",
					Namespace: "ns",
				},
			},
			ExpectNil: true,
		}),
		Entry("HostClaim case missing hardware data", dataGetterTestCase{
			Metal3Machine: metal3Machine(map[string]string{HostClaimAnnotation: "hc"}),
			HostClaim: &bmov1alpha1.HostClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hc",
					Namespace: "ns",
				},
				Status: bmov1alpha1.HostClaimStatus{
					HardwareData: &bmov1alpha1.HardwareReference{
						Name:      "hwd",
						Namespace: "ns2",
					},
				},
			},
			ExpectError: true,
		}),
	)
})

var (
	nicname = "ens3"
	mac     = "aa:bb:cc:dd:ee:ff"
)

var _ = DescribeTable("BMH Data Getter",
	func(dg DataGetter, expectedHostname string) {
		v, err := dg.GetAnnotation("a")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("v1"))
		v, err = dg.GetLabel("l")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("v2"))
		v = dg.GetHostName()
		Expect(v).To(Equal(expectedHostname))
		v = dg.GetName()
		Expect(v).To(Equal("bmh"))
		nics := dg.GetNICs()
		Expect(nics).To(HaveLen(1))
		Expect(nicname).To(BeKeyOf(nics))
		Expect(nics[nicname]).To(Equal(mac))
	},
	Entry(
		"BMH DataGetter",
		&BMHDataGetter{
			bmh: &bmov1alpha1.BareMetalHost{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "bmh",
					Namespace:   "ns",
					Annotations: map[string]string{"a": "v1"},
					Labels:      map[string]string{"l": "v2"},
				},
				Status: bmov1alpha1.BareMetalHostStatus{
					HardwareDetails: &bmov1alpha1.HardwareDetails{
						NIC: []bmov1alpha1.NIC{{Name: nicname, MAC: mac}},
					},
				},
			},
		},
		"bmh"),
	Entry(
		"HostClaim Data Getter",
		&HostDataGetter{
			hostclaimName: "hc",
			hardwareData: bmov1alpha1.HardwareData{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "bmh",
					Namespace:   "ns",
					Annotations: map[string]string{"a": "v1"},
					Labels:      map[string]string{"l": "v2"},
				},
				Spec: bmov1alpha1.HardwareDataSpec{
					HardwareDetails: &bmov1alpha1.HardwareDetails{
						NIC: []bmov1alpha1.NIC{{Name: nicname, MAC: mac}},
					},
				},
			},
		},
		"hc"),
)
