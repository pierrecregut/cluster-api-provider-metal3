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
	"errors"

	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type dataGetterTestCase struct {
	Metal3Machine      *infrav1.Metal3Machine
	BareMetalHost      *bmov1alpha1.BareMetalHost
	HardwareData       *bmov1alpha1.HardwareData
	HostClaim          *bmov1alpha1.HostClaim
	FailingRemoteCache bool
	NilRemoteCache     bool
	ExpectError        bool
	ExpectNil          bool
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
			var rcc RemoteClientCacheInterface
			switch {
			case tc.NilRemoteCache:
				rcc = nil
			case tc.FailingRemoteCache:
				rcc = &MockRemote{err: errors.New("remote boom")}
			default:
				rcc = MockRemoteContext()
			}
			getter, err := NewDataGetter(context.TODO(), rcc, tc.Metal3Machine, fakeClient, GinkgoLogr)
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
				hcDg, ok := getter.(*HostClaimDataGetter)
				Expect(ok).To(BeTrue())
				Expect(hcDg.hostclaim).To(Equal(tc.HostClaim))
				Expect(hcDg.hardwareData).To(Equal(tc.HardwareData))
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
					HardwareData: &bmov1alpha1.ObjectReference{
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
					HardwareData: &bmov1alpha1.ObjectReference{
						Name:      "hwd",
						Namespace: "ns2",
					},
				},
			},
			ExpectError: true,
		}),
		Entry("Malformed BMH annotation key", dataGetterTestCase{
			Metal3Machine: metal3Machine(map[string]string{HostAnnotation: "ns/extra/bmh"}),
			ExpectError:   true,
		}),
		Entry("HostClaim mode without a remote client cache", dataGetterTestCase{
			Metal3Machine:  metal3Machine(map[string]string{HostClaimAnnotation: "hc"}),
			NilRemoteCache: true,
			ExpectError:    true,
		}),
		Entry("HostClaim mode with a failing remote client cache", dataGetterTestCase{
			Metal3Machine:      metal3Machine(map[string]string{HostClaimAnnotation: "hc"}),
			FailingRemoteCache: true,
			ExpectError:        true,
		}),
	)
})

var (
	nicname = "ens3"
	mac     = "aa:bb:cc:dd:ee:ff"
)

// Fixtures for the DataGetter contract matrix. Keys/values are shared across
// modes so the same op entries assert the same thing in each mode.
var (
	dgBMH = &BMHDataGetter{
		bmh: &bmov1alpha1.BareMetalHost{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "bmh",
				Namespace:   "ns",
				Annotations: map[string]string{"a": "v1"},
				Labels:      map[string]string{"l": "v2"},
			},
			Spec: bmov1alpha1.BareMetalHostSpec{BootMACAddress: "boot-mac"},
			Status: bmov1alpha1.BareMetalHostStatus{
				HardwareDetails: &bmov1alpha1.HardwareDetails{
					NIC: []bmov1alpha1.NIC{{Name: nicname, MAC: mac}},
				},
			},
		},
	}
	dgBMHNil = &BMHDataGetter{bmh: nil}

	dgHostClaim = &HostClaimDataGetter{
		hostclaim: &bmov1alpha1.HostClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "hc",
				Namespace:   "ns",
				Annotations: map[string]string{"ac": "vc1"},
				Labels:      map[string]string{"lc": "vc2"},
			},
			Status: bmov1alpha1.HostClaimStatus{
				BareMetalHost: &bmov1alpha1.ObjectReference{Name: "bmh"},
			},
		},
		hardwareData: &bmov1alpha1.HardwareData{
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
	}
	dgHostClaimNoBMH = &HostClaimDataGetter{
		hostclaim: &bmov1alpha1.HostClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "hc", Namespace: "ns"},
		},
		hardwareData: &bmov1alpha1.HardwareData{},
	}
	dgHostClaimNoHWD = &HostClaimDataGetter{
		hostclaim: &bmov1alpha1.HostClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "hc", Namespace: "ns"},
			Status: bmov1alpha1.HostClaimStatus{
				BareMetalHost: &bmov1alpha1.ObjectReference{Name: "bmh"},
			},
		},
	}
	dgHostClaimNil = &HostClaimDataGetter{}
)

// dgOp is a reusable assertion closure. Each op checks a single DataGetter
// behavior; the matrix table pairs each op with the modes that should
// satisfy it.
type dgOp func(dg DataGetter)

var (
	opGetName = func(expected string) dgOp {
		return func(dg DataGetter) {
			v, err := dg.GetName()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(expected))
		}
	}
	opGetNameError = func(dg DataGetter) {
		_, err := dg.GetName()
		Expect(err).To(HaveOccurred())
	}
	opGetHostName = func(expected string) dgOp {
		return func(dg DataGetter) {
			v, err := dg.GetHostName()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal(expected))
		}
	}
	opGetHostNameError = func(dg DataGetter) {
		_, err := dg.GetHostName()
		Expect(err).To(HaveOccurred())
	}
	opLabelPresent = func(dg DataGetter) {
		v, err := dg.GetLabel("l")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("v2"))
	}
	opLabelError = func(dg DataGetter) {
		_, err := dg.GetLabel("l")
		Expect(err).To(HaveOccurred())
	}
	opAnnotationPresent = func(dg DataGetter) {
		v, err := dg.GetAnnotation("a")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("v1"))
	}
	opAnnotationError = func(dg DataGetter) {
		_, err := dg.GetAnnotation("a")
		Expect(err).To(HaveOccurred())
	}
	opHostClaimLabelPresent = func(dg DataGetter) {
		v, err := dg.GetHostClaimLabel("lc")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("vc2"))
	}
	opHostClaimLabelError = func(dg DataGetter) {
		_, err := dg.GetHostClaimLabel("lc")
		Expect(err).To(HaveOccurred())
	}
	opHostClaimAnnotationPresent = func(dg DataGetter) {
		v, err := dg.GetHostClaimAnnotation("ac")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("vc1"))
	}
	opHostClaimAnnotationError = func(dg DataGetter) {
		_, err := dg.GetHostClaimAnnotation("ac")
		Expect(err).To(HaveOccurred())
	}
	opNICsOne = func(dg DataGetter) {
		nics := dg.GetNICs()
		Expect(nics).To(HaveLen(1))
		Expect(nics[nicname]).To(Equal(mac))
	}
	opNICsEmpty = func(dg DataGetter) {
		Expect(dg.GetNICs()).To(BeEmpty())
	}
	opBootMAC = func(expected string) dgOp {
		return func(dg DataGetter) {
			Expect(dg.GetBootMACAddress()).To(Equal(expected))
		}
	}
)

var _ = DescribeTable("DataGetter contract",
	func(dg DataGetter, op dgOp) { op(dg) },
	// BareMetalHost mode
	Entry("BMH / GetName", dgBMH, opGetName("bmh")),
	Entry("BMH / GetHostName falls back to BMH name", dgBMH, opGetHostName("bmh")),
	Entry("BMH / GetLabel present", dgBMH, opLabelPresent),
	Entry("BMH / GetAnnotation present", dgBMH, opAnnotationPresent),
	Entry("BMH / GetHostClaimLabel errors in BMH mode", dgBMH, opHostClaimLabelError),
	Entry("BMH / GetHostClaimAnnotation errors in BMH mode", dgBMH, opHostClaimAnnotationError),
	Entry("BMH / GetNICs", dgBMH, opNICsOne),
	Entry("BMH / GetBootMACAddress", dgBMH, opBootMAC("boot-mac")),
	Entry("BMH / nil receiver: GetName errors", dgBMHNil, opGetNameError),
	Entry("BMH / nil receiver: GetLabel errors", dgBMHNil, opLabelError),
	Entry("BMH / nil receiver: GetAnnotation errors", dgBMHNil, opAnnotationError),
	Entry("BMH / nil receiver: GetBootMACAddress is empty", dgBMHNil, opBootMAC("")),

	// HostClaim mode
	Entry("HostClaim / GetName returns bound BMH name", dgHostClaim, opGetName("bmh")),
	Entry("HostClaim / GetHostName returns claim name", dgHostClaim, opGetHostName("hc")),
	Entry("HostClaim / GetLabel reads HardwareData labels", dgHostClaim, opLabelPresent),
	Entry("HostClaim / GetAnnotation reads HardwareData annotations", dgHostClaim, opAnnotationPresent),
	Entry("HostClaim / GetHostClaimLabel reads claim labels", dgHostClaim, opHostClaimLabelPresent),
	Entry("HostClaim / GetHostClaimAnnotation reads claim annotations", dgHostClaim, opHostClaimAnnotationPresent),
	Entry("HostClaim / GetNICs reads HardwareData NICs", dgHostClaim, opNICsOne),
	Entry("HostClaim / GetBootMACAddress is always empty", dgHostClaim, opBootMAC("")),
	Entry("HostClaim / not yet bound: GetName errors (transient)", dgHostClaimNoBMH, opGetNameError),
	Entry("HostClaim / nil hostclaim: GetHostName errors", dgHostClaimNil, opGetHostNameError),
	Entry("HostClaim / nil hostclaim: GetHostClaimLabel errors", dgHostClaimNil, opHostClaimLabelError),
	Entry("HostClaim / nil hostclaim: GetHostClaimAnnotation errors", dgHostClaimNil, opHostClaimAnnotationError),
	Entry("HostClaim / nil hardwareData: GetLabel errors", dgHostClaimNoHWD, opLabelError),
	Entry("HostClaim / nil hardwareData: GetAnnotation errors", dgHostClaimNoHWD, opAnnotationError),
	Entry("HostClaim / nil hardwareData: GetNICs is empty", dgHostClaimNoHWD, opNICsEmpty),
)
