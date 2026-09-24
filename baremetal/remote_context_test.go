/*
Copyright 2026 Orange.

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

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/goleak"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A fake but valid kubeconfig with two contexts.
const validKubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://remote-api:6443
  name: remote
contexts:
- context:
    cluster: remote
    user: admin
    namespace: default-ns
  name: default-context
- context:
    cluster: remote
    user: admin
    namespace: other-ns
  name: other-context
current-context: default-context
users:
- name: admin
  user: {}
`

func kubeconfigSecret(name, namespace string, data []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string][]byte{"kubeconfig": data},
	}
}

func machineWithIdentity(name, secretName, contextName string) *infrav1.Metal3Machine {
	return &infrav1.Metal3Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespaceName,
		},
		Spec: infrav1.Metal3MachineSpec{
			IdentityRef: infrav1.IdentityRef{
				Name:    secretName,
				Context: contextName,
			},
		},
	}
}

var _ = Describe("Remote client cache", func() {
	log := logr.Discard()

	type testCaseLoadRemoteClient struct {
		secret            *corev1.Secret
		m3m               *infrav1.Metal3Machine
		expectError       bool
		expectedNamespace string
	}
	DescribeTable("loadRemoteClient",
		func(tc testCaseLoadRemoteClient) {
			scheme := setupScheme()
			// wrapper handles nil secret
			cl := fakeClient(tc.secret)
			c, ns, err := createRemoteClient(context.TODO(), logr.Discard(), scheme, cl, tc.m3m)
			if tc.expectError {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(ns).To(Equal(tc.expectedNamespace))
				if tc.expectedNamespace != "" {
					Expect(c).NotTo(BeNil())
				}
			}
		},
		Entry("No identityRef", testCaseLoadRemoteClient{
			m3m:         machineWithIdentity(metal3machineName, "", ""),
			expectError: true,
		}),
		Entry("Error if no kubeconfig", testCaseLoadRemoteClient{
			m3m:         machineWithIdentity(metal3machineName, "missing", ""),
			expectError: true,
		}),
		Entry("Error if no data entry in kubeconfig", testCaseLoadRemoteClient{
			m3m: machineWithIdentity(metal3machineName, "no-key", ""),
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-key",
					Namespace: namespaceName,
				},
				Data: map[string][]byte{"other": nil},
			},
			expectError: true,
		}),
		Entry("error if malformed content", testCaseLoadRemoteClient{
			secret:      kubeconfigSecret("bad", namespaceName, []byte("***")),
			m3m:         machineWithIdentity(metal3machineName, "bad", ""),
			expectError: true,
		}),
		Entry("error if wrong context name", testCaseLoadRemoteClient{
			secret:      kubeconfigSecret("valid", namespaceName, []byte(validKubeconfig)),
			m3m:         machineWithIdentity(metal3machineName, "valid", "no-context"),
			expectError: true,
		}),
		Entry("successful - default context", testCaseLoadRemoteClient{
			secret:            kubeconfigSecret("valid", namespaceName, []byte(validKubeconfig)),
			m3m:               machineWithIdentity(metal3machineName, "valid", ""),
			expectedNamespace: "default-ns",
		}),
		Entry("successful - named context", testCaseLoadRemoteClient{
			secret:            kubeconfigSecret("valid", namespaceName, []byte(validKubeconfig)),
			m3m:               machineWithIdentity(metal3machineName, "valid", "other-context"),
			expectedNamespace: "other-ns",
		}),
	)

	Describe("GetRemoteClient / Release", func() {
		var (
			rcc               *RemoteClientCache
			fakeWatches       map[string]*watch.FakeWatcher
			cancelStart       context.CancelFunc
			ignorePreExisting goleak.Option
		)

		// newBuilder returns a fake remoteClientBuilder. client is kept defined.
		newBuilder := func() remoteClientBuilder {
			return func(_, _ context.Context, _ logr.Logger, _ *runtime.Scheme,
				_ client.Client, m3m *infrav1.Metal3Machine,
			) (client.WithWatch, string, watch.Interface, error) {
				w := watch.NewFake()

				fakeWatches[m3m.Name] = w
				return nil, "remote-ns", w, nil
			}
		}

		BeforeEach(func() {
			// Snapshot the goroutines already running (envtest, klog, prior
			// specs' stragglers) so goleak.Find only flags goroutines this
			// spec created and failed to clean up.
			ignorePreExisting = goleak.IgnoreCurrent()

			fakeWatches = map[string]*watch.FakeWatcher{}
			rcc, _ = NewRemoteClientCache(fakeClient()).(*RemoteClientCache)
			rcc.remoteClientBuilder = newBuilder()

			// Emulate the manager starting the controller
			var startCtx context.Context
			startCtx, cancelStart = context.WithCancel(context.Background())
			rcc.ctx = startCtx
			go func() {
				defer GinkgoRecover()
				Expect(rcc.Start(startCtx)).To(Succeed())
			}()
		})

		AfterEach(func() {
			for _, w := range fakeWatches {
				if !w.IsStopped() {
					w.Stop()
				}
			}
			cancelStart()
			Eventually(func() error {
				return goleak.Find(ignorePreExisting)
			}, "2s", "100ms").Should(Succeed())
		})

		It("when IdentityRef is empty, returns nil", func() {
			m3m := machineWithIdentity(metal3machineName, "", "")
			rc, err := rcc.GetRemoteClient(context.TODO(), log, m3m)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc).To(BeNil())
			Expect(rcc.RemoteClients).To(BeEmpty())
		})

		It("creates one entry and registers the consumer on first call", func() {
			m3m := machineWithIdentity("m1", "kc", "")
			rc, err := rcc.GetRemoteClient(context.TODO(), log, m3m)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc).NotTo(BeNil())
			Expect(rcc.RemoteClients).To(HaveLen(1))
			Expect(rc.Namespace).To(Equal("remote-ns"))
			Expect(rc.Consumers).To(HaveKey(namespaceName + "/m1"))
		})

		It("reuses the cached entry for a different machine with the same key", func() {
			m3m1 := machineWithIdentity("m1", "kc", "")
			m3m2 := machineWithIdentity("m2", "kc", "")
			rc1, err := rcc.GetRemoteClient(context.TODO(), log, m3m1)
			Expect(err).NotTo(HaveOccurred())
			rc2, err := rcc.GetRemoteClient(context.TODO(), log, m3m2)
			Expect(err).NotTo(HaveOccurred())
			Expect(rc1).To(BeIdenticalTo(rc2))
			Expect(rcc.RemoteClients).To(HaveLen(1))
			Expect(rc1.Consumers).To(HaveLen(2))
		})

		It("keeps different (secret, context) pairs in separate entries", func() {
			a := machineWithIdentity("m1", "kc", "ctxA")
			b := machineWithIdentity("m2", "kc", "ctxB")
			rc1, err := rcc.GetRemoteClient(context.TODO(), log, a)
			Expect(err).NotTo(HaveOccurred())
			rc2, err := rcc.GetRemoteClient(context.TODO(), log, b)
			Expect(err).NotTo(HaveOccurred())
			Expect(rcc.RemoteClients).To(HaveLen(2))
			Expect(rc1.Consumers).To(HaveLen(1))
			Expect(rc2.Consumers).To(HaveLen(1))
		})

		It("propagates builder error", func() {
			rcc.remoteClientBuilder = func(_, _ context.Context, _ logr.Logger, _ *runtime.Scheme,
				_ client.Client, _ *infrav1.Metal3Machine,
			) (client.WithWatch, string, watch.Interface, error) {
				return nil, "", nil, apierrors.NewInternalError(context.Canceled)
			}
			m3m := machineWithIdentity("m1", "kc", "")
			rc, err := rcc.GetRemoteClient(context.TODO(), log, m3m)
			Expect(err).To(HaveOccurred())
			Expect(rc).To(BeNil())
			Expect(rcc.RemoteClients).To(BeEmpty())
		})

		It("partial release - keeps entry", func() {
			m3m1 := machineWithIdentity("m1", "kc", "")
			m3m2 := machineWithIdentity("m2", "kc", "")
			rc, err := rcc.GetRemoteClient(context.TODO(), log, m3m1)
			Expect(err).NotTo(HaveOccurred())
			_, err = rcc.GetRemoteClient(context.TODO(), log, m3m2)
			Expect(err).NotTo(HaveOccurred())

			rc.Release(m3m1)
			Expect(rcc.RemoteClients).To(HaveLen(1))
			Expect(rc.Consumers).To(HaveLen(1))
			Expect(rc.Consumers).NotTo(HaveKey(namespaceName + "/m1"))
			Expect(fakeWatches["m1"].IsStopped()).To(BeFalse())
		})

		It("full release", func() {
			m3m := machineWithIdentity("m1", "kc", "")
			rc, err := rcc.GetRemoteClient(context.TODO(), log, m3m)
			Expect(err).NotTo(HaveOccurred())

			rc.Release(m3m)
			Expect(rcc.RemoteClients).To(BeEmpty())
			Eventually(fakeWatches["m1"].IsStopped).Should(BeTrue())
		})

		It("forwards HostClaim events to the watch channel", func() {
			m3m := machineWithIdentity("m1", "kc", "")
			ch := rcc.GetWatchChannel()
			_, err := rcc.GetRemoteClient(context.TODO(), log, m3m)
			Expect(err).NotTo(HaveOccurred())

			hc := &bmov1alpha1.HostClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "hc", Namespace: "remote-ns"},
			}
			go fakeWatches["m1"].Modify(hc)

			var evt = <-ch
			Expect(evt.Object).To(BeAssignableToTypeOf(&bmov1alpha1.HostClaim{}))
			Expect(evt.Object.GetName()).To(Equal("hc"))
		})

		It("filters out non-HostClaim events", func() {
			m3m := machineWithIdentity("m1", "kc", "")
			ch := rcc.GetWatchChannel()
			_, err := rcc.GetRemoteClient(context.TODO(), log, m3m)
			Expect(err).NotTo(HaveOccurred())

			// A ConfigMap should never reach the watch channel; follow it with a
			// HostClaim so we can assert the first event we receive is the second one.
			go func() {
				fakeWatches["m1"].Modify(&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: "configmap"},
				})
				fakeWatches["m1"].Modify(&bmov1alpha1.HostClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "hc"},
				})
			}()
			evt := <-ch
			Expect(evt.Object.GetName()).To(Equal("hc"))
		})
	})
})
