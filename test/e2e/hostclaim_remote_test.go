// Loopback remote-HostClaim test.
//
// This test exercises CAPM3's remote-HostClaim code path
// (RemoteClientCache / Watch / syncSecretData / remoteMetal3MachineKind /
// cross-namespace HardwareData read) without a second physical Kubernetes
// cluster. The kubeconfig CAPM3 consumes points back at the bootstrap
// cluster's own API server via the in-cluster DNS name
// `kubernetes.default.svc.cluster.local:443`, using a bearer token minted
// for a scoped ServiceAccount whose write scope is limited to a per-tenant
// namespace and whose HardwareData read is granted cluster-wide.
//
// Three namespaces are involved:
//   - `metal3`         (BMH namespace)     — owned by the infra operator; holds
//     BareMetalHosts, HostDeployPolicy, HardwareData. Tenant read-only.
//   - `cluster`        (CAPI namespace)    — holds the workload cluster's CAPI
//     and CAPM3 objects plus the kubeconfig Secret.
//   - `tenant-test1`   (HostClaim ns)      — new, owned by the tenant. Holds the
//     ServiceAccount, its RBAC, the token, the HostClaims CAPM3 creates via
//     the loopback client, and the copied user-data / metadata Secrets.
//
// Caveat: bugs that only manifest against a genuinely different API server
// (TLS SNI, watch reconnect on a different endpoint) will slip through.

package e2e

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// bmhNamespace is the namespace hosting BMHs and HardwareData. Same as
	// the package-level `namespace` var, but named explicitly here to make
	// the tenant/BMH split readable inline.
	bmhNamespace = "metal3"
	// hostclaimTenantNamespace is the per-tenant/per-cluster namespace where
	// HostClaims and copied Secrets land. In production one namespace per
	// cluster (or per tenant owning a group of clusters); here we hardcode
	// one to keep the test simple.
	hostclaimTenantNamespace = "tenant-test1"

	loopbackKubeconfigSecretName = "hostclaim-loopback-kubeconfig"
	loopbackServiceAccountName   = "hostclaim-agent"
	loopbackTokenSecretName      = "hostclaim-agent-token"
	loopbackClusterRoleName      = "hostclaim-agent-hardwaredata-reader"
	// Every pod on the bootstrap cluster can reach its own API server here,
	// with the CA baked into the service-account token secret.
	loopbackAPIServer       = "https://kubernetes.default.svc.cluster.local:443"
	remoteMetal3MachineKind = "RemoteMetal3Machine"
)

var _ = Describe("When testing cluster creation with hostclaims over a loopback remote kubeconfig", Label("hostclaim", "hostclaim-remote"), func() {
	BeforeEach(func() {
		osType = strings.ToLower(os.Getenv("OS"))
		Expect(osType).ToNot(Equal(""))
		validateGlobals(specName)

		// Redirect clusterctl apply log folder to avoid recording credentials
		// in the shared artifact folder.
		clusterctlLogFolder = filepath.Join(os.TempDir(), "target_cluster_logs", bootstrapClusterProxy.GetName())
	})

	It("Should create a workload cluster whose HostClaims land through a scoped kubeconfig", func() {
		By("Apply BMH for workload cluster")
		ApplyBmh(ctx, e2eConfig, bootstrapClusterProxy, namespace, specName)

		By("Create a suitable host deploy policy")
		Expect(BuildAndApplyKustomization(ctx, &BuildAndApplyKustomizationInput{
			Kustomization: "data/hostclaims/host-deploy-policy",
			ClusterProxy:  bootstrapClusterProxy,
		})).To(Succeed())

		By("Create tenant namespace, ServiceAccount, Role, RoleBinding, ClusterRole, ClusterRoleBinding and token Secret")
		Expect(BuildAndApplyKustomization(ctx, &BuildAndApplyKustomizationInput{
			Kustomization: "data/hostclaims/loopback-identity",
			ClusterProxy:  bootstrapClusterProxy,
		})).To(Succeed())

		By("Create cluster namespace")
		clusterClientSet := bootstrapClusterProxy.GetClientSet()
		clusterNamespaceObj := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: clusterNamespace},
		}
		_, err := clusterClientSet.CoreV1().Namespaces().Create(ctx, clusterNamespaceObj, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			Expect(err).ToNot(HaveOccurred(), "Unable to create the cluster namespace")
		}

		By("Wait for the ServiceAccount token to be populated and build the loopback kubeconfig secret")
		BuildLoopbackKubeconfigSecret(ctx,
			bootstrapClusterProxy,
			hostclaimTenantNamespace,
			loopbackTokenSecretName,
			clusterNamespace,
			loopbackKubeconfigSecretName,
			hostclaimTenantNamespace,
			loopbackAPIServer,
		)

		By("Provision Workload cluster")
		k8sVersion := e2eConfig.MustGetVariable("KUBERNETES_VERSION")
		targetCluster, _ = CreateTargetCluster(ctx, func() CreateTargetClusterInput {
			return CreateTargetClusterInput{
				E2EConfig:             e2eConfig,
				BootstrapClusterProxy: bootstrapClusterProxy,
				SpecName:              specName,
				ClusterName:           clusterName,
				K8sVersion:            k8sVersion,
				KCPMachineCount:       int64(numberOfControlplane),
				WorkerMachineCount:    int64(numberOfWorkers),
				ClusterctlLogFolder:   clusterctlLogFolder,
				ClusterctlConfigPath:  clusterctlConfigPath,
				OSType:                osType + "-hostclaims-remote",
				Namespace:             clusterNamespace,
			}
		})

		By("Assert HostClaims land in the tenant namespace with the remote consumer kind")
		AssertRemoteHostClaimsBound(ctx, bootstrapClusterProxy,
			clusterNamespace, hostclaimTenantNamespace, bmhNamespace, numberOfAllBmh)

		By("Assert the tenant SA cannot reach into the BMH namespace")
		AssertTenantIsolation(ctx, bootstrapClusterProxy,
			hostclaimTenantNamespace, loopbackTokenSecretName, bmhNamespace)

		By("HOSTCLAIM REMOTE TESTS PASSED!")
	})

	AfterEach(func() {
		ListBareMetalHosts(ctx, bootstrapClusterProxy.GetClient(), client.InNamespace(bmhNamespace))
		ListHostClaims(ctx, bootstrapClusterProxy.GetClient(), client.InNamespace(hostclaimTenantNamespace))
		ListMetal3Machines(ctx, bootstrapClusterProxy.GetClient(), client.InNamespace(clusterNamespace))
		ListMachines(ctx, bootstrapClusterProxy.GetClient(), client.InNamespace(clusterNamespace))
		ListNodes(ctx, targetCluster.GetClient())

		DumpSpecResourcesAndCleanup(ctx, specName, bootstrapClusterProxy, targetCluster, artifactFolder, clusterNamespace, e2eConfig.GetIntervals, clusterName, clusterctlLogFolder, skipCleanup, clusterctlConfigPath)
		if skipCleanup {
			return
		}
		// The tenant namespace holds the SA / Role / RoleBinding / token /
		// HostClaims / copied secrets; deleting it cascades everything inside.
		// The ClusterRole / ClusterRoleBinding are cluster-scoped and must be
		// removed separately.
		DeleteHostclaimLoopbackIdentity(ctx, bootstrapClusterProxy, hostclaimTenantNamespace, loopbackClusterRoleName)
	})
})

// BuildLoopbackKubeconfigSecret waits for a ServiceAccount token Secret to be
// populated by the token controller, then writes a Secret carrying a
// bearer-token kubeconfig that points at `apiServer`. The kubeconfig's
// current-context uses `contextNamespace` as its default namespace — that is
// what CAPM3's remote client picks up as the "remote" namespace where
// HostClaims and copied Secrets land.
//
// Only usable when CAPM3 runs in-cluster on the bootstrap cluster: the
// default `apiServer` is the in-cluster Kubernetes Service DNS name, which
// is not resolvable from outside the cluster.
func BuildLoopbackKubeconfigSecret(
	ctx context.Context,
	clusterProxy framework.ClusterProxy,
	tokenNamespace, tokenSecretName string,
	dstNamespace, dstSecretName string,
	contextNamespace, apiServer string,
) {
	cl := clusterProxy.GetClient()

	tokenSecret := &corev1.Secret{}
	Eventually(func(g Gomega) {
		g.Expect(cl.Get(ctx, client.ObjectKey{Namespace: tokenNamespace, Name: tokenSecretName}, tokenSecret)).To(Succeed())
		g.Expect(tokenSecret.Data).To(HaveKey("token"))
		g.Expect(tokenSecret.Data).To(HaveKey("ca.crt"))
		g.Expect(tokenSecret.Data["token"]).ToNot(BeEmpty())
		g.Expect(tokenSecret.Data["ca.crt"]).ToNot(BeEmpty())
	}, e2eConfig.GetIntervals(specName, "wait-controllers")...).Should(Succeed())

	// certificate-authority-data expects base64-encoded bytes; corev1.Secret
	// exposes .Data already decoded from wire, so we re-encode here.
	caB64 := base64.StdEncoding.EncodeToString(tokenSecret.Data["ca.crt"])
	token := string(tokenSecret.Data["token"])

	kubeconfig := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: loopback
  cluster:
    server: %s
    certificate-authority-data: %s
users:
- name: %s
  user:
    token: %s
contexts:
- name: default
  context:
    cluster: loopback
    user: %s
    namespace: %s
current-context: default
`, apiServer, caB64, loopbackServiceAccountName, token, loopbackServiceAccountName, contextNamespace)

	dst := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dstSecretName,
			Namespace: dstNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"kubeconfig": kubeconfig,
		},
	}
	err := cl.Create(ctx, dst)
	if apierrors.IsAlreadyExists(err) {
		existing := &corev1.Secret{}
		Expect(cl.Get(ctx, client.ObjectKeyFromObject(dst), existing)).To(Succeed())
		existing.StringData = dst.StringData
		existing.Data = nil
		Expect(cl.Update(ctx, existing)).To(Succeed())
	} else {
		Expect(err).ToNot(HaveOccurred(), "cannot create loopback kubeconfig secret")
	}
}

// AssertRemoteHostClaimsBound asserts the cross-namespace invariants of the
// remote-HostClaim flow:
//   - every HostClaim for this cluster lives in `tenantNamespace` (not in
//     `m3mNamespace`, not in `bmhNamespace`),
//   - each ConsumerRef is a RemoteMetal3Machine pointing back into
//     `m3mNamespace`,
//   - the copied `<hostClaim>-<role>` Secrets exist in `tenantNamespace`,
//     never in `bmhNamespace`,
//   - HostClaim.Status.HardwareData points at a real HardwareData in
//     `bmhNamespace` — that is the cross-namespace read CAPM3 makes via the
//     loopback client to render user-data / metadata / network-data.
func AssertRemoteHostClaimsBound(
	ctx context.Context,
	clusterProxy framework.ClusterProxy,
	m3mNamespace, tenantNamespace, bmhNamespace string,
	expectedCount int,
) {
	cl := clusterProxy.GetClient()

	Eventually(func(g Gomega) {
		inM3M := &bmov1alpha1.HostClaimList{}
		g.Expect(cl.List(ctx, inM3M, client.InNamespace(m3mNamespace))).To(Succeed())
		g.Expect(inM3M.Items).To(BeEmpty(), "unexpected HostClaim(s) in the CAPI namespace; every claim must land through the remote kubeconfig")

		inBMH := &bmov1alpha1.HostClaimList{}
		g.Expect(cl.List(ctx, inBMH, client.InNamespace(bmhNamespace))).To(Succeed())
		g.Expect(inBMH.Items).To(BeEmpty(), "unexpected HostClaim(s) in the BMH namespace; the tenant kubeconfig points at the tenant namespace")

		tenant := &bmov1alpha1.HostClaimList{}
		g.Expect(cl.List(ctx, tenant, client.InNamespace(tenantNamespace))).To(Succeed())
		g.Expect(tenant.Items).To(HaveLen(expectedCount), "expected one HostClaim per Metal3Machine in the tenant namespace")

		for i := range tenant.Items {
			hc := &tenant.Items[i]
			g.Expect(hc.Spec.ConsumerRef).ToNot(BeNil(), "HostClaim %s has no ConsumerRef", hc.Name)
			g.Expect(hc.Spec.ConsumerRef.Kind).To(Equal(remoteMetal3MachineKind), "HostClaim %s ConsumerRef.Kind should be %s", hc.Name, remoteMetal3MachineKind)
			g.Expect(hc.Spec.ConsumerRef.Namespace).To(Equal(m3mNamespace), "HostClaim %s ConsumerRef.Namespace should be %s", hc.Name, m3mNamespace)

			// BMO fills HardwareData with the BMH's own {namespace,name}, so
			// the reference must point into the BMH namespace. This is the
			// cross-namespace read the tenant SA's ClusterRole enables.
			g.Expect(hc.Status.HardwareData).ToNot(BeNil(), "HostClaim %s has no HardwareData reference yet", hc.Name)
			g.Expect(hc.Status.HardwareData.Namespace).To(Equal(bmhNamespace), "HostClaim %s HardwareData.Namespace should be %s", hc.Name, bmhNamespace)
			hwd := &bmov1alpha1.HardwareData{}
			g.Expect(cl.Get(ctx, client.ObjectKey{Namespace: hc.Status.HardwareData.Namespace, Name: hc.Status.HardwareData.Name}, hwd)).To(Succeed(),
				"HostClaim %s HardwareData %s/%s must exist in the BMH namespace",
				hc.Name, hc.Status.HardwareData.Namespace, hc.Status.HardwareData.Name)
		}
	}, e2eConfig.GetIntervals(specName, "wait-machine-running")...).Should(Succeed())

	// syncSecretData copies user-data / metadata (and optionally network-data)
	// as `<hostClaim>-<role>` in the tenant namespace, with the same Data as
	// the source Secret in the m3m namespace. None of these copies must
	// appear in the BMH namespace.
	tenant := &bmov1alpha1.HostClaimList{}
	Expect(cl.List(ctx, tenant, client.InNamespace(tenantNamespace))).To(Succeed())
	roles := []string{"userdata", "metadata"}
	for i := range tenant.Items {
		hc := &tenant.Items[i]
		for _, role := range roles {
			copyName := hc.Name + "-" + role
			Eventually(func(g Gomega) {
				copied := &corev1.Secret{}
				g.Expect(cl.Get(ctx, client.ObjectKey{Namespace: tenantNamespace, Name: copyName}, copied)).To(Succeed())
				g.Expect(copied.Data).ToNot(BeEmpty(), "copied secret %s/%s has no data", tenantNamespace, copyName)

				strayInBMH := &corev1.Secret{}
				err := cl.Get(ctx, client.ObjectKey{Namespace: bmhNamespace, Name: copyName}, strayInBMH)
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
					"copied secret %s/%s must not appear in the BMH namespace (found: %v)", bmhNamespace, copyName, err)
			}, e2eConfig.GetIntervals(specName, "wait-controllers")...).Should(Succeed(),
				fmt.Sprintf("expected copied secret %s/%s produced by syncSecretData", tenantNamespace, copyName))
		}
	}
}

// AssertTenantIsolation verifies via SelfSubjectAccessReview, using the
// tenant SA's own token, that the SA cannot reach into the BMH namespace.
// This is the negative side of the RBAC contract: even though the token
// works from CAPM3, it must not be usable as a springboard into `metal3`.
func AssertTenantIsolation(
	ctx context.Context,
	clusterProxy framework.ClusterProxy,
	tokenNamespace, tokenSecretName string,
	bmhNamespace string,
) {
	cl := clusterProxy.GetClient()
	tokenSecret := &corev1.Secret{}
	Expect(cl.Get(ctx, client.ObjectKey{Namespace: tokenNamespace, Name: tokenSecretName}, tokenSecret)).To(Succeed())
	Expect(tokenSecret.Data["token"]).ToNot(BeEmpty())

	// Fresh REST config using the SA bearer token; keep the same server URL
	// and CA as the ambient config so we hit the same API endpoint.
	base := clusterProxy.GetRESTConfig()
	scopedCfg := rest.CopyConfig(base)
	scopedCfg.BearerToken = string(tokenSecret.Data["token"])
	scopedCfg.BearerTokenFile = ""
	// Wipe any client-cert leftovers from the ambient config so the API server
	// picks up the bearer token and not the admin identity.
	scopedCfg.CertData = nil
	scopedCfg.KeyData = nil
	scopedCfg.CertFile = ""
	scopedCfg.KeyFile = ""
	scopedCfg.Username = ""
	scopedCfg.Password = ""
	scopedCfg.AuthProvider = nil
	scopedCfg.ExecProvider = nil
	scopedCfg.Impersonate = rest.ImpersonationConfig{}

	scopedCS, err := kubernetes.NewForConfig(scopedCfg)
	Expect(err).NotTo(HaveOccurred(), "cannot build kubernetes client from scoped SA token")

	// Every one of these must be refused for the tenant SA in the BMH
	// namespace. If any is allowed, the Role has drifted.
	forbidden := []authorizationv1.ResourceAttributes{
		{Namespace: bmhNamespace, Verb: "list", Group: "metal3.io", Resource: "baremetalhosts"},
		{Namespace: bmhNamespace, Verb: "create", Resource: "secrets"},
		{Namespace: bmhNamespace, Verb: "get", Group: "metal3.io", Resource: "hostdeploypolicies"},
	}
	for _, r := range forbidden {
		review := &authorizationv1.SelfSubjectAccessReview{
			Spec: authorizationv1.SelfSubjectAccessReviewSpec{
				ResourceAttributes: r.DeepCopy(),
			},
		}
		out, err := scopedCS.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred(), "SelfSubjectAccessReview failed for %v", r)
		Expect(out.Status.Allowed).To(BeFalse(), "tenant SA must not be allowed to %s %s/%s in %s",
			r.Verb, r.Group, r.Resource, r.Namespace)
	}
}

// DeleteHostclaimLoopbackIdentity tears down the tenant namespace (which
// cascades the SA, RBAC and token Secret) and the cluster-scoped
// ClusterRole / ClusterRoleBinding this test created. Idempotent.
func DeleteHostclaimLoopbackIdentity(
	ctx context.Context,
	clusterProxy framework.ClusterProxy,
	tenantNamespace, clusterRoleName string,
) {
	cs := clusterProxy.GetClientSet()
	// Deleting the tenant namespace cascades everything inside (SA, Role,
	// RoleBinding, token Secret, HostClaims, copied Secrets).
	if err := cs.CoreV1().Namespaces().Delete(ctx, tenantNamespace, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		Logf("failed to delete tenant namespace %s: %v", tenantNamespace, err)
	}
	// ClusterRoleBinding first so a still-existing binding does not reference
	// an already-gone role.
	if err := cs.RbacV1().ClusterRoleBindings().Delete(ctx, clusterRoleName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		Logf("failed to delete ClusterRoleBinding %s: %v", clusterRoleName, err)
	}
	if err := cs.RbacV1().ClusterRoles().Delete(ctx, clusterRoleName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		Logf("failed to delete ClusterRole %s: %v", clusterRoleName, err)
	}
}
