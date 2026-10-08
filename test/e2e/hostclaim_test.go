package e2e

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	clusterNamespace = "cluster"
)

var _ = Describe("When testing cluster creation with hostclaims", Label("hostclaim", "hostclaim-basic"), func() {
	BeforeEach(func() {
		validateGlobals(specName)

		// We need to override clusterctl apply log folder to avoid getting our credentials exposed.
		clusterctlLogFolder = filepath.Join(os.TempDir(), "target_cluster_logs", bootstrapClusterProxy.GetName())
	})

	It("Should create a workload cluster", func() {
		By("Apply BMH for workload cluster")
		ApplyBmh(ctx, e2eConfig, bootstrapClusterProxy, namespace, specName)
		By("Create a suitable host deploy policy")
		Expect(BuildAndApplyKustomization(ctx, &BuildAndApplyKustomizationInput{
			Kustomization: "data/hostclaims/host-deploy-policy",
			ClusterProxy:  bootstrapClusterProxy,
		})).To(Succeed())
		By("Fetching cluster configuration")
		k8sVersion := e2eConfig.MustGetVariable("KUBERNETES_VERSION")
		By("Create Ironic namespace")
		clusterClientSet := bootstrapClusterProxy.GetClientSet()
		ironicNamespaceObj := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: clusterNamespace},
		}
		_, err := clusterClientSet.CoreV1().Namespaces().Create(ctx, ironicNamespaceObj, metav1.CreateOptions{})
		if err != nil {
			if apierrors.IsAlreadyExists(err) {
				Logf("Cluster namespace %q already exists, continuing", clusterNamespace)
			} else {
				Expect(err).ToNot(HaveOccurred(), "Unable to create the namespace of the cluster")
			}
		}
		By("Provision Workload cluster")
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
				OSType:                osType + "-hostclaims",
				Namespace:             clusterNamespace,
			}
		})
		By("HOSTCLAIM BASE TESTS PASSED!")
	})

	AfterEach(func() {
		ListBareMetalHosts(ctx, bootstrapClusterProxy.GetClient(), client.InNamespace(namespace))
		ListHostClaims(ctx, bootstrapClusterProxy.GetClient(), client.InNamespace(clusterNamespace))
		ListMetal3Machines(ctx, bootstrapClusterProxy.GetClient(), client.InNamespace(clusterNamespace))
		ListMachines(ctx, bootstrapClusterProxy.GetClient(), client.InNamespace(clusterNamespace))
		ListNodes(ctx, targetCluster.GetClient())
		DumpSpecResourcesAndCleanup(ctx, specName, bootstrapClusterProxy, targetCluster, artifactFolder, clusterNamespace, e2eConfig.GetIntervals, clusterName, clusterctlLogFolder, skipCleanup, clusterctlConfigPath)
	})
})
