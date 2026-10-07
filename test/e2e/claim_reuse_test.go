package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Node-reuse on a HostClaim-backed cluster.
//
// This is the HostClaim analog of the "node-reuse" portion of
// pivoting_based_feature_test.go.
var _ = Describe("Node reuse on a hostclaim-backed cluster",
	Label("hostclaim", "claim-reuse"), func() {

		var ctxt = context.TODO()

		BeforeEach(func() {
			osType = strings.ToLower(os.Getenv("OS"))
			Expect(osType).ToNot(Equal(""))
			validateGlobals(specName)

			clusterctlLogFolder = filepath.Join(os.TempDir(), "target_cluster_logs", bootstrapClusterProxy.GetName())
		})

		It("reuses the same HostClaims through a KCP and MachineDeployment rolling upgrade", func() {
			By("Apply BMH for workload cluster")
			ApplyBmh(ctxt, e2eConfig, bootstrapClusterProxy, namespace, specName)

			By("Create a suitable host deploy policy")
			Expect(BuildAndApplyKustomization(ctxt, &BuildAndApplyKustomizationInput{
				Kustomization: "data/hostclaims/host-deploy-policy",
				ClusterProxy:  bootstrapClusterProxy,
			})).To(Succeed())

			By("Create cluster namespace")
			clusterClientSet := bootstrapClusterProxy.GetClientSet()
			clusterNamespaceObj := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: clusterNamespace},
			}
			_, err := clusterClientSet.CoreV1().Namespaces().Create(ctxt, clusterNamespaceObj, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				Expect(err).ToNot(HaveOccurred())
			}

			// KCP rolling upgrade with MaxSurge=0 refuses to proceed on a
			// single-node control plane. Same precondition as the hostclaim
			// k8s-upgrade label.
			Expect(numberOfControlplane).To(BeNumerically(">=", 3))

			By("Creating target cluster at the starting Kubernetes version")
			targetCluster, _ = CreateTargetCluster(ctxt, func() CreateTargetClusterInput {
				return CreateTargetClusterInput{
					E2EConfig:             e2eConfig,
					BootstrapClusterProxy: bootstrapClusterProxy,
					SpecName:              specName,
					ClusterName:           clusterName,
					K8sVersion:            e2eConfig.MustGetVariable("KUBERNETES_VERSION_UPGRADE_FROM"),
					KCPMachineCount:       int64(numberOfControlplane),
					WorkerMachineCount:    int64(numberOfWorkers),
					ClusterctlLogFolder:   clusterctlLogFolder,
					ClusterctlConfigPath:  clusterctlConfigPath,
					OSType:                osType + "-hostclaims",
					Namespace:             clusterNamespace,
				}
			})

			By("Running HostClaim node-reuse tests")
			ClaimReuse(ctxt, func() ClaimReuseInput {
				return ClaimReuseInput{
					E2EConfig:      e2eConfig,
					ClusterProxy:   bootstrapClusterProxy,
					TargetCluster:  targetCluster,
					SpecName:       specName,
					ClusterName:    clusterName,
					Namespace:      clusterNamespace,
					BMHNamespace:   namespace,
					ArtifactFolder: artifactFolder,
				}
			})
		})

		AfterEach(func() {
			ListBareMetalHosts(ctxt, bootstrapClusterProxy.GetClient(), client.InNamespace(namespace))
			ListHostClaims(ctxt, bootstrapClusterProxy.GetClient(), client.InNamespace(clusterNamespace))
			ListMetal3Machines(ctxt, bootstrapClusterProxy.GetClient(), client.InNamespace(clusterNamespace))
			ListMachines(ctxt, bootstrapClusterProxy.GetClient(), client.InNamespace(clusterNamespace))
			if targetCluster != nil {
				ListNodes(ctxt, targetCluster.GetClient())
			}
			DumpSpecResourcesAndCleanup(ctxt, specName, bootstrapClusterProxy, targetCluster, artifactFolder, clusterNamespace, e2eConfig.GetIntervals, clusterName, clusterctlLogFolder, skipCleanup, clusterctlConfigPath)
		})
	})
