package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/test/framework/clusterctl"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Kubernetes version upgrade in target nodes through hostclaims", Label("hostclaim", "hostclaim-k8s-upgrade"), func() {

	BeforeEach(func() {
		osType = strings.ToLower(os.Getenv("OS"))
		Expect(osType).ToNot(Equal(""))
		validateGlobals(specName)

		// We need to override clusterctl apply log folder to avoid getting our credentials exposed.
		clusterctlLogFolder = filepath.Join(os.TempDir(), "target_cluster_logs", bootstrapClusterProxy.GetName())
	})

	It("Should create a hostclaim-backed cluster and run k8s_upgrade tests", func() {
		By("Apply BMH for workload cluster")
		ApplyBmh(ctx, e2eConfig, bootstrapClusterProxy, namespace, specName)

		By("Create a suitable host deploy policy")
		Expect(BuildAndApplyKustomization(ctx, &BuildAndApplyKustomizationInput{
			Kustomization: "data/hostclaims/host-deploy-policy",
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

		// scripts/environment.sh must export CONTROL_PLANE_MACHINE_COUNT >= 3
		// for this label — the KCP webhook refuses to upgrade a single-node
		// control plane, so the MaxSurge=0 patch further down would be rejected.
		Expect(numberOfControlplane).To(BeNumerically(">=", 3), "hostclaim k8s-upgrade needs >= 3 CP replicas; check the case for this label in scripts/environment.sh")

		By("Creating target cluster at the starting Kubernetes version")
		targetCluster, _ = CreateTargetCluster(ctx, func() CreateTargetClusterInput {
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

		By("Running Kubernetes Upgrade tests over hostclaims")
		upgradeKubernetesHostclaims(ctx, func() upgradeKubernetesHostclaimsInput {
			return upgradeKubernetesHostclaimsInput{
				E2EConfig:             e2eConfig,
				BootstrapClusterProxy: bootstrapClusterProxy,
				TargetCluster:         targetCluster,
				SpecName:              specName,
				ClusterName:           clusterName,
				Namespace:             clusterNamespace,
				BMHNamespace:          namespace,
			}
		})
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

type upgradeKubernetesHostclaimsInput struct {
	E2EConfig             *clusterctl.E2EConfig
	BootstrapClusterProxy framework.ClusterProxy
	TargetCluster         framework.ClusterProxy
	SpecName              string
	ClusterName           string
	// Namespace holds the CAPI/CAPM3 resources of the workload cluster
	// (Cluster, KCP, MachineDeployment, Metal3MachineTemplate, HostClaim, ...).
	Namespace string
	// BMHNamespace holds the BareMetalHosts that back the HostClaims.
	BMHNamespace string
}

// upgradeKubernetesHostclaims runs the same rolling Kubernetes upgrade as upgradeKubernetes,
// but on a cluster whose Metal3Machines are bound to BareMetalHosts indirectly through
// HostClaims. Cluster resources live in `Namespace`, while the BareMetalHosts they claim
// live in `BMHNamespace`.
func upgradeKubernetesHostclaims(ctx context.Context, inputGetter func() upgradeKubernetesHostclaimsInput) {
	Logf("Starting Kubernetes upgrade tests (hostclaims)")
	input := inputGetter()
	clusterClient := input.BootstrapClusterProxy.GetClient()
	targetClusterClient := input.TargetCluster.GetClient()
	clientSet := input.TargetCluster.GetClientSet()
	kubernetesVersion := input.E2EConfig.MustGetVariable("KUBERNETES_VERSION_UPGRADE_FROM")
	upgradedK8sVersion := input.E2EConfig.MustGetVariable("KUBERNETES_VERSION")

	var (
		controlplaneTaints = []corev1.Taint{{Key: "node-role.kubernetes.io/control-plane", Effect: corev1.TaintEffectNoSchedule},
			{Key: "node-role.kubernetes.io/master", Effect: corev1.TaintEffectNoSchedule}}
	)

	Logf("KUBERNETES VERSION: %v", kubernetesVersion)
	Logf("UPGRADED K8S VERSION: %v", upgradedK8sVersion)
	Logf("NUMBER OF CONTROLPLANE BMH: %v", numberOfControlplane)
	Logf("NUMBER OF WORKER BMH: %v", numberOfWorkers)
	Logf("CLUSTER NAMESPACE: %s", input.Namespace)
	Logf("BMH NAMESPACE: %s", input.BMHNamespace)

	ListBareMetalHosts(ctx, clusterClient, client.InNamespace(input.BMHNamespace))
	ListMetal3Machines(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListMachines(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListNodes(ctx, targetClusterClient)

	// Download node image
	By("Download image")
	imageURL, imageChecksum := EnsureImage(input.E2EConfig, upgradedK8sVersion)

	By("Create new KCP Metal3MachineTemplate with upgraded image to boot")
	m3MachineTemplateName := input.ClusterName + "-controlplane"
	newM3MachineTemplateName := input.ClusterName + "-new-controlplane"
	CreateNewM3MachineTemplate(ctx, input.Namespace, newM3MachineTemplateName, m3MachineTemplateName, clusterClient, imageURL, imageChecksum)

	Byf("Update KCP to upgrade k8s version and binaries from %s to %s", kubernetesVersion, upgradedK8sVersion)
	kcpObj := framework.GetKubeadmControlPlaneByCluster(ctx, framework.GetKubeadmControlPlaneByClusterInput{
		Lister:      clusterClient,
		ClusterName: input.ClusterName,
		Namespace:   input.Namespace,
	})
	helper, err := patch.NewHelper(kcpObj, clusterClient)
	Expect(err).NotTo(HaveOccurred())
	kcpObj.Spec.MachineTemplate.Spec.InfrastructureRef.Name = newM3MachineTemplateName
	kcpObj.Spec.Version = upgradedK8sVersion
	kcpObj.Spec.Rollout.Strategy.RollingUpdate.MaxSurge.IntVal = 0
	Expect(helper.Patch(ctx, kcpObj)).To(Succeed())

	Byf("Wait until %d BMH(s) in deprovisioning state", 1)
	WaitForNumBmhInState(ctx, bmov1alpha1.StateDeprovisioning, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.BMHNamespace)},
		Replicas:  1,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-deprovisioning"),
	})

	Byf("Wait until %d Control Plane machines become running and updated with the new %s k8s version", numberOfControlplane, upgradedK8sVersion)
	runningAndUpgradedKCPMachines := func(machine clusterv1.Machine) bool {
		running := machine.Status.GetTypedPhase() == clusterv1.MachinePhaseRunning
		upgraded := machine.Spec.Version == upgradedK8sVersion
		_, isControlPlane := machine.GetLabels()[clusterv1.MachineControlPlaneLabel]
		return running && upgraded && isControlPlane
	}
	WaitForNumMachines(ctx, runningAndUpgradedKCPMachines, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.Namespace)},
		Replicas:  numberOfControlplane,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-machine-running"),
	})

	By("Untaint CP nodes")
	controlplaneNodes := getControlplaneNodes(ctx, clientSet)
	untaintNodes(ctx, targetClusterClient, controlplaneNodes, controlplaneTaints)

	By("Put maxSurge field in KubeadmControlPlane back to default value(1)")
	kcpObj = framework.GetKubeadmControlPlaneByCluster(ctx, framework.GetKubeadmControlPlaneByClusterInput{
		Lister:      clusterClient,
		ClusterName: input.ClusterName,
		Namespace:   input.Namespace,
	})
	helper, err = patch.NewHelper(kcpObj, clusterClient)
	Expect(err).NotTo(HaveOccurred())
	kcpObj.Spec.Rollout.Strategy.RollingUpdate.MaxSurge.IntVal = 1
	for range 3 {
		err = helper.Patch(ctx, kcpObj)
		if err == nil {
			break
		}
		time.Sleep(30 * time.Second)
	}

	By("Get MachineDeployment")
	machineDeployments := framework.GetMachineDeploymentsByCluster(ctx, framework.GetMachineDeploymentsByClusterInput{
		Lister:      clusterClient,
		ClusterName: input.ClusterName,
		Namespace:   input.Namespace,
	})
	Expect(machineDeployments).To(HaveLen(1), "Expected exactly 1 MachineDeployment")
	machineDeploy := machineDeployments[0]

	By("Create new Metal3MachineTemplate for MD with upgraded image to boot")
	m3MachineTemplateName = input.ClusterName + "-workers"
	newM3MachineTemplateName = input.ClusterName + "-new-workers"
	CreateNewM3MachineTemplate(ctx, input.Namespace, newM3MachineTemplateName, m3MachineTemplateName, clusterClient, imageURL, imageChecksum)

	Byf("Update MD to upgrade k8s version and binaries from %s to %s", kubernetesVersion, upgradedK8sVersion)
	helper, err = patch.NewHelper(machineDeploy, clusterClient)
	Expect(err).NotTo(HaveOccurred())
	machineDeploy.Spec.Rollout.Strategy.RollingUpdate.MaxSurge.IntVal = 0
	machineDeploy.Spec.Rollout.Strategy.RollingUpdate.MaxUnavailable.IntVal = 1
	machineDeploy.Spec.Template.Spec.InfrastructureRef.Name = newM3MachineTemplateName
	machineDeploy.Spec.Template.Spec.Version = upgradedK8sVersion
	Expect(helper.Patch(ctx, machineDeploy)).To(Succeed())

	Byf("Wait until %d BMH(s) in deprovisioning state", 1)
	WaitForNumBmhInState(ctx, bmov1alpha1.StateDeprovisioning, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.BMHNamespace)},
		Replicas:  1,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-deprovisioning"),
	})

	Byf("Wait until %d BMH(s) in provisioning state", 1)
	WaitForNumBmhInState(ctx, bmov1alpha1.StateProvisioning, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.BMHNamespace)},
		Replicas:  1,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-deprovisioning"),
	})

	Byf("Wait until the worker BMH becomes provisioned")
	WaitForNumBmhInState(ctx, bmov1alpha1.StateProvisioned, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.BMHNamespace)},
		Replicas:  numberOfAllBmh,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-provisioned"),
	})

	Byf("Wait until worker machine becomes running and updated with new %s k8s version", upgradedK8sVersion)
	runningAndUpgradedWorkerMachines := func(machine clusterv1.Machine) bool {
		running := machine.Status.GetTypedPhase() == clusterv1.MachinePhaseRunning
		upgraded := machine.Spec.Version == upgradedK8sVersion
		_, isControlPlane := machine.GetLabels()[clusterv1.MachineControlPlaneLabel]
		return running && upgraded && !isControlPlane
	}
	WaitForNumMachines(ctx, runningAndUpgradedWorkerMachines, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.Namespace)},
		Replicas:  numberOfWorkers,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-machine-running"),
	})

	// Verify that all nodes are using the new k8s version.
	Byf("Verify all machines become running and updated with new %s k8s version", upgradedK8sVersion)
	runningAndUpgraded := func(machine clusterv1.Machine) bool {
		running := machine.Status.GetTypedPhase() == clusterv1.MachinePhaseRunning
		upgraded := machine.Spec.Version == upgradedK8sVersion
		return running && upgraded
	}
	WaitForNumMachines(ctx, runningAndUpgraded, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.Namespace)},
		Replicas:  numberOfAllBmh,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-machine-running"),
	})

	By("HOSTCLAIM K8S UPGRADE TESTS PASSED!")
}
