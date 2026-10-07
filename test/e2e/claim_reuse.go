package e2e

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/test/framework/clusterctl"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClaimReuseInput carries the handles and names ClaimReuse needs.
//
// Unlike node_reuse.go there is no pivoting here: CAPM3 keeps running on the
// bootstrap cluster for the whole test. ClusterProxy therefore points at the
// bootstrap (where CAPI/CAPM3 objects + HostClaims + backing BareMetalHosts
// live) and TargetCluster points at the workload cluster (used only for node-
// level untainting and for waiting on kube-system pods).
type ClaimReuseInput struct {
	E2EConfig      *clusterctl.E2EConfig
	ClusterProxy   framework.ClusterProxy
	TargetCluster  framework.ClusterProxy
	SpecName       string
	ClusterName    string
	Namespace      string // cluster namespace: CAPI + CAPM3 + HostClaims
	BMHNamespace   string // BMH namespace: BareMetalHosts + HostDeployPolicy
	ArtifactFolder string
}

// ClaimReuse verifies the nodeReuse feature on a HostClaim-backed cluster.
//
// The actions are similar to node_reuse.go but the assertions are different as
// they are mostly done on hostclaims.
func ClaimReuse(ctx context.Context, inputGetter func() ClaimReuseInput) {
	Logf("Starting hostclaim node reuse tests [claim_reuse]")
	input := inputGetter()
	clusterClient := input.ClusterProxy.GetClient()
	targetClientSet := input.TargetCluster.GetClientSet()
	targetClient := input.TargetCluster.GetClient()

	toK8sVersion := input.E2EConfig.MustGetVariable("KUBERNETES_VERSION_PATCH_TO")
	fromK8sVersion := input.E2EConfig.MustGetVariable("KUBERNETES_VERSION_UPGRADE_FROM")
	numberOfWorkers := int(*input.E2EConfig.MustGetInt32PtrVariable("WORKER_MACHINE_COUNT"))
	numberOfControlplane := int(*input.E2EConfig.MustGetInt32PtrVariable("CONTROL_PLANE_MACHINE_COUNT"))
	numberOfAllBmh := numberOfWorkers + numberOfControlplane

	var (
		controlplaneTaints = []corev1.Taint{
			{Key: "node-role.kubernetes.io/control-plane", Effect: corev1.TaintEffectNoSchedule},
			{Key: "node-role.kubernetes.io/master", Effect: corev1.TaintEffectNoSchedule},
		}
		emptyTaint = []corev1.Taint{}
	)

	Logf("KUBERNETES VERSION: %v", fromK8sVersion)
	Logf("UPGRADED K8S VERSION: %v", toK8sVersion)
	Logf("NUMBER OF CONTROLPLANE BMH: %v", numberOfControlplane)
	Logf("NUMBER OF WORKER BMH: %v", numberOfWorkers)
	Logf("CLUSTER NAMESPACE: %s", input.Namespace)
	Logf("BMH NAMESPACE: %s", input.BMHNamespace)

	ListBareMetalHosts(ctx, clusterClient, client.InNamespace(input.BMHNamespace))
	ListHostClaims(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListMetal3Machines(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListMachines(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListNodes(ctx, targetClient)

	By("Untaint all CP nodes before scaling down MachineDeployment [claim_reuse]")
	for controlplaneNodesPresent := 0; controlplaneNodesPresent < numberOfControlplane; {
		for untaintedNodeCount := 1; untaintedNodeCount > 0; {
			controlplaneNodes := getControlplaneNodes(ctx, targetClientSet)
			controlplaneNodesPresent = len(controlplaneNodes.Items)
			untaintedNodeCount = untaintNodes(ctx, targetClient, controlplaneNodes, controlplaneTaints)
			time.Sleep(10 * time.Second)
		}
	}

	By("Scale down MachineDeployment to 0 [claim_reuse]")
	// Frees exactly one BMH that the KCP rolling upgrade will reuse.
	ScaleMachineDeployment(ctx, clusterClient, input.ClusterName, input.Namespace, 0)
	numberOfWorkers = 0

	Byf("Wait until the worker is scaled down and %d BMH(s) are Available [claim_reuse]", numberOfAllBmh-numberOfControlplane-numberOfWorkers)
	WaitForNumBmhInState(ctx, bmov1alpha1.StateAvailable, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.BMHNamespace)},
		Replicas:  numberOfAllBmh - numberOfControlplane - numberOfWorkers,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-available"),
	})

	By("Wait for all pods to be running after workers were scaled down to 0 [claim_reuse]")
	framework.WaitForPodListCondition(ctx, framework.WaitForPodListConditionInput{
		Lister:      targetClient,
		ListOptions: &client.ListOptions{LabelSelector: labels.Everything(), Namespace: "kube-system"},
		Condition:   framework.PhasePodCondition(corev1.PodRunning),
	}, input.E2EConfig.GetIntervals(input.SpecName, "wait-all-pod-to-be-running-on-target-cluster")...)

	By("Record the HostClaim names and backing BMHs before the KCP upgrade [claim_reuse]")
	kcpClaimsBeforeUpgrade := getProvisionedHostClaimNames(ctx, input.Namespace, clusterClient)
	kcpBmhsBeforeUpgrade := getBackingBmhNamesForClaims(ctx, input.Namespace, clusterClient)

	By("Download image [claim_reuse]")
	imageURL, imageChecksum := EnsureImage(input.E2EConfig, toK8sVersion)

	By("Set nodeReuse field to 'True' and create new KCP Metal3MachineTemplate with upgraded image [claim_reuse]")
	m3MachineTemplateName := input.ClusterName + "-controlplane"
	updateNodeReuse(ctx, input.Namespace, true, m3MachineTemplateName, clusterClient)
	newM3MachineTemplateName := input.ClusterName + "-new-controlplane"
	CreateNewM3MachineTemplate(ctx, input.Namespace, newM3MachineTemplateName, m3MachineTemplateName, clusterClient, imageURL, imageChecksum)

	Byf("Update KCP to upgrade k8s version and binaries from %s to %s [claim_reuse]", fromK8sVersion, toK8sVersion)
	kcpObj := framework.GetKubeadmControlPlaneByCluster(ctx, framework.GetKubeadmControlPlaneByClusterInput{
		Lister:      clusterClient,
		ClusterName: input.ClusterName,
		Namespace:   input.Namespace,
	})
	helper, err := patch.NewHelper(kcpObj, clusterClient)
	Expect(err).NotTo(HaveOccurred())
	kcpObj.Spec.MachineTemplate.Spec.InfrastructureRef.Name = newM3MachineTemplateName
	kcpObj.Spec.Version = toK8sVersion
	kcpObj.Spec.Rollout.Strategy.RollingUpdate.MaxSurge.IntVal = 0
	// Prevent new control-plane nodes from being tainted so we can finish the
	// test without needing a dedicated untaint step at the very end.
	kcpObj.Spec.KubeadmConfigSpec.JoinConfiguration.NodeRegistration.Taints = ptr.To(emptyTaint)
	kcpObj.Spec.KubeadmConfigSpec.InitConfiguration.NodeRegistration.Taints = ptr.To(emptyTaint)

	By("Fetch cluster logs and manifests before patching KCP [claim_reuse]")
	if err2 := FetchClusterLogs(input.ClusterProxy, filepath.Join(input.ArtifactFolder, "claim_reuse", "preUpgrade-kcp", "logs")); err2 != nil {
		Logf("Error: %v", err2)
	}
	if err2 := FetchManifests(input.ClusterProxy, filepath.Join(input.ArtifactFolder, "claim_reuse", "preUpgrade-kcp", "manifests")); err2 != nil {
		Logf("Error fetching manifests: %v", err2)
	}

	Expect(helper.Patch(ctx, kcpObj)).To(Succeed(), "patching KCP for upgrade")

	By("Check that only a single machine is Deleting and no new machines are Provisioning [claim_reuse]")
	WaitForNumMachinesInState(ctx, clusterv1.MachinePhaseDeleting, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.Namespace)},
		Replicas:  1,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-machine-deleting"),
	})
	machineList := &clusterv1.MachineList{}
	Expect(clusterClient.List(ctx, machineList, client.InNamespace(input.Namespace))).To(Succeed())
	Expect(FilterMachinesByPhase(machineList.Items, clusterv1.MachinePhaseProvisioning)).To(BeEmpty())

	Byf("Wait until 1 BMH is in deprovisioning state [claim_reuse]")
	WaitForNumBmhInState(ctx, bmov1alpha1.StateDeprovisioning, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.BMHNamespace)},
		Replicas:  1,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-deprovisioning"),
	})

	Logf("Find the deprovisioning BMH [claim_reuse]")
	bmhList := bmov1alpha1.BareMetalHostList{}
	Expect(clusterClient.List(ctx, &bmhList, client.InNamespace(input.BMHNamespace))).To(Succeed())
	deprovisioningBmhs := FilterBmhsByProvisioningState(bmhList.Items, bmov1alpha1.StateDeprovisioning)
	Expect(deprovisioningBmhs).To(HaveLen(1), "exactly one BMH must be deprovisioning while the KCP upgrade rolls")
	kcpBmhKey := types.NamespacedName{Name: deprovisioningBmhs[0].Name, Namespace: input.BMHNamespace}

	By("Assert the kept HostClaim is retargeted to the KCP as part of node reuse [claim_reuse]")
	// This is the main HostClaim-level invariant from
	// baremetal/metal3machine_host_manager.go Delete: on deprovision the
	// HostClaim's Spec.ConsumerRef must switch to the KubeadmControlPlane and
	// its provisioning fields must be cleared. The HostClaim is identified by
	// its backing BMH, which we just captured in kcpBmhKey.
	retargetedClaimName := assertKeptHostClaimRetargeted(ctx, clusterClient, input.Namespace, kcpBmhKey.Name, "KubeadmControlPlane", input.ClusterName,
		input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-deprovisioning"))

	By("Wait until the above deprovisioning BMH is Available again [claim_reuse]")
	Eventually(func(g Gomega) {
		bmh := bmov1alpha1.BareMetalHost{}
		g.Expect(clusterClient.Get(ctx, kcpBmhKey, &bmh)).To(Succeed())
		g.Expect(bmh.Status.Provisioning.State).To(Equal(bmov1alpha1.StateAvailable))
	}, input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-deprovisioning-available")...).Should(Succeed())

	By("Fetch cluster logs and manifests after patching KCP [claim_reuse]")
	if err2 := FetchClusterLogs(input.ClusterProxy, filepath.Join(input.ArtifactFolder, "claim_reuse", "postUpgrade-kcp", "logs")); err2 != nil {
		Logf("Error: %v", err2)
	}
	if err2 := FetchManifests(input.ClusterProxy, filepath.Join(input.ArtifactFolder, "claim_reuse", "postUpgrade-kcp", "manifests")); err2 != nil {
		Logf("Error fetching manifests: %v", err2)
	}

	By("Check the just-deprovisioned BMH is reused for the next provisioning [claim_reuse]")
	Eventually(func(g Gomega) {
		bmh := bmov1alpha1.BareMetalHost{}
		g.Expect(clusterClient.Get(ctx, kcpBmhKey, &bmh)).To(Succeed())
		g.Expect(bmh.Status.Provisioning.State).To(Equal(bmov1alpha1.StateProvisioning))
	}, input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-available-provisioning")...).Should(Succeed())

	By("Check the kept HostClaim is rebound to the new Metal3Machine [claim_reuse]")
	// provideHostClaim's reuse path should rebind the same HostClaim to the
	// replacement Metal3Machine and drop the nodeReuseCleared annotation.
	assertHostClaimRebound(ctx, clusterClient, input.Namespace, retargetedClaimName,
		input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-available-provisioning"))

	Byf("Wait until all %v KCP machines become running and updated with new %s k8s version [claim_reuse]", numberOfControlplane, toK8sVersion)
	runningAndUpgradedKCPMachines := func(machine clusterv1.Machine) bool {
		running := machine.Status.GetTypedPhase() == clusterv1.MachinePhaseRunning
		upgraded := machine.Spec.Version == toK8sVersion
		_, isControlPlane := machine.GetLabels()[clusterv1.MachineControlPlaneLabel]
		return running && upgraded && isControlPlane
	}
	WaitForNumMachines(ctx, runningAndUpgradedKCPMachines, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.Namespace)},
		Replicas:  numberOfControlplane,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-machine-running"),
	})

	ListBareMetalHosts(ctx, clusterClient, client.InNamespace(input.BMHNamespace))
	ListHostClaims(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListMetal3Machines(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListMachines(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListNodes(ctx, targetClient)

	verifyMetal3MachineHostClaimAssociation(ctx, clusterClient, input.Namespace, retargetedClaimName,
		input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-available-provisioning"))

	By("Wait for all pods to be running after the CP upgrade [claim_reuse]")
	framework.WaitForPodListCondition(ctx, framework.WaitForPodListConditionInput{
		Lister:      targetClient,
		ListOptions: &client.ListOptions{LabelSelector: labels.Everything(), Namespace: "kube-system"},
		Condition:   framework.PhasePodCondition(corev1.PodRunning),
	}, input.E2EConfig.GetIntervals(input.SpecName, "wait-all-pod-to-be-running-on-target-cluster")...)
	Logf("The CP upgrade process has ended [claim_reuse]")

	By("Compare HostClaim and backing-BMH sets before and after the KCP upgrade [claim_reuse]")
	kcpClaimsAfterUpgrade := getProvisionedHostClaimNames(ctx, input.Namespace, clusterClient)
	Expect(reflect.DeepEqual(kcpClaimsBeforeUpgrade, kcpClaimsAfterUpgrade)).To(BeTrue(),
		"The same HostClaims were not reused across the KCP upgrade: before=%v, after=%v", kcpClaimsBeforeUpgrade, kcpClaimsAfterUpgrade)
	kcpBmhsAfterUpgrade := getBackingBmhNamesForClaims(ctx, input.Namespace, clusterClient)
	Expect(reflect.DeepEqual(kcpBmhsBeforeUpgrade, kcpBmhsAfterUpgrade)).To(BeTrue(),
		"The same backing BareMetalHosts were not reused across the KCP upgrade: before=%v, after=%v", kcpBmhsBeforeUpgrade, kcpBmhsAfterUpgrade)

	By("Put maxSurge field in KubeadmControlPlane back to the default value (1) [claim_reuse]")
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

	By("Scale the controlplane down to 1 [claim_reuse]")
	ScaleKubeadmControlPlane(ctx, clusterClient, client.ObjectKey{Namespace: input.Namespace, Name: input.ClusterName}, 1)
	numberOfControlplane = 1
	Byf("Wait until controlplane is scaled down and %d BMHs are Available [claim_reuse]", numberOfAllBmh-numberOfControlplane-numberOfWorkers)
	WaitForNumBmhInState(ctx, bmov1alpha1.StateAvailable, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.BMHNamespace)},
		Replicas:  numberOfAllBmh - numberOfControlplane - numberOfWorkers,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-cp-available"),
	})

	By("Wait for 1 CP node to be present after CP was scaled to 1 [claim_reuse]")
	for controlplaneNodesPresent := 0; controlplaneNodesPresent != numberOfControlplane; {
		controlplaneNodes := getControlplaneNodes(ctx, targetClientSet)
		controlplaneNodesPresent = len(controlplaneNodes.Items)
		time.Sleep(5 * time.Second)
	}

	By("Wait for all pods to be running after CP was scaled to 1 [claim_reuse]")
	framework.WaitForPodListCondition(ctx, framework.WaitForPodListConditionInput{
		Lister:      targetClient,
		ListOptions: &client.ListOptions{LabelSelector: labels.Everything(), Namespace: "kube-system"},
		Condition:   framework.PhasePodCondition(corev1.PodRunning),
	}, input.E2EConfig.GetIntervals(input.SpecName, "wait-all-pod-to-be-running-on-target-cluster")...)

	By("Scrub stale KubeadmControlPlane ConsumerRefs on freed HostClaims [claim_reuse]")
	// The CP scale-down left HostClaims pointing at the KCP (nodeReuse mode).
	// Those HostClaims must be made available again so the MD can pick one up.
	scrubStaleHostClaimConsumerRefs(ctx, clusterClient, input.Namespace, "KubeadmControlPlane")

	By("Get MachineDeployment [claim_reuse]")
	machineDeployments := framework.GetMachineDeploymentsByCluster(ctx, framework.GetMachineDeploymentsByClusterInput{
		Lister:      clusterClient,
		ClusterName: input.ClusterName,
		Namespace:   input.Namespace,
	})
	Expect(machineDeployments).To(HaveLen(1), "Expected exactly 1 MachineDeployment")
	machineDeploy := machineDeployments[0]

	By("Set MD Metal3MachineTemplate name [claim_reuse]")
	m3MachineTemplateName = input.ClusterName + "-workers"

	By("Scale the worker up to 1 to start testing MachineDeployment [claim_reuse]")
	ScaleMachineDeployment(ctx, clusterClient, input.ClusterName, input.Namespace, 1)
	numberOfWorkers = 1

	Byf("Wait until the worker BMH becomes provisioned [claim_reuse]")
	WaitForNumBmhInState(ctx, bmov1alpha1.StateProvisioned, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.BMHNamespace)},
		Replicas:  numberOfControlplane + numberOfWorkers,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-provisioned"),
	})

	Byf("Wait until the worker machine becomes running with %s k8s version [claim_reuse]", fromK8sVersion)
	runningWorkerMachines := func(machine clusterv1.Machine) bool {
		running := machine.Status.GetTypedPhase() == clusterv1.MachinePhaseRunning
		upgraded := machine.Spec.Version == fromK8sVersion
		_, isControlPlane := machine.GetLabels()[clusterv1.MachineControlPlaneLabel]
		return running && upgraded && !isControlPlane
	}
	WaitForNumMachines(ctx, runningWorkerMachines, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.Namespace)},
		Replicas:  numberOfWorkers,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-machine-running"),
	})

	By("Wait for all pods to be running after the worker was scaled to 1 [claim_reuse]")
	framework.WaitForPodListCondition(ctx, framework.WaitForPodListConditionInput{
		Lister:      targetClient,
		ListOptions: &client.ListOptions{LabelSelector: labels.Everything(), Namespace: "kube-system"},
		Condition:   framework.PhasePodCondition(corev1.PodRunning),
	}, input.E2EConfig.GetIntervals(input.SpecName, "wait-all-pod-to-be-running-on-target-cluster")...)

	By("Record HostClaim and backing-BMH names before the MachineDeployment upgrade [claim_reuse]")
	mdClaimsBeforeUpgrade := getProvisionedHostClaimNames(ctx, input.Namespace, clusterClient)
	mdBmhsBeforeUpgrade := getBackingBmhNamesForClaims(ctx, input.Namespace, clusterClient)

	By("Set nodeReuse field to 'True' and create new Metal3MachineTemplate for MD with upgraded image [claim_reuse]")
	updateNodeReuse(ctx, input.Namespace, true, m3MachineTemplateName, clusterClient)
	newM3MachineTemplateName = input.ClusterName + "-new-workers"
	CreateNewM3MachineTemplate(ctx, input.Namespace, newM3MachineTemplateName, m3MachineTemplateName, clusterClient, imageURL, imageChecksum)

	Byf("Update MD to upgrade k8s version and binaries from %s to %s [claim_reuse]", fromK8sVersion, toK8sVersion)
	helper, err = patch.NewHelper(machineDeploy, clusterClient)
	Expect(err).NotTo(HaveOccurred())
	machineDeploy.Spec.Rollout.Strategy.RollingUpdate.MaxSurge.IntVal = 0
	machineDeploy.Spec.Rollout.Strategy.RollingUpdate.MaxUnavailable.IntVal = 1
	machineDeploy.Spec.Template.Spec.InfrastructureRef.Name = newM3MachineTemplateName
	machineDeploy.Spec.Template.Spec.Version = toK8sVersion

	By("Fetch cluster logs and manifests before patching MD [claim_reuse]")
	if err := FetchClusterLogs(input.ClusterProxy, filepath.Join(input.ArtifactFolder, "claim_reuse", "preUpgrade-worker", "logs")); err != nil {
		Logf("Error: %v", err)
	}
	if err := FetchManifests(input.ClusterProxy, filepath.Join(input.ArtifactFolder, "claim_reuse", "preUpgrade-worker", "manifests")); err != nil {
		Logf("Error fetching manifests: %v", err)
	}

	Expect(helper.Patch(ctx, machineDeploy)).To(Succeed(), "patching MD for upgrade")

	Byf("Wait until 1 BMH is deprovisioning [claim_reuse]")
	WaitForNumBmhInState(ctx, bmov1alpha1.StateDeprovisioning, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.BMHNamespace)},
		Replicas:  1,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-deprovisioning"),
	})

	Logf("Find the deprovisioning BMH [claim_reuse]")
	bmhList = bmov1alpha1.BareMetalHostList{}
	Expect(clusterClient.List(ctx, &bmhList, client.InNamespace(input.BMHNamespace))).To(Succeed())
	deprovisioningBmhs = FilterBmhsByProvisioningState(bmhList.Items, bmov1alpha1.StateDeprovisioning)
	Expect(deprovisioningBmhs).To(HaveLen(1))
	workerBmhKey := types.NamespacedName{Name: deprovisioningBmhs[0].Name, Namespace: input.BMHNamespace}

	By("Assert the kept worker HostClaim is retargeted to the MachineDeployment [claim_reuse]")
	workerClaimName := assertKeptHostClaimRetargeted(ctx, clusterClient, input.Namespace, workerBmhKey.Name, "MachineDeployment", machineDeploy.Name,
		input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-deprovisioning"))

	By("Wait until the above deprovisioning BMH is Available again [claim_reuse]")
	Eventually(func(g Gomega) {
		bmh := bmov1alpha1.BareMetalHost{}
		g.Expect(clusterClient.Get(ctx, workerBmhKey, &bmh)).To(Succeed())
		g.Expect(bmh.Status.Provisioning.State).To(Equal(bmov1alpha1.StateAvailable))
	}, input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-deprovisioning-available")...).Should(Succeed())

	By("Fetch cluster logs and manifests after patching MD [claim_reuse]")
	if err := FetchClusterLogs(input.ClusterProxy, filepath.Join(input.ArtifactFolder, "claim_reuse", "postUpgrade-worker", "logs")); err != nil {
		Logf("Error: %v", err)
	}
	if err := FetchManifests(input.ClusterProxy, filepath.Join(input.ArtifactFolder, "claim_reuse", "postUpgrade-worker", "manifests")); err != nil {
		Logf("Error fetching manifests: %v", err)
	}

	By("Check the just-deprovisioned BMH is reused for the next provisioning [claim_reuse]")
	Eventually(func(g Gomega) {
		bmh := bmov1alpha1.BareMetalHost{}
		g.Expect(clusterClient.Get(ctx, workerBmhKey, &bmh)).To(Succeed())
		g.Expect(bmh.Status.Provisioning.State).To(Equal(bmov1alpha1.StateProvisioning))
	}, input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-available-provisioning")...).Should(Succeed())

	By("Check the kept worker HostClaim is rebound to the new Metal3Machine [claim_reuse]")
	assertHostClaimRebound(ctx, clusterClient, input.Namespace, workerClaimName,
		input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-available-provisioning"))

	Byf("Wait until the worker machine becomes running with new %s k8s version [claim_reuse]", toK8sVersion)
	runningAndUpgradedWorkerMachines := func(machine clusterv1.Machine) bool {
		running := machine.Status.GetTypedPhase() == clusterv1.MachinePhaseRunning
		upgraded := machine.Spec.Version == toK8sVersion
		_, isControlPlane := machine.GetLabels()[clusterv1.MachineControlPlaneLabel]
		return running && upgraded && !isControlPlane
	}
	WaitForNumMachines(ctx, runningAndUpgradedWorkerMachines, WaitForNumInput{
		Client:    clusterClient,
		Options:   []client.ListOption{client.InNamespace(input.Namespace)},
		Replicas:  numberOfWorkers,
		Intervals: input.E2EConfig.GetIntervals(input.SpecName, "wait-machine-running"),
	})

	verifyMetal3MachineHostClaimAssociation(ctx, clusterClient, input.Namespace, workerClaimName,
		input.E2EConfig.GetIntervals(input.SpecName, "wait-bmh-available-provisioning"))

	By("Compare HostClaim and backing-BMH sets before and after the MD upgrade [claim_reuse]")
	mdClaimsAfterUpgrade := getProvisionedHostClaimNames(ctx, input.Namespace, clusterClient)
	Expect(reflect.DeepEqual(mdClaimsBeforeUpgrade, mdClaimsAfterUpgrade)).To(BeTrue(),
		"The same HostClaims were not reused across the MD upgrade: before=%v, after=%v", mdClaimsBeforeUpgrade, mdClaimsAfterUpgrade)
	mdBmhsAfterUpgrade := getBackingBmhNamesForClaims(ctx, input.Namespace, clusterClient)
	Expect(reflect.DeepEqual(mdBmhsBeforeUpgrade, mdBmhsAfterUpgrade)).To(BeTrue(),
		"The same backing BareMetalHosts were not reused across the MD upgrade: before=%v, after=%v", mdBmhsBeforeUpgrade, mdBmhsAfterUpgrade)

	ListBareMetalHosts(ctx, clusterClient, client.InNamespace(input.BMHNamespace))
	ListHostClaims(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListMetal3Machines(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListMachines(ctx, clusterClient, client.InNamespace(input.Namespace))
	ListNodes(ctx, targetClient)

	By("CLAIM REUSE TESTS PASSED!")
}

// getProvisionedHostClaimNames returns the sorted list of namespaced names of
// HostClaims that are currently bound to a BareMetalHost (Status.BareMetalHost
// set). Sorted so that reflect.DeepEqual on before/after snapshots is stable.
func getProvisionedHostClaimNames(ctx context.Context, namespace string, cl client.Client) []string {
	hcs := bmov1alpha1.HostClaimList{}
	Expect(cl.List(ctx, &hcs, client.InNamespace(namespace))).To(Succeed())
	var names []string
	for i := range hcs.Items {
		hc := &hcs.Items[i]
		if hc.Status.BareMetalHost == nil {
			continue
		}
		names = append(names, hc.Namespace+"/"+hc.Name)
	}
	sort.Strings(names)
	return names
}

// getBackingBmhNamesForClaims returns the sorted list of
// Status.BareMetalHost.Name for every HostClaim in the namespace that is bound.
// Used to assert that the same hardware is reused across an upgrade when
// HostClaim names alone are insufficient (they are kept across reuse, so the
// claim set never changes).
func getBackingBmhNamesForClaims(ctx context.Context, namespace string, cl client.Client) []string {
	hcs := bmov1alpha1.HostClaimList{}
	Expect(cl.List(ctx, &hcs, client.InNamespace(namespace))).To(Succeed())
	var names []string
	for i := range hcs.Items {
		hc := &hcs.Items[i]
		if hc.Status.BareMetalHost == nil {
			continue
		}
		names = append(names, hc.Status.BareMetalHost.Namespace+"/"+hc.Status.BareMetalHost.Name)
	}
	sort.Strings(names)
	return names
}

// assertKeptHostClaimRetargeted waits until the HostClaim whose Status.BareMetalHost
// points at bmhName has:
//   - ConsumerRef.Kind == expectedKind
//   - ConsumerRef.Name == expectedName
//   - Spec.Image/UserData/MetaData/NetworkData all nil and PoweredOn == false
//   - the nodeReuseCleared annotation set
//
// This is the proof that baremetal/metal3machine_host_manager.go Delete took
// the node-reuse branch and cleared the spec. Returns the retargeted
// HostClaim's name so later asserts can refer to it.
func assertKeptHostClaimRetargeted(
	ctx context.Context, cl client.Client, namespace, bmhName, expectedKind, expectedName string,
	intervals []interface{},
) string {
	const nodeReuseCleared = "infrastructure.cluster.x-k8s.io/node-reuse-cleared-at"
	var claimName string
	Eventually(func(g Gomega) {
		hcs := bmov1alpha1.HostClaimList{}
		g.Expect(cl.List(ctx, &hcs, client.InNamespace(namespace))).To(Succeed())
		var found *bmov1alpha1.HostClaim
		for i := range hcs.Items {
			hc := &hcs.Items[i]
			if hc.Status.BareMetalHost != nil && hc.Status.BareMetalHost.Name == bmhName {
				found = hc
				break
			}
		}
		g.Expect(found).NotTo(BeNil(), "no HostClaim references backing BMH %s", bmhName)
		g.Expect(found.Spec.ConsumerRef).NotTo(BeNil(), "HostClaim %s has no ConsumerRef", found.Name)
		g.Expect(found.Spec.ConsumerRef.Kind).To(Equal(expectedKind),
			"HostClaim %s ConsumerRef.Kind: want %s, got %s", found.Name, expectedKind, found.Spec.ConsumerRef.Kind)
		g.Expect(found.Spec.ConsumerRef.Name).To(Equal(expectedName),
			"HostClaim %s ConsumerRef.Name: want %s, got %s", found.Name, expectedName, found.Spec.ConsumerRef.Name)
		g.Expect(found.Spec.Image).To(BeNil(), "HostClaim %s Spec.Image should be nil after retarget", found.Name)
		g.Expect(found.Spec.UserData).To(BeNil(), "HostClaim %s Spec.UserData should be nil after retarget", found.Name)
		g.Expect(found.Spec.MetaData).To(BeNil(), "HostClaim %s Spec.MetaData should be nil after retarget", found.Name)
		g.Expect(found.Spec.NetworkData).To(BeNil(), "HostClaim %s Spec.NetworkData should be nil after retarget", found.Name)
		g.Expect(found.Spec.PoweredOn).To(BeFalse(), "HostClaim %s Spec.PoweredOn should be false after retarget", found.Name)
		g.Expect(found.Annotations).To(HaveKey(nodeReuseCleared), "HostClaim %s missing %s annotation after retarget", found.Name, nodeReuseCleared)
		claimName = found.Name
	}, intervals...).Should(Succeed())
	return claimName
}

// assertHostClaimRebound waits until the kept HostClaim is rebound to a
// Metal3Machine (ConsumerRef.Kind back to Metal3Machine) and the
// nodeReuseCleared annotation is gone.
func assertHostClaimRebound(
	ctx context.Context, cl client.Client, namespace, claimName string,
	intervals []interface{},
) {
	const nodeReuseCleared = "infrastructure.cluster.x-k8s.io/node-reuse-cleared-at"
	Eventually(func(g Gomega) {
		hc := bmov1alpha1.HostClaim{}
		g.Expect(cl.Get(ctx, client.ObjectKey{Namespace: namespace, Name: claimName}, &hc)).To(Succeed())
		g.Expect(hc.Spec.ConsumerRef).NotTo(BeNil(), "HostClaim %s has no ConsumerRef after rebind", claimName)
		g.Expect(hc.Spec.ConsumerRef.Kind).To(Equal("Metal3Machine"),
			"HostClaim %s ConsumerRef.Kind should be Metal3Machine after rebind, got %s", claimName, hc.Spec.ConsumerRef.Kind)
		g.Expect(hc.Annotations).NotTo(HaveKey(nodeReuseCleared),
			"HostClaim %s nodeReuseCleared annotation should be removed after rebind", claimName)
	}, intervals...).Should(Succeed())
}

// verifyMetal3MachineHostClaimAssociation waits until the Metal3Machine that
// now owns the given HostClaim surfaces AssociateBareMetalHostCondition=True
// with reason AssociateHostClaimSuccessReason. Weaker than the BMH-path check
// in verifyMetal3MachineNodeReuseCondition because no HostClaim-specific
// "via node reuse" reason exists; adding one would let this assertion be
// strengthened.
func verifyMetal3MachineHostClaimAssociation(
	ctx context.Context, cl client.Client, namespace, claimName string,
	intervals []interface{},
) {
	By("Verify Metal3Machine for the reused HostClaim has AssociateHostClaimSuccessReason [claim_reuse]")
	Eventually(func(g Gomega) {
		hc := bmov1alpha1.HostClaim{}
		g.Expect(cl.Get(ctx, client.ObjectKey{Namespace: namespace, Name: claimName}, &hc)).To(Succeed())
		g.Expect(hc.Spec.ConsumerRef).NotTo(BeNil(), "HostClaim %s has no ConsumerRef", claimName)
		g.Expect(hc.Spec.ConsumerRef.Kind).To(Equal("Metal3Machine"))
		m3mName := hc.Spec.ConsumerRef.Name
		g.Expect(m3mName).NotTo(BeEmpty())

		m3m := infrav1.Metal3Machine{}
		g.Expect(cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: m3mName}, &m3m)).To(Succeed())
		cond := conditions.Get(&m3m, infrav1.AssociateBareMetalHostCondition)
		g.Expect(cond).NotTo(BeNil(), "AssociateBareMetalHostCondition not found on Metal3Machine %s", m3mName)
		g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(cond.Reason).To(Equal(infrav1.AssociateHostClaimSuccessReason),
			"Expected AssociateHostClaimSuccessReason, got %s", cond.Reason)
	}, intervals...).Should(Succeed())
}

// scrubStaleHostClaimConsumerRefs clears Spec.ConsumerRef on HostClaims whose
// ConsumerRef.Kind == kind (typically "KubeadmControlPlane" after a CP
// scale-down) so they can be picked up by provideHostClaim for a different
// owner. Mirrors the BMH-side scrub in node_reuse.go lines 326-355.
func scrubStaleHostClaimConsumerRefs(ctx context.Context, cl client.Client, namespace, kind string) {
	hcs := bmov1alpha1.HostClaimList{}
	Expect(cl.List(ctx, &hcs, client.InNamespace(namespace))).To(Succeed())
	for i := range hcs.Items {
		hc := &hcs.Items[i]
		if hc.Spec.ConsumerRef == nil || hc.Spec.ConsumerRef.Kind != kind {
			continue
		}
		helper, err := patch.NewHelper(hc, cl)
		Expect(err).NotTo(HaveOccurred())
		hc.Spec.ConsumerRef = nil
		Expect(helper.Patch(ctx, hc)).To(Succeed(), "clearing stale ConsumerRef on HostClaim %s", hc.Name)
	}

	// Verify none remain.
	Eventually(func(g Gomega) {
		hcs := bmov1alpha1.HostClaimList{}
		g.Expect(cl.List(ctx, &hcs, client.InNamespace(namespace))).To(Succeed())
		for i := range hcs.Items {
			hc := &hcs.Items[i]
			if hc.Spec.ConsumerRef != nil {
				g.Expect(hc.Spec.ConsumerRef.Kind).NotTo(Equal(kind), "HostClaim %s still bound to %s", hc.Name, kind)
			}
		}
	}, 30*time.Second, 5*time.Second).Should(Succeed())
}
