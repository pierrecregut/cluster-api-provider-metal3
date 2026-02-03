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
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	"github.com/metal3-io/cluster-api-provider-metal3/baremetal"
	"github.com/metal3-io/cluster-api-provider-metal3/internal/metrics"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	k8strings "k8s.io/utils/strings"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/predicates"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var (
	bmhSyncInterval = 60 * time.Second
)

const (
	labelSyncControllerName = "metal3-label-sync-controller"
)

// Metal3LabelSyncReconciler reconciles label updates to BareMetalHost objects with the corresponding K Node objects in the workload cluster.
type Metal3LabelSyncReconciler struct {
	Client           client.Client
	ClusterCache     clustercache.ClusterCache
	ManagerFactory   baremetal.ManagerFactoryInterface
	Log              logr.Logger
	CapiClientGetter baremetal.ClientGetter
	WatchFilterValue string
	RemoteClients    baremetal.RemoteClientCacheInterface
}

// +kubebuilder:rbac:groups=metal3.io,resources=baremetalhosts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=metal3.io,resources=baremetalhosts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=metal3.io,resources=hostclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=metal3.io,resources=hardwaredata,verbs=get;list;watch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=metal3machines,verbs=get;list;watch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=metal3machines/status,verbs=get
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines;machines/status,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile handles label sync events.
func (r *Metal3LabelSyncReconciler) Reconcile(ctx context.Context, req ctrl.Request) (rres ctrl.Result, rerr error) {
	reconcileStart := time.Now()
	controllerLog := r.Log.WithName(labelSyncControllerName).WithValues(
		baremetal.LogFieldMetal3Machine, req.NamespacedName,
	)
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Reconcile: starting label sync reconciliation")
	// Record metrics at the end of reconciliation
	defer func() {
		hasError := rerr != nil
		metrics.RecordMetal3LabelSyncReconcile(req.Namespace, reconcileStart, hasError)
		if rerr != nil {
			metrics.RecordReconcileError(labelSyncControllerName, req.Namespace, false)
		}
	}()

	var hostLabels map[string]string

	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Fetching Metal3Machine from ConsumerRef")
	capm3Machine := &infrav1.Metal3Machine{}
	if err := r.Client.Get(ctx, req.NamespacedName, capm3Machine); err != nil {
		if apierrors.IsNotFound(err) {
			controllerLog.Info("Could not find Metal3Machine, will retry")
			return ctrl.Result{RequeueAfter: requeueAfter}, nil
		}
		return ctrl.Result{}, err
	}
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Found Metal3Machine")

	// Fetch the Machine.
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Fetching owner Machine")
	capiMachine, err := util.GetOwnerMachine(ctx, r.Client, capm3Machine.ObjectMeta)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("metal3Machine's owner Machine could not be retrieved: %w", err)
	}
	if capiMachine == nil {
		controllerLog.Info("Could not find Machine object, will retry")
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
	controllerLog = controllerLog.WithValues(baremetal.LogFieldMachine, capiMachine.Name)
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Found Machine")

	if !capiMachine.Status.NodeRef.IsDefined() {
		controllerLog.Info("Could not find Node Ref on Machine object, will retry")
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
	controllerLog.V(baremetal.VerbosityLevelDebug).Info("NodeRef found",
		baremetal.LogFieldNode, capiMachine.Status.NodeRef.Name)

	// Fetch the Cluster
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Fetching Cluster")
	cluster := &clusterv1.Cluster{}
	clusterKey := client.ObjectKey{
		Name:      capiMachine.Spec.ClusterName,
		Namespace: capiMachine.Namespace,
	}
	err = r.Client.Get(ctx, clusterKey, cluster)
	if err != nil {
		controllerLog.V(baremetal.VerbosityLevelDebug).Info("Failed to fetch Cluster",
			baremetal.LogFieldError, err.Error())
		controllerLog.Info("Error fetching cluster, will retry")
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}
	controllerLog = controllerLog.WithValues(baremetal.LogFieldCluster, cluster.Name)
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Found Cluster")

	// Fetch the Metal3 cluster.
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Fetching Metal3Cluster")
	metal3Cluster := &infrav1.Metal3Cluster{}
	metal3ClusterName := types.NamespacedName{
		Namespace: capm3Machine.Namespace,
		Name:      cluster.Spec.InfrastructureRef.Name,
	}
	if err = r.Client.Get(ctx, metal3ClusterName, metal3Cluster); err != nil {
		controllerLog.V(baremetal.VerbosityLevelDebug).Info("Failed to fetch Metal3Cluster",
			baremetal.LogFieldError, err.Error())
		controllerLog.Info("Error fetching Metal3Cluster, will retry")
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}
	controllerLog = controllerLog.WithValues(baremetal.LogFieldMetal3Cluster, metal3Cluster.Name)
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Found Metal3Cluster")

	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Checking pause status")
	if annotations.IsPaused(cluster, metal3Cluster) {
		controllerLog.V(baremetal.VerbosityLevelDebug).Info("Cluster or Metal3Cluster is paused")
		controllerLog.Info("Cluster and/or Metal3Cluster are currently paused. Remove pause to continue reconciliation.")
		return ctrl.Result{RequeueAfter: bmhSyncInterval}, nil
	}

	// Get prefix set
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Getting prefix annotations from Metal3Cluster")
	annotations := metal3Cluster.ObjectMeta.GetAnnotations()
	if annotations == nil {
		controllerLog.V(baremetal.VerbosityLevelDebug).Info("No annotations on Metal3Cluster")
		return ctrl.Result{}, nil
	}
	prefixStr, ok := annotations[baremetal.PrefixAnnotationKey]
	if !ok {
		controllerLog.V(baremetal.VerbosityLevelDebug).Info("No prefix annotation found on Metal3Cluster")
		return ctrl.Result{}, nil
	}
	controllerLog.V(baremetal.VerbosityLevelDebug).Info("Found prefix annotation",
		"prefixes", prefixStr)

	m3mAnnotations := capm3Machine.ObjectMeta.GetAnnotations()
	if m3mAnnotations == nil {
		return ctrl.Result{}, nil
	}

	controllerLog.V(baremetal.VerbosityLevelDebug).Info("Metal3Machine annotations", "m3m", m3mAnnotations)
	if capm3Machine.Spec.HostSelector != nil && capm3Machine.Spec.HostSelector.HostClaim == infrav1.HostClaimEnabled {
		hostClaimName, ok := m3mAnnotations[baremetal.HostClaimAnnotation]
		if ok {
			cl := r.Client
			remoteClient, err2 := r.RemoteClients.GetRemoteClient(ctx, controllerLog, capm3Machine)
			if err2 != nil {
				controllerLog.Error(err2, "Cannot get client on remote host provisioner cluster")
				return ctrl.Result{}, err2
			}
			hostClaimNamespace := capm3Machine.Namespace
			if remoteClient != nil {
				cl = remoteClient
				hostClaimNamespace = remoteClient.Namespace
			}
			hostClaim := &bmov1alpha1.HostClaim{}
			hostClaimKey := client.ObjectKey{Namespace: hostClaimNamespace, Name: hostClaimName}
			err = cl.Get(ctx, hostClaimKey, hostClaim)
			if apierrors.IsNotFound(err) {
				controllerLog.V(baremetal.VerbosityLevelDebug).Info("Annotated hostclaim not found", baremetal.LogFieldHostClaim, hostClaimKey)
				return ctrl.Result{}, nil
			} else if err != nil {
				return ctrl.Result{}, err
			}

			controllerLog.V(baremetal.VerbosityLevelDebug).Info("Hostclaim", baremetal.LogFieldName, hostClaim.Name, baremetal.LogFieldNamespace, hostClaim.Namespace, baremetal.LogFieldAnnotation,
				hostClaim.Annotations, baremetal.LogFieldLabel, hostClaim.Labels)

			if hostClaim.Annotations != nil {
				if _, ok := hostClaim.Annotations[bmov1alpha1.PausedAnnotation]; ok {
					controllerLog.V(baremetal.VerbosityLevelDebug).Info("Hostclaim is currently paused. Remove pause to continue reconciliation.")
					return ctrl.Result{}, nil
				}
			}

			if hostClaim.Status.HardwareData == nil {
				controllerLog.V(baremetal.VerbosityLevelDebug).Info("HardwareData not present")
				return ctrl.Result{}, nil
			}
			hardwareData := &bmov1alpha1.HardwareData{}
			hardwareDataName := hostClaim.Status.HardwareData.Name
			hardwareDataNamespace := hostClaim.Status.HardwareData.Namespace
			hardwareDataKey := client.ObjectKey{Namespace: hardwareDataNamespace, Name: hardwareDataName}
			err = cl.Get(ctx, hardwareDataKey, hardwareData)
			if apierrors.IsNotFound(err) {
				controllerLog.V(baremetal.VerbosityLevelDebug).Info("hardwaredata not found", "hardwareDataKey", hardwareDataKey)
				return ctrl.Result{RequeueAfter: bmhSyncInterval}, nil
			} else if err != nil {
				return ctrl.Result{}, err
			}

			hostLabels = hardwareData.Labels
			controllerLog.V(baremetal.VerbosityLevelDebug).Info("Hardwaredata labels", "name", hardwareDataName, "labels", hostLabels)
			controllerLog.V(baremetal.VerbosityLevelDebug).Info("Synchronizing labels between hardwaredata and node")
		} else {
			return ctrl.Result{}, nil
		}
	} else {
		bareMetalHostKey, ok := m3mAnnotations[baremetal.HostAnnotation]
		if ok {
			_, bareMetalHostName, err2 := cache.SplitMetaNamespaceKey(bareMetalHostKey)
			if err2 != nil {
				controllerLog.Error(err2, "Error parsing annotation value", "annotation key", bareMetalHostKey)
				return ctrl.Result{}, err2
			}

			bareMetalHost := bmov1alpha1.BareMetalHost{}
			key := client.ObjectKey{
				Name:      bareMetalHostName,
				Namespace: capm3Machine.Namespace,
			}
			err = r.Client.Get(ctx, key, &bareMetalHost)
			if apierrors.IsNotFound(err) {
				controllerLog.V(baremetal.VerbosityLevelDebug).Info("Annotated baremetalhost not found", "host", bareMetalHostKey)
				return ctrl.Result{}, nil
			} else if err != nil {
				return ctrl.Result{}, err
			}

			if bareMetalHost.Annotations != nil {
				if _, ok := bareMetalHost.Annotations[bmov1alpha1.PausedAnnotation]; ok {
					controllerLog.V(baremetal.VerbosityLevelDebug).Info("BaremetalHost is currently paused. Remove pause to continue reconciliation.")
					return ctrl.Result{RequeueAfter: bmhSyncInterval}, nil
				}
			}

			hostLabels = bareMetalHost.Labels
			controllerLog.V(baremetal.VerbosityLevelDebug).Info("Synchronizing labels between baremetalhost and node")
		} else {
			return ctrl.Result{}, nil
		}
	}

	prefixSet, err := baremetal.ParsePrefixAnnotation(prefixStr)
	if err != nil {
		controllerLog.V(baremetal.VerbosityLevelDebug).Info("Failed to parse prefix annotation",
			baremetal.LogFieldError, err.Error())
		return ctrl.Result{}, err
	}
	controllerLog.V(baremetal.VerbosityLevelDebug).Info("Prefix set parsed",
		baremetal.LogFieldCount, len(prefixSet))

	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Reconciling BMH labels to Node")
	err = r.reconcileBMHLabels(ctx, hostLabels, capiMachine, cluster, prefixSet, controllerLog)
	if err != nil {
		controllerLog.V(baremetal.VerbosityLevelDebug).Info("Failed to reconcile labels",
			baremetal.LogFieldError, err.Error())
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}
	controllerLog.Info("Finished synchronizing labels between BaremetalHost and Node")
	controllerLog.V(baremetal.VerbosityLevelTrace).Info("Reconcile: completed successfully")
	// Always requeue to ensure label sync runs periodically for each BareMetalHost. This is necessary to catch any label updates to the Node that are synchronized through the BareMetalHost.
	return ctrl.Result{RequeueAfter: bmhSyncInterval}, nil
}

func (r *Metal3LabelSyncReconciler) reconcileBMHLabels(ctx context.Context, hostLabels map[string]string, machine *clusterv1.Machine, cluster *clusterv1.Cluster, prefixSet map[string]struct{}, log logr.Logger) error {
	log.V(baremetal.VerbosityLevelTrace).Info("reconcileBMHLabels: starting label synchronization")
	log.V(baremetal.VerbosityLevelTrace).Info("Building host label sync set")
	hostLabelSyncSet := buildLabelSyncSet(prefixSet, hostLabels)
	log.V(baremetal.VerbosityLevelDebug).Info("Host label sync set built",
		baremetal.LogFieldCount, len(hostLabelSyncSet))

	// Get the Node from the workload cluster
	log.V(baremetal.VerbosityLevelTrace).Info("Getting remote client for workload cluster")
	corev1Remote, err := r.CapiClientGetter(ctx, r.Client, cluster)
	if err != nil {
		return fmt.Errorf("error creating a remote client: %w", err)
	}

	log.V(baremetal.VerbosityLevelTrace).Info("Fetching Node from workload cluster",
		baremetal.LogFieldNode, machine.Status.NodeRef.Name)
	node, err := corev1Remote.Nodes().Get(ctx, machine.Status.NodeRef.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	log.V(baremetal.VerbosityLevelDebug).Info("Node fetched from workload cluster",
		baremetal.LogFieldNode, node.Name)

	log.V(baremetal.VerbosityLevelTrace).Info("Building node label sync set")
	nodeLabelSyncSet := buildLabelSyncSet(prefixSet, node.Labels)
	log.V(baremetal.VerbosityLevelDebug).Info("Node label sync set built",
		baremetal.LogFieldCount, len(nodeLabelSyncSet))

	log.V(baremetal.VerbosityLevelTrace).Info("Synchronizing labels on node")
	synchronizeLabelSyncSetsOnNode(hostLabelSyncSet, nodeLabelSyncSet, node)

	log.V(baremetal.VerbosityLevelTrace).Info("Updating node with synchronized labels")
	_, err = corev1Remote.Nodes().Update(ctx, node, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("unable to update the target node: %w", err)
	}
	log.V(baremetal.VerbosityLevelDebug).Info("Node updated successfully")
	log.V(baremetal.VerbosityLevelTrace).Info("reconcileBMHLabels: completed successfully")
	return nil
}

func buildLabelSyncSet(prefixSet map[string]struct{}, labels map[string]string) map[string]string {
	labelSyncSet := make(map[string]string)
	for labelKey, labelVal := range labels {
		p, n := k8strings.SplitQualifiedName(labelKey)
		if p == "" || n == "" {
			continue
		}
		if _, ok := prefixSet[p]; !ok {
			continue
		}
		labelSyncSet[labelKey] = labelVal
	}
	return labelSyncSet
}

func synchronizeLabelSyncSetsOnNode(hostLabelSyncSet, nodeLabelSyncSet map[string]string, node *corev1.Node) {
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	for labelKey, labelVal := range nodeLabelSyncSet {
		val, ok := hostLabelSyncSet[labelKey]
		if !ok || val != labelVal {
			delete(node.Labels, labelKey)
		}
	}
	for labelKey, labelVal := range hostLabelSyncSet {
		val, ok := nodeLabelSyncSet[labelKey]
		if !ok || val != labelVal {
			node.Labels[labelKey] = labelVal
		}
	}
}

// SetupWithManager will add watches for this controller.
func (r *Metal3LabelSyncReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager, options controller.Options) error {
	if r.RemoteClients == nil {
		return errors.New("remoteClients must not be nil")
	}
	src := source.Channel(r.RemoteClients.GetWatchChannel(), handler.EnqueueRequestsFromMapFunc(r.HostClaimToMetal3Machines))
	return ctrl.NewControllerManagedBy(mgr).
		Named("labelsync-controller").
		For(&infrav1.Metal3Machine{}).
		WithOptions(options).
		Watches(
			&infrav1.Metal3Cluster{},
			handler.EnqueueRequestsFromMapFunc(r.Metal3ClusterToMetal3Machine),
		).
		Watches(
			&bmov1alpha1.HostClaim{},
			handler.EnqueueRequestsFromMapFunc(r.HostClaimToMetal3Machines),
		).
		Watches(
			&bmov1alpha1.BareMetalHost{},
			handler.EnqueueRequestsFromMapFunc(r.BareMetalHostToMetal3Machines),
		).
		WatchesRawSource(src).
		WithEventFilter(predicates.ResourceNotPausedAndHasFilterLabel(mgr.GetScheme(), ctrl.LoggerFrom(ctx), r.WatchFilterValue)).
		Complete(r)
}

// BareMetalHostToMetal3Machines will return a reconcile request for a Metal3Machine if the event is for a
// BareMetalHost and that BareMetalHost references a Metal3Machine.
func (r *Metal3LabelSyncReconciler) BareMetalHostToMetal3Machines(_ context.Context, obj client.Object) []ctrl.Request {
	if host, ok := obj.(*bmov1alpha1.BareMetalHost); ok {
		log := r.Log.WithValues(baremetal.LogFieldBMH, host.Name, baremetal.LogFieldNamespace, host.Namespace)
		if host.Spec.ConsumerRef != nil &&
			host.Spec.ConsumerRef.Kind == metal3MachineKind &&
			host.Spec.ConsumerRef.Namespace == host.Namespace && // bmh and m3m must be in same namespace
			host.Spec.ConsumerRef.GroupVersionKind().Group == infrav1.GroupVersion.Group {
			log.V(baremetal.VerbosityLevelTrace).Info("found baremetalhost, enqueuing request", baremetal.LogFieldMetal3Machine, host.Spec.ConsumerRef.Name)
			return metal3MachinesRequest(host.Spec.ConsumerRef)
		}
	} else {
		r.Log.Error(fmt.Errorf("expected a BareMetalHost but got a %T", obj),
			"failed to get Metal3Machine for BareMetalHost",
		)
	}
	return []ctrl.Request{}
}

// HostClaimToMetal3Machines will return a reconcile request for a Metal3Machine if the event is for a
// local HostClaim and that HostClaim references a Metal3Machine.
func (r *Metal3LabelSyncReconciler) HostClaimToMetal3Machines(_ context.Context, obj client.Object) []ctrl.Request {
	if host, ok := obj.(*bmov1alpha1.HostClaim); ok {
		log := r.Log.WithValues(baremetal.LogFieldHostClaim, host.Name, baremetal.LogFieldHostClaimNamespace, host.Namespace)
		if host.Spec.ConsumerRef != nil &&
			(host.Spec.ConsumerRef.Kind == metal3MachineKind || host.Spec.ConsumerRef.Kind == remoteMetal3MachineKind) &&
			host.Spec.ConsumerRef.GroupVersionKind().Group == infrav1.GroupVersion.Group {
			log.V(baremetal.VerbosityLevelTrace).Info("found hostclaim, enqueuing request", baremetal.LogFieldMetal3Machine, host.Spec.ConsumerRef.Name,
				baremetal.LogFieldNamespace, host.Spec.ConsumerRef.Namespace)
			return metal3MachinesRequest(host.Spec.ConsumerRef)
		}
	} else {
		r.Log.Error(fmt.Errorf("expected a HostClaim but got a %T", obj),
			"failed to get Metal3Machine for HostClaim",
		)
	}
	return []ctrl.Request{}
}

// metal3MachinesRequest will return a reconcile request for a Metal3Machine
// defined with name and namespace from a consumerRef object.
func metal3MachinesRequest(consumerRef *corev1.ObjectReference) []ctrl.Request {
	m3mObjKey := client.ObjectKey{
		Name:      consumerRef.Name,
		Namespace: consumerRef.Namespace,
	}
	return []ctrl.Request{
		{
			NamespacedName: m3mObjKey,
		},
	}
}

// Metal3ClusterToMetal3Machine is a handler.ToRequestsFunc to be used to enqueue
// requests for reconciliation of BareMetalHosts' label updates.
func (r *Metal3LabelSyncReconciler) Metal3ClusterToMetal3Machine(ctx context.Context, o client.Object) []ctrl.Request {
	result := []ctrl.Request{}
	c, ok := o.(*infrav1.Metal3Cluster)
	if !ok {
		r.Log.Error(fmt.Errorf("expected a Metal3Cluster but got a %T", o),
			"failed to get Metal3Mahine for Metal3Cluster",
		)
		return nil
	}
	log := r.Log.WithValues(
		baremetal.LogFieldMetal3Cluster, c.Name,
		baremetal.LogFieldNamespace, c.Namespace,
	)
	cluster, err := util.GetOwnerCluster(ctx, r.Client, c.ObjectMeta)
	switch {
	case apierrors.IsNotFound(err) || cluster == nil:
		return nil
	case err != nil:
		log.Error(err, "failed to get owning cluster")
		return nil
	}

	labels := map[string]string{clusterv1.ClusterNameLabel: cluster.Name}
	capiMachineList := &clusterv1.MachineList{}
	if err := r.Client.List(ctx, capiMachineList, client.InNamespace(c.Namespace), client.MatchingLabels(labels)); err != nil {
		log.Error(err, "failed to list Machines")
		return nil
	}
	for _, m := range capiMachineList.Items {
		if m.Spec.InfrastructureRef.Name == "" {
			continue
		}

		m3mObjKey := client.ObjectKey{
			Name:      m.Spec.InfrastructureRef.Name,
			Namespace: m.Namespace,
		}
		log.V(baremetal.VerbosityLevelTrace).Info("found Metal3Machine, enqueuing request", "name", m3mObjKey)
		result = append(result, ctrl.Request{NamespacedName: m3mObjKey})
	}
	return result
}
