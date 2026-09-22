/*
Copyright 2024 Orange.

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
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/scheme"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const channelSize = 100

type RemoteClientCacheInterface interface {
	GetRemoteClient(ctx context.Context, log logr.Logger, m3machine *infrav1.Metal3Machine) (*RemoteClient, error)
	GetWatchChannel() chan event.GenericEvent
	Start(ctx context.Context) error // Exported for the manager
}

/* Remote clients is a kind of cache for clients on remote clusters and for
   watch over HostClaims on those remote clusters. They are attached to metal3machine
   and disapear when all the metal3machines associated to them are deleted. */

type RemoteClient struct {
	client.WithWatch
	WatchHost         watch.Interface
	Namespace         string
	Consumers         map[string]bool
	mutex             sync.Mutex
	remoteClientCache *RemoteClientCache
	key               string
}

// remoteClientBuilder builds a remote client, resolves its default namespace and
// opens a HostClaim watch for a given Metal3Machine. It is a seam so that tests
// can inject a fake client and watch without needing a live remote API server.
// watchCtx is the long-lived context used for the Watch call.
type remoteClientBuilder func(
	ctx context.Context,
	watchCtx context.Context,
	log logr.Logger,
	scheme *runtime.Scheme,
	cl client.Client,
	m3machine *infrav1.Metal3Machine,
) (client.WithWatch, string, watch.Interface, error)

type RemoteClientCache struct {
	// LocalClient is a client to query the local cluster
	LocalClient client.Client
	// watchChannels is the global watch channel accumulating events from remote clients
	watchChannels []chan event.GenericEvent
	// RemoteClients is a map associating remote client to each pair kubeconfig/context
	RemoteClients map[string]*RemoteClient
	// scheme is the scheme used for requests on the remote clusters (Core + bmo)
	scheme *runtime.Scheme
	// mutex is a global mutex for the cache.
	mutex sync.Mutex
	// ctx is a long life context managed by the manager.
	ctx context.Context //nolint:containedctx
	// remoteClientBuilder produces (client, namespace, watch) for a machine. Overridable by tests.
	remoteClientBuilder remoteClientBuilder
}

// defaultRemoteClientBuilder creates an actual client and the associated watch interface.
func defaultRemoteClientBuilder(
	ctx, watchCtx context.Context, log logr.Logger, scheme *runtime.Scheme,
	cl client.Client, m3machine *infrav1.Metal3Machine,
) (client.WithWatch, string, watch.Interface, error) {
	rclient, ns, err := createRemoteClient(ctx, log, scheme, cl, m3machine)
	if err != nil {
		return nil, "", nil, err
	}
	watchInterface, err := rclient.Watch(watchCtx, &bmov1alpha1.HostClaimList{}, client.InNamespace(ns))
	if err != nil {
		return nil, "", nil, err
	}
	return rclient, ns, watchInterface, nil
}

func NewRemoteClientCache(cl client.Client) RemoteClientCacheInterface {
	remoteClients := make(map[string]*RemoteClient)
	rccScheme := runtime.NewScheme()
	_ = scheme.AddToScheme(rccScheme)
	_ = bmov1alpha1.AddToScheme(rccScheme)
	return &RemoteClientCache{
		LocalClient:         cl,
		RemoteClients:       remoteClients,
		scheme:              rccScheme,
		ctx:                 context.Background(),
		remoteClientBuilder: defaultRemoteClientBuilder,
	}
}

// Start implements manager.Runnable. Called once by the manager;
// ctx is cancelled when the manager shuts down.
func (rcc *RemoteClientCache) Start(ctx context.Context) error {
	rcc.mutex.Lock()
	rcc.ctx = ctx
	rcc.mutex.Unlock()
	<-ctx.Done() // block otherwise considered as non running by manager
	return nil
}

func createRemoteClient(ctx context.Context, log logr.Logger, scheme *runtime.Scheme, cl client.Client, m3machine *infrav1.Metal3Machine) (client.WithWatch, string, error) {
	idRef := m3machine.Spec.IdentityRef
	if idRef.Name == "" {
		return nil, "", errors.New("no kubeconfig referenced")
	}
	namespace := m3machine.Namespace
	kcSecretName := idRef.Name
	keySecret := client.ObjectKey{
		Name:      kcSecretName,
		Namespace: namespace,
	}
	log.V(1).Info("Generate new remote client to access hosts", "secretName", kcSecretName, "namespace", namespace)
	secret := corev1.Secret{}
	err := cl.Get(ctx, keySecret, &secret)
	if err != nil {
		log.Error(err, "Cannot get kubeconfig for remote cluster access", "secretName", kcSecretName)
		return nil, "", err
	}
	kubeconfig, ok := secret.Data["kubeconfig"]
	if !ok {
		err = fmt.Errorf("no kubeconfig field in secret %s/%s", namespace, kcSecretName)
		log.Error(err, "Missing data in secret for remote cluster access", "secretName", kcSecretName)
		return nil, "", err
	}
	cc, err := clientcmd.Load(kubeconfig)
	if err != nil {
		log.Error(err, "Cannot get client config for remote cluster access")
		return nil, "", err
	}
	conf := clientcmd.NewNonInteractiveClientConfig(*cc, idRef.Context, &clientcmd.ConfigOverrides{}, nil)
	restconf, err := conf.ClientConfig()
	if err != nil {
		log.Error(err, "Cannot get client config for remote cluster access")
		return nil, "", err
	}
	ns, _, err := conf.Namespace()
	if err != nil {
		log.Error(err, "Cannot extract default namespace from config")
		return nil, "", err
	}
	remoteClient, err := client.NewWithWatch(restconf, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		log.Error(err, "Cannot create REST client for remote cluster access")
		return nil, "", err
	}
	return remoteClient, ns, nil
}

func (rcc *RemoteClientCache) GetWatchChannel() chan event.GenericEvent {
	channel := make(chan event.GenericEvent, channelSize)
	rcc.watchChannels = append(rcc.watchChannels, channel)
	return channel
}

// Release tracks that the (deleted) metal3machine no longer needs an access to the
// client.
func (remoteClient *RemoteClient) Release(m3machine *infrav1.Metal3Machine) {
	consumer := m3machine.Namespace + "/" + m3machine.Name
	remoteClient.mutex.Lock()
	delete(remoteClient.Consumers, consumer)
	l := len(remoteClient.Consumers)
	remoteClient.mutex.Unlock()
	if l == 0 {
		// Stops the spawned watcher
		remoteClient.remoteClientCache.mutex.Lock()
		delete(remoteClient.remoteClientCache.RemoteClients, remoteClient.key)
		remoteClient.remoteClientCache.mutex.Unlock()
		remoteClient.WatchHost.Stop()
	}
}

func (rcc *RemoteClientCache) GetRemoteClient(ctx context.Context, log logr.Logger, m3machine *infrav1.Metal3Machine) (*RemoteClient, error) {
	idRef := m3machine.Spec.IdentityRef
	if idRef.Name == "" {
		return nil, nil //nolint:nilnil
	}
	namespace := m3machine.Namespace
	key := namespace + "/" + idRef.Name + "/" + idRef.Context
	consumer := namespace + "/" + m3machine.Name
	rcc.mutex.Lock()
	defer rcc.mutex.Unlock()
	remoteClient, ok := rcc.RemoteClients[key]
	if ok {
		remoteClient.mutex.Lock()
		remoteClient.Consumers[consumer] = true
		remoteClient.mutex.Unlock()
		log.V(1).Info("Use existing remote client to access hosts", "key", key)
		return remoteClient, nil
	}
	rclient, remoteNs, watchInterface, err := rcc.remoteClientBuilder(ctx, rcc.ctx, log, rcc.scheme, rcc.LocalClient, m3machine)
	if err != nil {
		log.Error(err, "Failed to get remote client")
		return nil, err
	}
	// This goroutine should exit when the channel is closed. The channel is closed when Stop is
	// called on the watch.Interface object.
	go func() {
		for evt := range watchInterface.ResultChan() {
			// cast promotes the runtime object to a client object
			if obj, ok := evt.Object.(*bmov1alpha1.HostClaim); ok {
				ev := event.GenericEvent{
					Object: obj,
				}
				for _, ch := range rcc.watchChannels {
					ch <- ev
				}
			}
		}
	}()
	remoteClient = &RemoteClient{
		WithWatch:         rclient,
		Namespace:         remoteNs,
		WatchHost:         watchInterface,
		Consumers:         map[string]bool{consumer: true},
		remoteClientCache: rcc,
		key:               key,
	}
	// rcc.mutex is already held by the defer above.
	rcc.RemoteClients[key] = remoteClient
	log.V(1).Info("Return remote client")
	return remoteClient, nil
}
