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
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	bmov1alpha1 "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

type RemoteClientCacheInterface interface {
	GetRemoteClient(log logr.Logger, m3machine *infrav1.Metal3Machine) (*RemoteClient, error)
	GetWatchChannel() chan event.GenericEvent
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

type RemoteClientCache struct {
	LocalClient   client.Client
	WatchChannel  chan event.GenericEvent
	RemoteClients map[string]*RemoteClient
	mutex         sync.Mutex
}

func NewRemoteClientCache(cl client.Client) RemoteClientCacheInterface {
	channel := make(chan event.GenericEvent)
	remoteClients := make(map[string]*RemoteClient)
	return &RemoteClientCache{
		LocalClient:   cl,
		WatchChannel:  channel,
		RemoteClients: remoteClients,
	}
}

func getRemoteClient(log logr.Logger, cl client.Client, m3machine *infrav1.Metal3Machine) (client.WithWatch, string, error) {
	idRef := m3machine.Spec.IdentityRef
	if idRef == nil {
		return nil, "", nil
	}
	namespace := m3machine.Namespace
	kcSecretName := idRef.Name
	keySecret := client.ObjectKey{
		Name:      kcSecretName,
		Namespace: namespace,
	}
	log.V(1).Info("Generate new remote client to access hosts", "secretName", kcSecretName, "namespace", namespace)
	secret := corev1.Secret{}
	ctx := context.Background()
	err := cl.Get(ctx, keySecret, &secret)
	if err != nil {
		log.Error(err, "Cannot get kubeconfig for remote cluster access", "secretName", kcSecretName)
		return nil, "", err
	}
	kubeconfig, ok := secret.Data["kubeconfig"]
	if !ok {
		err := fmt.Errorf("no kubeconfig field in secret %s/%s", namespace, kcSecretName)
		log.Error(err, "Missing data in secret for remote cluster access", "secretName", kcSecretName)
		return nil, "", err
	}
	cc, err := clientcmd.Load(kubeconfig)
	if err != nil {
		log.Error(err, "Cannot get client config for remote cluster access")
		return nil, "", err
	}
	conf := clientcmd.NewInteractiveClientConfig(*cc, idRef.Context, &clientcmd.ConfigOverrides{}, nil, nil)
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
		Scheme: cl.Scheme(),
	})
	if err != nil {
		log.Error(err, "Cannot create REST client for remote cluster access")
		return nil, "", err
	}
	return remoteClient, ns, nil
}

func (rcc *RemoteClientCache) GetWatchChannel() chan event.GenericEvent {
	return rcc.WatchChannel
}

// Implements a reference count garbage collection using metal3machine
func (remoteClient *RemoteClient) Release(m3machine *infrav1.Metal3Machine) {
	consumer := m3machine.Namespace + "/" + m3machine.Name
	remoteClient.mutex.Lock()
	defer remoteClient.mutex.Unlock()
	delete(remoteClient.Consumers, consumer)
	if len(remoteClient.Consumers) == 0 {
		// Stops the spawned watcher
		remoteClient.WatchHost.Stop()
		remoteClient.remoteClientCache.mutex.Lock()
		delete(remoteClient.remoteClientCache.RemoteClients, remoteClient.key)
		remoteClient.remoteClientCache.mutex.Unlock()
	}
}

func (rcc *RemoteClientCache) GetRemoteClient(log logr.Logger, m3machine *infrav1.Metal3Machine) (*RemoteClient, error) {
	idRef := m3machine.Spec.IdentityRef
	if idRef == nil {
		return nil, nil
	}
	namespace := m3machine.Namespace
	key := namespace + "/" + idRef.Name + idRef.Context
	consumer := namespace + "/" + m3machine.Name
	rcc.mutex.Lock()
	remoteClient, ok := rcc.RemoteClients[key]
	rcc.mutex.Unlock()
	if ok {
		remoteClient.mutex.Lock()
		remoteClient.Consumers[consumer] = true
		remoteClient.mutex.Unlock()
		log.V(1).Info("Use existing remote client to access hosts", "key", key)
		return remoteClient, nil
	}
	rclient, namespace, err := getRemoteClient(log, rcc.LocalClient, m3machine)
	if err != nil {
		log.Error(err, "Failed to get remote client")
		return nil, err
	}
	watchInterface, err := rclient.Watch(context.Background(), &bmov1alpha1.HostClaimList{}, client.InNamespace(namespace))
	if err != nil {
		log.Error(err, "Cannot create watch over Hosts")
		return nil, err
	}
	// This goroutine should exit when the channel is closed. The channel is closed when Stop is
	// called on the watch.Interface object.
	go func() {
		for evt := range watchInterface.ResultChan() {
			// cast promotes the runtime object to a client object
			if obj, ok := evt.Object.(*bmov1alpha1.HostClaim); ok {
				rcc.WatchChannel <- event.GenericEvent{
					Object: obj,
				}
			}
		}
	}()
	remoteClient = &RemoteClient{
		WithWatch:         rclient,
		Namespace:         namespace,
		WatchHost:         watchInterface,
		Consumers:         map[string]bool{consumer: true},
		remoteClientCache: rcc,
		key:               key,
	}
	rcc.mutex.Lock()
	rcc.RemoteClients[key] = remoteClient
	rcc.mutex.Unlock()
	log.V(1).Info("Return remote client")
	return remoteClient, nil
}
