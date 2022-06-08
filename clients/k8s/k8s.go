// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package k8s

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	k8sRuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/joamaki/goreactive/stream"
)

type Item[T k8sRuntime.Object] struct {
	// Key for the object in the form "<namespace>/<name>"
	Key string

	// The object itself, nil if object was deleted.
	Object T
}

func Resource[T k8sRuntime.Object](ctx context.Context, resource string, namespace string, client rest.Interface) stream.Observable[[]Item[T]] {
	// Number of unique items in queue before backpressure from subscriber
	// TODO: configurable
	const queueBufSize = 16

	listWatcher := cache.NewListWatchFromClient(
		client,
		resource,
		namespace,
		fields.Everything(),
	)

	var (
		mu     sync.RWMutex
		subId  int
		queues = make(map[int]workqueue.RateLimitingInterface)
	)

	// function to push the object to all subscribed queues.
	push := func(key string) {
		mu.RLock()
		for _, queue := range queues {
			queue.Add(key)
		}
		mu.RUnlock()
	}

	var empty T
	indexer, informer := cache.NewIndexerInformer(listWatcher, empty, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				push(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				push(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				push(key)
			}
		},
	}, cache.Indexers{})

	// TODO: wait for Run() to return?
	go informer.Run(ctx.Done())

	return stream.FuncObservable[[]Item[T]](
		func(subCtx context.Context, next func([]Item[T]) error) error {
			// Wait for the cache to sync, so we can emit a consistent object set
			// as the first item.
			if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
				return fmt.Errorf("Timed out waiting for caches to sync")
			}

			nextErrs := make(chan error, 1)
			defer close(nextErrs)

			outErr := make(chan error, 1)

			// Subscribe to changes first so they would not be missed.
			mu.Lock()
			subId++
			queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
			queues[subId] = queue
			mu.Unlock()

			// Fork a goroutine to wait for either context cancellation or
			// for completion to then clean up the subscriber queue.
			go func() {
				var err error
				select {
				case <-ctx.Done():
					err = ctx.Err()
				case <-subCtx.Done():
					err = ctx.Err()
				case err = <-nextErrs:
				}

				outErr <- err
				close(outErr)

				mu.Lock()
				delete(queues, subId)
				mu.Unlock()

				queue.ShutDownWithDrain()
			}()

			// Emit all existing objects as the first slice
			var items []Item[T]
			initialVersions := make(map[string]string)
			for _, obj := range indexer.List() {
				key, err := cache.MetaNamespaceKeyFunc(obj)
				if err == nil {
					items = append(items, Item[T]{key, obj.(T)})
					initialVersions[key] = resourceVersion(obj)
				}
			}
			if err := next(items); err != nil {
				nextErrs <- err
				return <-outErr
			}

			// And then start draining the queue.
			for {
				rawKey, shutdown := queue.Get()
				if shutdown {
					break
				}
				queue.Done(rawKey)
				key := rawKey.(string)
				rawObj, exists, err := indexer.GetByKey(key)
				if err != nil {
					// TODO: double check if this handling makes any sense. what possible
					// errors could we get from the cache.Store?
					if queue.NumRequeues(key) < 5 {
						queue.AddRateLimited(rawKey)
						continue
					}
				}
				queue.Forget(rawKey)

				var obj T
				if exists {
					obj = rawObj.(T)
				}

				if len(initialVersions) > 0 {
					version := resourceVersion(obj)
					if initialVersion, ok := initialVersions[key]; ok {
						// We can now forget the initial version.
						delete(initialVersions, key)
						if initialVersion == version {
							// Already emitted, skip.
							continue
						}
					}
				}

				// TODO: here we could requeue and try the item again on error
				items = []Item[T]{{key, obj}}
				if err := next(items); err != nil {
					nextErrs <- err
					return <-outErr
				}
			}
			nextErrs <- nil
			return <-outErr
		})
}

func resourceVersion(obj any) (version string) {
	meta, err := meta.Accessor(obj)
	if err == nil {
		return meta.GetResourceVersion()
	}
	return ""
}
