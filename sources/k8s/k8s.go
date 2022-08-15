// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package k8s

import (
	"context"
	"errors"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	k8sRuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/joamaki/goreactive/stream"
)

// Key of an K8s object, e.g. name and optional namespace.
type Key struct {
	// Name is the name of the object
	Name string

	// Namespace is the namespace, or empty if object is not namespaced.
	Namespace string
}

func (k Key) String() string {
	if len(k.Namespace) > 0 {
		return k.Namespace + "/" + k.Name
	}
	return k.Name
}

func NewKey(obj any) Key {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		namespace, name, _ := cache.SplitMetaNamespaceKey(d.Key)
		return Key{name, namespace}
	}

	meta, err := meta.Accessor(obj)
	if err != nil {
		return Key{}
	}
	if len(meta.GetNamespace()) > 0 {
		return Key{meta.GetName(), meta.GetNamespace()}
	}
	return Key{meta.GetName(), ""}
}

// Event emitted from resource. One of SyncEvent, UpdateEvent or DeleteEvent.
type Event[T k8sRuntime.Object] interface {
	isEvent(T)

	// Dispatch dispatches to the right event handler. Prefer this over
	// type switch on event.
	Dispatch(
		onSync func(Store[T]),
		onUpdate func(Key, T),
		onDelete func(Key),
	)
}

// SyncEvent is emitted when the store has completed the initial synchronization
// with the cluster.
type SyncEvent[T k8sRuntime.Object] struct {
	Store Store[T]
}

var _ Event[*corev1.Node] = &SyncEvent[*corev1.Node]{}

func (*SyncEvent[T]) isEvent(T) {}
func (s *SyncEvent[T]) Dispatch(onSync func(store Store[T]), onUpdate func(Key, T), onDelete func(Key)) {
	onSync(s.Store)
}

// UpdateEvent is emitted when an object has been added or updated
type UpdateEvent[T k8sRuntime.Object] struct {
	Key    Key
	Object T
}

var _ Event[*corev1.Node] = &UpdateEvent[*corev1.Node]{}

func (*UpdateEvent[T]) isEvent(T) {}
func (ev *UpdateEvent[T]) Dispatch(onSync func(Store[T]), onUpdate func(Key, T), onDelete func(Key)) {
	onUpdate(ev.Key, ev.Object)
}

// DeleteEvent is emitted when an object has been deleted
type DeleteEvent[T k8sRuntime.Object] struct {
	Key Key
}

var _ Event[*corev1.Node] = &DeleteEvent[*corev1.Node]{}

func (*DeleteEvent[T]) isEvent(T) {}
func (ev *DeleteEvent[T]) Dispatch(onSync func(Store[T]), onUpdate func(Key, T), onDelete func(Key)) {
	onDelete(ev.Key)
}

// Store is a read-only typed wrapper for cache.Store.
type Store[T k8sRuntime.Object] interface {
	List() []T
	ListKeys() []string
	Get(obj T) (item T, exists bool, err error)
	GetByKey(key string) (item T, exists bool, err error)
}

type typedStore[T k8sRuntime.Object] struct {
	store cache.Store
}

var _ Store[*corev1.Node] = &typedStore[*corev1.Node]{}

func (s *typedStore[T]) List() []T {
	items := s.store.List()
	result := make([]T, len(items))
	for i := range items {
		result[i] = items[i].(T)
	}
	return result
}

func (s *typedStore[T]) ListKeys() []string {
	return s.store.ListKeys()
}

func (s *typedStore[T]) Get(obj T) (item T, exists bool, err error) {
	var key string
	key, err = cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		return
	}
	var itemAny any
	itemAny, exists, err = s.store.GetByKey(key)
	item = itemAny.(T)
	return
}

func (s *typedStore[T]) GetByKey(key string) (item T, exists bool, err error) {
	var itemAny any
	itemAny, exists, err = s.store.GetByKey(key)
	item = itemAny.(T)
	return
}

// NewResource creates a stream of events from a ListerWatcher.
// The initial set of objects is emitted first as UpdateEvents, followed by a SyncEvent, after
// which updates follow. SyncEvent contains a read-only handle onto the underlying store.
//
// Returns an observable for the stream of events and the function to run the resource.
func NewResource[T k8sRuntime.Object](rootCtx context.Context, lw cache.ListerWatcher) (src stream.Observable[Event[T]], run func()) {
	var (
		mu            sync.RWMutex
		subId         int
		queues        = make(map[int]workqueue.RateLimitingInterface)
		exampleObject T
	)

	// Helper to push the key to all subscribed queues.
	push := func(key Key) {
		mu.RLock()
		for _, queue := range queues {
			queue.Add(key)
		}
		mu.RUnlock()
	}

	handlerFuncs :=
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { push(NewKey(obj)) },
			UpdateFunc: func(old interface{}, new interface{}) { push(NewKey(new)) },
			DeleteFunc: func(obj interface{}) { push(NewKey(obj)) },
		}

	store, informer := cache.NewInformer(lw, exampleObject, 0, handlerFuncs)

	run = func() { informer.Run(rootCtx.Done()) }
	src = stream.FuncObservable[Event[T]](
		func(subCtx context.Context, next func(Event[T]) error) error {
			// Subscribe to changes first so they would not be missed.
			mu.Lock()
			subId++
			queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
			queues[subId] = queue
			mu.Unlock()

			// Wait for cache to be synced before emitting the initial set.
			if !cache.WaitForCacheSync(rootCtx.Done(), informer.HasSynced) {
				return errors.New("Resource is shutting down")
			}

			// Emit the initial set of objects followed by the sync event
			initialVersions := make(map[Key]string)
			for _, obj := range store.List() {
				key := NewKey(obj.(T))
				next(&UpdateEvent[T]{key, obj.(T)})
				initialVersions[key] = resourceVersion(obj)
			}
			next(&SyncEvent[T]{&typedStore[T]{store}})

			subCtx, cancel := context.WithCancel(subCtx)
			errs := make(chan error, 1)
			events := make(chan Event[T], 16)

			// Fork a goroutine to wait for updates
			go func() {
				defer close(errs)
				defer close(events)
				for {
					rawKey, shutdown := queue.Get()
					if shutdown {
						break
					}
					queue.Done(rawKey)
					queue.Forget(rawKey)
					key := rawKey.(Key)

					rawObj, exists, err := store.GetByKey(key.String())
					if err != nil {
						errs <- err
						return
					}

					if len(initialVersions) > 0 {
						version := resourceVersion(rawObj)
						if initialVersion, ok := initialVersions[key]; ok {
							// We can now forget the initial version.
							delete(initialVersions, key)
							if initialVersion == version {
								// Already emitted, skip.
								continue
							}
						}
					}

					if exists {
						obj := rawObj.(T)
						events <- &UpdateEvent[T]{key, obj}
					} else {
						events <- &DeleteEvent[T]{key}
					}
				}
				errs <- nil
			}()

			var err error
			done := false
			for !done {
				select {
				case err = <-errs:
					done = true
				case <-rootCtx.Done():
					// Upstream cancelled
					err = rootCtx.Err()
					done = true
				case <-subCtx.Done():
					// Subscriber cancelled
					err = subCtx.Err()
					done = true
				case ev := <-events:
					err = next(ev)
				}
			}

			mu.Lock()
			delete(queues, subId)
			mu.Unlock()

			// Cancel and drain
			go queue.ShutDownWithDrain()
			cancel()
			for range events {
			}

			return err

		})

	return
}

// NewResourceFromListWatch creates a stream of events from the typed client, e.g. (kubernetes.Interface).Pods() etc.
func NewResourceFromListWatch[ObjT k8sRuntime.Object, ListT k8sRuntime.Object](ctx context.Context, lw TypedListerWatcher[ListT]) (src stream.Observable[Event[ObjT]], run func()) {
	return NewResource[ObjT](ctx, listerWatcherAdapter[ListT]{ctx, lw})
}

// NewResourceFromClient creates a stream of events from a k8s REST client for
// the given resource and namespace.
func NewResourceFromClient[T k8sRuntime.Object](
	ctx context.Context,
	resource string,
	namespace string,
	client rest.Interface,
) (src stream.Observable[Event[T]], run func()) {
	lw := cache.NewListWatchFromClient(
		client,
		resource,
		namespace,
		fields.Everything(),
	)
	return NewResource[T](ctx, lw)
}

func resourceVersion(obj any) (version string) {
	if obj != nil {
		meta, err := meta.Accessor(obj)
		if err == nil {
			return meta.GetResourceVersion()
		}
	}
	return ""
}

// TypedListerWatcher is the interface implemented by the generated clients.
type TypedListerWatcher[ListT k8sRuntime.Object] interface {
	List(context.Context, metav1.ListOptions) (ListT, error)
	Watch(context.Context, metav1.ListOptions) (watch.Interface, error)
}

// listerWatcherAdapter implements cache.ListerWatcher in terms of a typed List and Watch methods.
type listerWatcherAdapter[ListT k8sRuntime.Object] struct {
	ctx   context.Context
	typed TypedListerWatcher[ListT]
}

func (tlw listerWatcherAdapter[T]) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return tlw.typed.Watch(tlw.ctx, options)
}

func (tlw listerWatcherAdapter[T]) List(options metav1.ListOptions) (runtime.Object, error) {
	return tlw.typed.List(tlw.ctx, options)
}
