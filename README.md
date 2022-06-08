Go Reactive
===========

A reactive streams library for Go in the spirit of Reactive Extensions implemented
with generic functions. The library was born as I wanted a Reactive Extensions library
for Go that is type safe (e.g. no `interface{}`, otherwise I would've used [RxGo](https://github.com/ReactiveX/RxGo))
and could not yet find one.

The library is still in its infancy and thus sparse in terms of documentation and features. The general concepts from
[RxGo](https://github.com/ReactiveX/RxGo) and from the [ReactiveX](http://reactivex.io/) do apply, so if you're
new to reactive streams, you may want to browse these for an introduction to the concept.

The stream package
------------------

The stream package provides the Observable interface and the core functions.

The Observable in this take on reactive streams is defined as:

```go
type Observable[T any] interface {
	Observe(ctx context.Context, next(func T) error) error
}
```

The `next` function is called on each element. To retain good stack traces the `next` function is called
from the goroutine that called `Observe`. A non-nil `error` returned by `next` it will close the stream and
this error is returned by `Observe`.

The call to `Observe` is blocking and returns if:
* `ctx` provided by the observer is closed
* `next` returns an error
* the observable encounters an error

The core functions that operate on `Observable[T]` are divided into:

* [sources](stream/sources.go) that create Observables
* [operators](stream/operators.go) that transform Observables
* [sinks](stream/sinks.go) that terminate the Observable

Since Go's generics does not yet allow new type parameters in methods, all of these
are implemented as top-level functions rather than methods in the Observable interface
(e.g. as it is with RxGo and usual implementations).

Getting started
---------------

As a first example, we'll implement a simple source `Observable` that emits a single integer:

```go

type singleIntegerObservable int

func (num singleIntegerObservable) Observe(ctx context.Context, next func(int) error) error {
	if ctx.Err() != nil {
        	// Context already cancelled, stop before emitting items.
		return ctx.Err()
	}
	return next(int(num))
}
```

We can now try it out with the `Map` operator:

```go
func main() {
	var ten stream.Observable[int] = singleIntegerObservable(10)

	twenty := stream.Map(ten, func(x int) int) { return x * 2 })

	twenty.Observe(context.Background(), func(x int) error {
		fmt.Printf("%d\n", x)
		return nil
	})
}
```

Instead of defining a new type every time we want to implement `Observe`, we can use the `FuncObservable`
wrapper:

```go
func singleInt(x int) stream.Observable[int] {
	return stream.FuncObservable(func(ctx context.Context, next func(int) error) error {
		if ctx.Err() != nil {
			// Context already cancelled, stop before emitting items.
			return ctx.Err()
		}
		return next(x)
	})
}
```

Tour of the operators
---------------------

[Sources](stream/sources.go) provide different ways of creating `Observable`s without
implementing `Observe` by hand:

```go
Single(10)                 // emits 10
Error(errors.New("oh no")) // returns error without emitting items
Empty()                    // returns nil error without emitting items
FromSlice([]int{1,2,3})    // emits 1,2,3 and completes
FromChannel(in)            // emits items from the given channel
Interval(time.Second)      // emits items from sequence 0,1,... once a second
Range(0,3)                 // emits 0,1,2 and completes
```

[Operators](stream/operators.go) transform streams in different ways:
```go
// Map[A, B any](src Observable[A], apply func(A) B) Observable[B]
Map(src, apply)            // applies function 'apply' to each item.

// ParallelMap[A, B any](src Observable[A], par int, apply func(A) B) Observable[B]
ParallelMap(src, 4, apply) // applies function 'apply' to each item using 4 parallel workers.

// FlatMap[A, B any](src Observable[A], apply func(A) Observable[B]) Observable[B]
FlatMap(src, apply)        // applies function 'apply' to each item. 'apply' returns an observable
                           // that is then flattened (Observable[Observable[B]] => Observable[B]).

// Filter[T any](src Observable[T], filter func(T) bool) Observable[T]
Filter(src, filter)        // applies function 'filter' to each item. If 'filter' returns false the
                           // item is dropped.

// Reduce[T, Result any](src Observable[T], init Result, reduce func(T, Result) Result) Observable[Result]
Reduce(src, 0, reduce)     // applies function 'reduce' to each item to "reduce" the stream into a single value.
Reduce(Range(0, 10), 0, func(x, result int) int { return x + result })

// Concat[T any](srcs ...Observable[T]) Observable[T]
Concat(Range(0, 10), Range(10, 20)) == Range(0,20)

// Broadcast[T any](ctx context.Context, bufSize int, src Observable[T]) Observable[T]
// Creates an observable that broadcasts each element from the source to all observers
// of the returned observable.
Broadcast(ctx, 16, FromChannel(events))

// Merge[T any](srcs ...Observable[T]) Observable[T]
Merge(Range(0, 10), Range(10, 20)) // values 0 to 19 in undefined order

// Delay[T any](src Observable[T], interval time.Duration) Observable[T]
Delay(src, time.Second)    // emit at most one value per second

// OnNext[T any](src Observable[T], fn func(T)) Observable[T]
OnNext(src, fn)            // insert the function 'fn' to be called before each item

// Retry[T any](src Observable[T], shouldRetry func(err error) bool) Observable[T]
Retry(src, func(err error) bool { return true }) // always retry if 'src' completes with error

// Take[T any](n int, src Observable[T]) Observable[T]
Take(10, src)              // take 10 items from 'src' and then complete it.

// Buffer[T any](src Observable[T], bufSize int, strategy BackpressureStrategy) Observable[T]
Buffer(src, 16, BackpressureDrop) // buffer up to 16 items from 'src' and drop items if buffer is full

// SplitHead[T any](src Observable[T]) (head Observable[T], tail Observable[T])
SplitHead(Range(0,10)) => (Single(0), Range(1,10))
```

[Sinks](stream/sinks.go) consume streams:
```go
// First[T any](ctx context.Context, src Observable[T]) (item T, err error)
// Takes the first item from the observable and then cancels it.
item, err := First(ctx, src)

// ToSlice[T any](ctx context.Context, src Observable[T]) (items []T, err error)
// Converts the observable into a slice.
items, err := ToSlice(ctx, src)

// ToChannels[T any](ctx context.Context, src Observable[T]) (<-chan T, <-chan error)
// Converts the observable into items channel and errors channel.
items, errs := ToChannels(ctx, src)

// Discard[T any](ctx context.Context, src Observable[T]) error
err := Discard(ctx, src)
```

Additional sources
------------------

Included in this repository are also some additional sources:
* [sources/http](sources/http/http.go) - A wrapper for the Go net/http client
* [sources/k8s](sources/k8s/k8s.go) - A wrapper for a Kubernetes informer
