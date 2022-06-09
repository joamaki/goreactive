---
title: Going reactive with Go Generics
theme: solarized
highlightTheme: a11y-light
revealOptions:
  width: 1280
  transition: 'none'
---

## Going reactive with Go Generics

### [github.com/joamaki/goreactive](https://github.com/joamaki/goreactive)
### 2022-06-08

---

## Outline of the talk

- Reactive programming?
- Examples
- Demo
- Tour of the library
- Q&A

---

### Reactive programming?

[Wikipedia](https://en.wikipedia.org/wiki/Reactive_programming):
> In computing, reactive programming is a
> declarative programming paradigm concerned
> with data streams and the propagation of change.

---

### Reactive programming?

- Reactive programming (to me) is a modular way of composing systems out
  of event streams.
- Think unix pipes
- A reactive library provides the language for composing and transforming streams

---

### Reactive Extensions (ReactiveX)

- API for reactive programming originally from Microsoft (2011)
- Official implementations for many languages, including Go (RxGo)
- My library roughly follows the API

---

### Reactive Extensions (ReactiveX)

- Building block: `Observable`.
- Emits events: next, error or complete.
- Operators for creating, transforming and composing `Observables`

https://reactivex.io/intro.html

---

### Example in ReactiveX (JS)

```js
getDataFromNetwork()
  .skip(10)
  .take(5)
  .map({ s -> return s + " transformed" })
  .subscribe({ println "onNext => " + it })
```

---

### Unix pipes?

```shell
curl https://go.dev | \
  tail -n +10       | \
  head -n 5         | \
  awk '{ print $1 " transformed" }'
```

---

### Example in goreactive

```go
import "github.com/joamaki/goreactive/stream"
var src stream.Observable[string]
src = GetDataFromNetwork()
src = stream.Take(5, stream.Skip(10, src))
src = stream.Map(
    src,
    func(s string) string {
        return s + " transformed"
    })
src.Observe(
    ctx,
    func(next string) error {
        fmt.Printf("next => %s", next)
        return nil
    })
```

---

### Observable

```go
type Observable[T any] interface {
	Observe(ctx context.Context, next func(T) error) error
}
```

- `Observe`: observe a stream of values
- Blocks until `ctx` cancelled, source fails, or `next` returns an error.

---

### Observable

```go
type Observable[T any] interface {
	Observe(ctx context.Context, next func(T) error) error
}
```

- `Observable` is usually lazy.
- calling `Observe` puts the machinery in motion.

---

### A trivial Observable

```go
type JustInt int

func (num JustInt) Observe(ctx context.Context, next func(int) error) error {
        return next(int(num))
}
```

---

### A trivial Observable

```go
func main() {
    var ten stream.Observable[int] = JustInt(10)

    stream.Map(
      ten,
      func(x int) int { return x * 2 },
    ).Observe(
        context.Background(),
        func(x int) error {
            fmt.Printf("x: %d\n", x)
            return nil
        },
    )
}
```

---

### A trivial Observable

Actual implementations:

```go
type FuncObservable[T any] func(context.Context, func(T) error) error
func (f FuncObservable[T]) Observe(ctx context.Context, next func(T) error) error {
	return f(ctx, next)
}

func Just[T any](item T) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T) error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return next(item)
		})
}
```

---

### Sources

```go
// func <<source>>[T any](...) Observable[T]

Just(10)                 // emits 10
Error(errors.New("oh no")) // errors without emitting
Empty()                    // completes immediately
FromSlice([]int{1,2,3})    // emits 1,2,3 and completes
FromChannel(in)            // emits items from a channel
Interval(time.Second)      // emits increasing int once a sec
Range(0,3)                 // emits 0,1,2 and completes
```

---

### Operators: Map

```go
// Map[A, B any](
//     src Observable[A],
//     apply func(A) B,
// ) Observable[B]

Map(Range(0, 3), double) == FromSlice([]int{0,2,4})
```

---

### Map implementation

```go
func Map[A, B any](src Observable[A], apply func(A) B) Observable[B] {
    return FuncObservable[B](
        func(ctx context.Context, next func(B) error) error {
            return src.Observe(
              ctx,
              func(a A) error { return next(apply(a)) },
            )
        })
}
```

---

### Operators: Filter

```go
// Filter[T any](
//     src Observable[T],
//     filter func(T) bool,
// ) Observable[T]

func isOdd(n int) bool { return n % 2 != 0 }
Filter(Range(0, 5), isOdd) == FromSlice([]int{1,3})
```

---

### Operators: Reduce

```go
// Reduce[T, Result any](
//     src Observable[T],
//     init Result,
//     reduce func(T, Result) Result,
// ) Observable[Result]

func sum(result, x int) int { return result + x }
Reduce(Range(0, 5), 0, sum) == Single(0+0+1+2+3+4)
```

---

### Operators: Concat & Merge

```go
// Concat[T any](srcs ...Observable[T]) Observable[T]
Concat(Range(0, 10), Range(10, 20)) == Range(0,20)

// Merge[T any](srcs ...Observable[T]) Observable[T]
Merge(Range(0, 10), Range(10, 20))  == [0..20] in some order
```

---

### Operators: Throttle

```go
// Throttle[T any](
//     src Observable[T],
//     ratePerSecond float64,
//     burst int,
// ) Observable[T]

Throttle(src, 10.0, 5)
  => items from 'src' emitted at most 10 per second
```
---

### Operators: Throttle (implementation)

```go
import "golang.org/x/time/rate"
func Throttle[T any](src Observable[T], ratePerSecond float64, burst int) Observable[T] {
    return FuncObservable[T](
        func(ctx context.Context, next func(T) error) error {
            limiter := rate.NewLimiter(rate.Limit(ratePerSecond), burst)
            return src.Observe(
                ctx,
                func(item T) error {
                    if err := limiter.Wait(ctx); err != nil {
                        return err
                    }
                    return next(item)
               })
        })
}
```

---

### Operators: Buffer

```go
// Buffer[T any](
//     src Observable[T],
//     bufSize int,
//     strategy BackpressureStrategy,
// ) Observable[T]

Buffer(src, 16, BackpressureDrop)
  => buffer up to 16 items and then start dropping
```

---

### Sinks: ToSlice

```go
// ToSlice[T any](
//     ctx context.Context,
//     src Observable[T],
// ) (items []T, err error)

items, err := ToSlice(ctx, src)
```

---

### Sinks: ToChannels

```go
// ToChannels[T any](
//     ctx context.Context,
//     src Observable[T],
// ) (<-chan T, <-chan error)

items, errs := ToChannels(ctx, src)
for {
  select {
  case item := <-items:
    // ...
  case err := <-errs:
    return err
}
```

---

## Demo

---

### Go's generics

- Worked without issues
- Syntax fits well into the language and easy to pick up
- No method-level polymorphism made this slightly clunky

---

## Q&A

Thank you!

---
