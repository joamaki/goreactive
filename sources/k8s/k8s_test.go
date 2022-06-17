// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/joamaki/goreactive/stream"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sRuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
)

type typedListerWatcher[T k8sRuntime.Object] struct {
	ctx context.Context
	watch func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error)
	list func(ctx context.Context, options metav1.ListOptions) (T, error)
}

func (tlw typedListerWatcher[T])  Watch(options metav1.ListOptions) (watch.Interface, error) {
	return tlw.watch(tlw.ctx, options)
}

func (tlw typedListerWatcher[T]) List(options metav1.ListOptions) (runtime.Object, error) {
	return tlw.list(tlw.ctx, options)
}

var node = &corev1.Node{
	ObjectMeta: metav1.ObjectMeta{
		Name:                       "hello",
		Namespace:                  "world",
	},
	Status:     corev1.NodeStatus{
		Phase:           "funk",
	},
}

func TestK8sResourceWithFakeClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cs := fake.NewSimpleClientset(node)

	nodes := cs.CoreV1().Nodes()
	lw := typedListerWatcher[*corev1.NodeList]{watch: nodes.Watch, list: nodes.List}
	n, err := stream.First(ctx, stream.Flatten(Resource[*corev1.Node](ctx, lw)))

	if err != nil {
		t.Fatalf("expected nil error, got %s", err)
	}

	if n.Object.GetName() != "hello" {
		t.Fatalf("unexpected node: %#v", node)
	}
}
