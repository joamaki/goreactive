// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"strings"

	"github.com/kr/pretty"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/joamaki/goreactive/sources/k8s"
	. "github.com/joamaki/goreactive/stream"
)

var (
	apiServerURL   string
	kubeConfigPath string
)

func init() {
	flag.StringVar(&apiServerURL, "server-url", "", "Kubernetes API server URL")
	var defaultKubeConfigPath string
	if homeDir, err := os.UserHomeDir(); err == nil {
		defaultKubeConfigPath = path.Join(homeDir, ".kube", "config")
	}
	flag.StringVar(&kubeConfigPath, "kubeconfig", defaultKubeConfigPath, "Path to kubeconfig")
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel the context on interrupt (ctrl-c)
	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		log.Printf("Interrupted, stopping...")
		cancel()
	}()

	client, err := newK8sClient(apiServerURL, kubeConfigPath)
	if err != nil {
		log.Fatalf("Failed to create k8s client: %s", err)
	}

	pods :=  k8s.NewResourceFromListWatch[*v1.Pod, *v1.PodList](
			ctx,
			client.CoreV1().Pods("default"))

	services :=
		k8s.NewResourceFromListWatch[*v1.Service, *v1.ServiceList](
			ctx,
			client.CoreV1().Services("default"))

	endpoints :=
		k8s.NewResourceFromListWatch[*v1.Endpoints, *v1.EndpointsList](
			ctx,
			client.CoreV1().Endpoints("default"))

	podDiffer := newDiffer[*v1.Pod]()
	serviceDiffer := newDiffer[*v1.Service]()
	endpointsDiffer := newDiffer[*v1.Endpoints]()

	// Combine everything into a stream of update messages.
	updates :=
		Merge(
			Just("Waiting for updates...\n"),

			Map(pods,
				func(ev k8s.Event[*v1.Pod]) string {
					var out string
					ev.Dispatch(
						func(){
							out = "pods synced"
						},
						func(key k8s.Key, pod *v1.Pod) {
							out = fmt.Sprintf("pod %s updated:\n%s\n", key, podDiffer.diff(key, pod))
						},
						func(key k8s.Key) {
							out = fmt.Sprintf("pod %s deleted", key)
						})
					return out
				}),
			Map(services,
				func(ev k8s.Event[*v1.Service]) string {
					var out string
					ev.Dispatch(
						func(){
							out = "services synced"
						},
						func(key k8s.Key, service *v1.Service) {
							out = fmt.Sprintf("service %s updated:\n%s\n", key, serviceDiffer.diff(key, service))
						},
						func(key k8s.Key) {
							out = fmt.Sprintf("service %s deleted", key)
						})
					return out
				}),
			Map(endpoints,
				func(ev k8s.Event[*v1.Endpoints]) string {
					var out string
					ev.Dispatch(
						func(){
							out = "endpoints synced"
						},
						func(key k8s.Key, endpoints *v1.Endpoints) {
							out = fmt.Sprintf("endpoints %s updated:\n%s\n", key, endpointsDiffer.diff(key, endpoints))
						},
						func(key k8s.Key) {
							out = fmt.Sprintf("endpoints %s deleted", key)
						})
					return out
				}),
		)

	// Only display 3 updates per second
	updates = Throttle(updates, 3.0, 3)

	err = updates.Observe(ctx,
		func(desc string) error {
			fmt.Println(desc)
			return nil
		},
	)
	if err != nil && ctx.Err() == nil {
		log.Fatalf("error: %s", err)
	}
}

type differ[T any] struct {
	previous map[k8s.Key]T
}

func newDiffer[T any]() differ[T] {
	return differ[T]{make(map[k8s.Key]T)}
}

func (d differ[T]) diff(key k8s.Key, obj T) string {
	changeDesc := ""
	if prev, ok := d.previous[key]; ok {
		changes := pretty.Diff(prev, obj)
		changeDesc = strings.Join(changes, "\n")
	} else {
		// TODO: pretty printing is faulty:
		// (PANIC=Format method: runtime error: invalid memory address or nil pointer dereference)
		//changeDesc = pretty.Sprint(obj)
		changeDesc = fmt.Sprintf("%#v", obj)
	}
	d.previous[key] = obj
	return changeDesc

}

func newK8sClient(url, kubeconfig string) (kubernetes.Interface, error) {
	config, err := clientcmd.BuildConfigFromFlags(url, kubeconfig)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}
