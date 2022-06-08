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
	"k8s.io/client-go/rest"
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

	client, err := newK8sRESTClient(apiServerURL, kubeConfigPath)
	if err != nil {
		log.Fatalf("Failed to create k8s client: %s", err)
	}

	pods := Flatten(
		// Resource() returns []T with the first one being the initially
		// synced object set, but we don't care about the distinctions here
		// so we flatten it.
		k8s.Resource[*v1.Pod](
			ctx,
			"pods",
			"default",
			client))

	services := Flatten(
		k8s.Resource[*v1.Service](
			ctx,
			"services",
			"default",
			client))

	endpoints := Flatten(
		k8s.Resource[*v1.Endpoints](
			ctx,
			"endpoints",
			"default",
			client))

	podDiffer := newDiffer[*v1.Pod]()
	serviceDiffer := newDiffer[*v1.Service]()
	endpointsDiffer := newDiffer[*v1.Endpoints]()

	// Combine everything into a stream of update messages.
	updates :=
		Merge(
			Single("Waiting for updates...\n"),

			Map(pods,
				func(item k8s.Item[*v1.Pod]) string {
					if item.Object == nil {
						return fmt.Sprintf("pod %s deleted", item.Key)
					}
					return fmt.Sprintf("pod %s updated:\n%s\n", item.Key, podDiffer.diff(item.Key, item.Object))
				}),
			Map(services,
				func(item k8s.Item[*v1.Service]) string {
					if item.Object == nil {
						return fmt.Sprintf("service %s deleted", item.Key)
					}
					return fmt.Sprintf("service %s updated:\n%s\n", item.Key, serviceDiffer.diff(item.Key, item.Object))
				}),
			Map(endpoints,
				func(item k8s.Item[*v1.Endpoints]) string {
					if item.Object == nil {
						return fmt.Sprintf("endpoints %s deleted", item.Key)
					}
					return fmt.Sprintf("endpoints %s updated:\n%s\n", item.Key, endpointsDiffer.diff(item.Key, item.Object))
				}),
		)

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
	previous map[string]T
}

func newDiffer[T any]() differ[T] {
	return differ[T]{make(map[string]T)}
}

func (d differ[T]) diff(key string, obj T) string {
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

func newK8sRESTClient(url, kubeconfig string) (rest.Interface, error) {
	config, err := clientcmd.BuildConfigFromFlags(url, kubeconfig)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset.CoreV1().RESTClient(), nil
}
