package multicluster

import (
	"context"
	clustergatewayv1alpha1 "github.com/oam-dev/cluster-gateway/pkg/apis/cluster/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type watchClient struct {
	client.Client
	base    client.WithWatch
	gateway client.WithWatch
}

func NewWatchClient(cfg *rest.Config, options ClientOptions) (client.WithWatch, error) {
	wrapped := rest.CopyConfig(cfg)
	wrapped.Wrap(NewTransportWrapper())
	constructor := client.NewWithWatch

	if options.DisableRemoteClusterClient {
		return constructor(wrapped, options.Options)
	}

	if len(options.ClusterGateway.URL) == 0 {
		return constructor(wrapped, options.Options)
	}

	base, err := constructor(cfg, options.Options)
	if err != nil {
		return nil, err
	}

	wrapped.Host = options.ClusterGateway.URL
	if len(options.ClusterGateway.CAFile) > 0 {
		if wrapped.CAData, err = os.ReadFile(options.ClusterGateway.CAFile); err != nil {
			return nil, err
		}
	} else {
		wrapped.CAData = nil
		wrapped.Insecure = true
	}

	if options.Options.Scheme != nil {
		// no err will be returned here
		_ = clustergatewayv1alpha1.AddToScheme(options.Options.Scheme)
	}

	gateway, err := constructor(wrapped, options.Options)
	if err != nil {
		return nil, err
	}

	return &watchClient{
		base:    base,
		gateway: gateway,
	}, nil
}

func (client *watchClient) Watch(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	cluster, _ := ClusterFrom(ctx)
	_, ok := obj.(*unstructured.Unstructured)

	if IsLocal(cluster) || !ok {
		return client.base.Watch(ctx, obj, opts...)
	}

	return client.gateway.Watch(ctx, obj, opts...)
}
