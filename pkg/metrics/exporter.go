package metrics

import (
	"net/http"

	"contrib.go.opencensus.io/exporter/prometheus"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opencensus.io/stats/view"
)

func NewExporter(views ...*view.View) http.Handler {
	if err := view.Register(DefaultViews...); err != nil {
		logger.Errorf("cannot register default metric views: %s", err)
	}

	if err := view.Register(views...); err != nil {
		logger.Errorf("cannot register metric views: %s", err)
	}

	registry, ok := promclient.DefaultRegisterer.(*promclient.Registry)
	if !ok {
		logger.Warnf("failed to export default prometheus registry; some metrics will be unavailable; unexpected type: %T", promclient.DefaultRegisterer)
	}

	_ = registry.Register(collectors.NewGoCollector())
	_ = registry.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	exp, err := prometheus.NewExporter(prometheus.Options{
		Registry:  registry,
		Namespace: "autoretrieve",
	})
	if err != nil {
		logger.Errorf("cannot create the prometheus stats exporter: %v", err)
	}

	return exp
}
