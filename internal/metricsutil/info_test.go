package metricsutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestInfoCollector(t *testing.T) {
	ic := NewInfoCollector(InfoOpts{
		Name:        "test_info",
		Help:        "Test info metric.",
		ConstLabels: prometheus.Labels{"cluster_name": "test-cluster"},
	}, "state")
	ic.MustSet("state", "participant")

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(ic))

	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Equal(t, "test_info", families[0].GetName())
	require.Len(t, families[0].GetMetric(), 1)

	labels := map[string]string{}
	for _, lp := range families[0].GetMetric()[0].GetLabel() {
		labels[lp.GetName()] = lp.GetValue()
	}
	require.Equal(t, map[string]string{
		"cluster_name": "test-cluster",
		"state":        "participant",
	}, labels)
	require.Equal(t, 1.0, families[0].GetMetric()[0].GetGauge().GetValue())
}

func TestInfoCollector_UnknownLabel(t *testing.T) {
	ic := NewInfoCollector(InfoOpts{Name: "test_info", Help: "Test info metric."}, "state")
	require.Error(t, ic.Set("other", "value"))
}
