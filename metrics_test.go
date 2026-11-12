package ckit

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestMetrics_ClusterName checks that every metric of a node has the
// cluster_name label. Alerts join the metrics on this label.
func TestMetrics_ClusterName(t *testing.T) {
	m := newMetrics("test-cluster")
	m.nodeInfo.MustSet("state", "participant")

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(m))

	families, err := reg.Gather()
	require.NoError(t, err)

	var foundInfo bool
	for _, family := range families {
		if family.GetName() == "cluster_node_info" {
			foundInfo = true
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, lp := range metric.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			require.Equal(t, "test-cluster", labels["cluster_name"], "metric %s", family.GetName())
		}
	}
	require.True(t, foundInfo, "cluster_node_info was not collected")
}
