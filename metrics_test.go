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

	// Vectors without children are not gathered. Add one child to each.
	m.gossipEventsTotal.WithLabelValues(eventNodeJoin).Inc()
	m.gossipBroadcastsTotal.WithLabelValues(eventNodeJoin).Inc()
	m.nodePeers.WithLabelValues("participant").Set(1)

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(m))

	families, err := reg.Gather()
	require.NoError(t, err)

	gathered := map[string]bool{}
	for _, family := range families {
		gathered[family.GetName()] = true
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, lp := range metric.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			require.Equal(t, "test-cluster", labels["cluster_name"], "metric %s", family.GetName())
		}
	}

	for _, name := range []string{
		"cluster_node_gossip_received_events_total",
		"cluster_node_gossip_broadcasts_total",
		"cluster_node_peers",
		"cluster_node_updating",
		"cluster_node_update_duration_seconds",
		"cluster_node_update_observers",
		"cluster_node_info",
	} {
		require.True(t, gathered[name], "metric %s was not gathered", name)
	}
}
