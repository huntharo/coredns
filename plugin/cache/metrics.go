package cache

import (
	"time"
)

// MetricsManager updates expensive count stats on an interval.
type MetricsManager struct {
	interval time.Duration
	server   string
	c        *Cache

	stop chan bool
}

func newMetricsManager(server string, c *Cache) *MetricsManager {
	m := &MetricsManager{
		interval: defaultInterval,
		server:   server,
		c:        c,
		stop:     make(chan bool),
	}
	return m
}

func (m *MetricsManager) metricsManager() {
	ticker := time.NewTicker(m.interval)

	for {
		select {
		case <-ticker.C:
			// Calculating cache size requires reading the size of all shards
			// Reading the size of the shards requires obtaining a ReadLock
			// on each shard.  If called after adding an item that turns every
			// writer into a reader, which serializes each write effectively twice.
			// Additionally, ristretto with ttlEvict enabled can update cache sizes
			// outside of adding items.
			cacheSize.WithLabelValues(m.server, Success).Set(float64(m.c.pcache.Len()))
			cacheSize.WithLabelValues(m.server, Denial).Set(float64(m.c.ncache.Len()))

		case <-m.stop:
			return
		}
	}
}

// Start starts the metrics updater.
func (m *MetricsManager) Start() { go m.metricsManager() }

// Stop stops the metrics updater.
func (m *MetricsManager) Stop() { close(m.stop) }

const (
	defaultInterval = time.Duration(10) * time.Second
)
