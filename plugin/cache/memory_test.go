package cache

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/cache/storage"
	"github.com/coredns/coredns/plugin/test"

	"github.com/miekg/dns"
)

// testParallelInserts is testing memory usage of inserts
func testParallelInserts(t *testing.T, c *Cache) {
	c.Next = BackendHandler(0)

	// Get basline memory usage
	// Note: This is *after* the empty cache has been created
	printMemUsage()

	ctx := context.TODO()

	loopsPerRoutine := 1000000

	parallelRoutines := runtime.NumCPU() - 2
	r := make([]chan int, parallelRoutines)
	for p := 0; p < parallelRoutines; p++ {
		r[p] = make(chan int)
		// Startup the goroutines in parallel
		go func(p int) {
			defer close(r[p])

			// Create a new random number generator for this routine
			source := rand.NewSource(time.Now().UnixNano() * int64(p))
			generator := rand.New(source)

			for i := 0; i < loopsPerRoutine; i++ {
				req := new(dns.Msg)
				req.SetQuestion(fmt.Sprintf("%d.example.org.", generator.Intn(math.MaxInt64)), dns.TypeA)
				c.ServeDNS(ctx, &test.ResponseWriter{}, req)
			}
			// Just write the parallel number (we don't care)
			r[p] <- p
		}(p)
	}

	// Wait for all the goroutines to stop
	for p := 0; p < parallelRoutines; p++ {
		_ = <-r[p]
	}

	// Stop the cache goroutines
	c.pcache.Stop()
	c.ncache.Stop()
	c.pcache = nil
	c.ncache = nil

	// Recalculate memory stats
	printMemUsage()

	// Force a GC
	runtime.GC()

	// Force a GC
	runtime.GC()

	// Recalculate memory stats
	printMemUsage()
}

func TestBuiltinParallelInserts(t *testing.T) {
	c := newTestCacheOnly("", 10000)
	testParallelInserts(t, c)
}

func TestRistrettoParallelInserts(t *testing.T) {
	c := newTestCacheOnly("ristretto", 10000)
	testParallelInserts(t, c)
}

func BackendHandler(delayMS int) plugin.Handler {
	return plugin.HandlerFunc(func(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
		m := new(dns.Msg)
		m.SetReply(r)
		m.Response = true
		m.RecursionAvailable = true

		owner := m.Question[0].Name
		m.Answer = []dns.RR{test.A(owner + " 303 IN A 127.0.0.53")}

		if strings.HasSuffix(owner, ".slow.") {
			if delayMS != 0 {
				// Only slow ".slow." when delay is enabled at all
				time.Sleep(time.Duration(delayMS) * time.Millisecond)
			}
		}

		w.WriteMsg(m)
		return dns.RcodeSuccess, nil
	})
}

func newTestCacheOnly(cacheType string, cap int) *Cache {
	c := New()
	c.pcap = cap
	c.ncap = cap

	// Overwrite the default buitin cache objects
	if cacheType == "ristretto" {
		c.ristretto = true
		c.pcache = storage.NewStorageRistretto(c.pcap, false)
		c.ncache = storage.NewStorageRistretto(c.ncap, false)
	}

	return c
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
