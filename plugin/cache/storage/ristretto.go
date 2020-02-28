//
// External Cache Library
// https://blog.dgraph.io/post/introducing-ristretto-high-perf-go-cache/
// Reasons to use external cache library:
//  1. Internal cache serializes requests with writes within a shard
//  2. Internal cache makes the request wait while writing to a shard
//  3. Internal cache has no admission criteria - Any non-cached request
//     will force an existing, potentially highly used, item out of cache
//  4. Internal cache does not attempt to drop in LRU fashion
//     (eviction is random when cache is full)
//  5. The above problems may be what is leading to OOM conditions
//     under heavy load when many cache writes are happening
//

package storage

import (
	"hash/fnv"
	"sync/atomic"

	"github.com/dgraph-io/ristretto"
)

type storageRistretto struct {
	// Note: the int64 for atomic must be 8 byte aligned
	// We achieve that by allocating it with new(int64)
	// https://go101.org/article/concurrent-atomic-operation.html
	// "Please note, up to now (Go 1.14), atomic operations for 64-bit words,
	// a.k.a., int64 and uint64 values, require the 64-bit words must be 8-byte
	// aligned in memory."
	approxLength *int64
	cache        *ristretto.Cache
}

// NewStorageRistretto creates a new Ristretto cache
func NewStorageRistretto(size int) Storage {
	storage := new(storageRistretto)

	storage.approxLength = new(int64)

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(10 * size),           // suggestion is 10x max stored items
		MaxCost:     int64(size),                // maximum cost - using cost 1 per item
		BufferItems: 64,                         // number of keys per Get buffer
		Metrics:     true,                       // enable metrics as they are needed for tests
		KeyToHash:   storage.ristrettoKeyToHash, // replace default hash with FNV since it's faster
		//Cost:        storage.ristrettoCost,      // track when items are stored
		//OnEvict:     storage.ristrettoOnEvict,   // track when items are evicted
	})

	if err != nil {
		return nil
	}

	storage.cache = cache

	return storage
}

// Override default hash with FNV as it is faster
func (s storageRistretto) ristrettoKeyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case *StorageHash:
		return k.uhash, 0
	default:
		panic("Key type not supported")
	}
	return 0, 0
}

// Decrement storage cost by 1
// Called only on evict
func (s storageRistretto) ristrettoOnEvict(key, conflict uint64, value interface{}, cost int64) {
	atomic.AddInt64(s.approxLength, -1)
}

// Increment storage cost by 1
// Called only on set / store
func (s storageRistretto) ristrettoCost(value interface{}) int64 {
	atomic.AddInt64(s.approxLength, 1)
	return 1
}

// Hash key parameters using FNV to uint64
func (s storageRistretto) Hash(qname string, qtype uint16, do bool) *StorageHash {
	h := fnv.New64()

	if do {
		h.Write(one)
	} else {
		h.Write(zero)
	}

	h.Write([]byte{byte(qtype >> 8)})
	h.Write([]byte{byte(qtype)})
	h.Write([]byte(qname))

	storageHash := new(StorageHash)
	storageHash.qname = qname
	storageHash.qtype = qtype
	storageHash.do = do
	storageHash.uhash = h.Sum64()

	return storageHash
}

// Add an item to the cache
func (s storageRistretto) Add(key *StorageHash, el interface{}) {
	s.cache.Set(key, el, 1)
}

// Attempt to get an item from the cache
func (s storageRistretto) Get(key *StorageHash) (interface{}, bool) {
	return s.cache.Get(key)
}

// Retrieve the current cache storage usage
// Note: this will be a lagging indicator
// It will only update when items are admitted into the cache
func (s storageRistretto) Len() int {
	return int(s.cache.Metrics.CostAdded() - s.cache.Metrics.CostEvicted())
	//return int(atomic.LoadInt64(s.approxLength))
}

// Remove an item from the cache
func (s storageRistretto) Remove(key *StorageHash) {
	s.cache.Del(key)
	//atomic.AddInt64(s.approxLength, -1)
}

func (s storageRistretto) Stop() {
	s.cache.Close()
}
