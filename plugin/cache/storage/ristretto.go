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
	"time"

	"github.com/dgraph-io/ristretto"
)

type storageRistretto struct {
	cache       *ristretto.Cache
	ttlEviction bool
}

// NewStorageRistretto creates a new Ristretto cache
func NewStorageRistretto(size int, ttlEviction bool) Storage {
	storage := new(storageRistretto)

	storage.ttlEviction = ttlEviction

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(10 * size),           // suggestion is 10x max stored items
		MaxCost:     int64(size),                // maximum cost - using cost 1 per item
		BufferItems: 64,                         // number of keys per Get buffer
		Metrics:     true,                       // enable metrics as they are needed for tests
		KeyToHash:   storage.ristrettoKeyToHash, // replace default hash with FNV since it's faster
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
func (s storageRistretto) Add(key *StorageHash, el interface{}, ttl time.Duration) {
	if s.ttlEviction && ttl > 0 {
		// Add with ttl specified by caller
		s.cache.SetWithTTL(key, el, 1, ttl)
	} else {
		// Add without ttl (never evict on ttl)
		s.cache.Set(key, el, 1)
	}
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
}

// Remove an item from the cache
func (s storageRistretto) Remove(key *StorageHash) {
	s.cache.Del(key)
}

func (s storageRistretto) Stop() {
	s.cache.Close()
}
