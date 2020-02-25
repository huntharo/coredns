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
	"fmt"

	"github.com/dgraph-io/ristretto"
)

type storageRistretto struct {
	cache *ristretto.Cache
}

// NewStorageRistretto creates a new Ristretto cache
func NewStorageRistretto(size int) Storage {
	storage := new(storageRistretto)

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(10 * size), // suggestion is 10x max stored items
		MaxCost:     int64(size),      // maximum cost - using cost 1 per item
		BufferItems: 64,               // number of keys per Get buffer
	})

	if err != nil {
		return nil
	}

	storage.cache = cache

	return storage
}

// Construct a hash string (do not actually hash)
func (s storageRistretto) Hash(qname string, qtype uint16, do bool) *StorageHash {
	doC := ""
	if do {
		doC = "1"
	} else {
		doC = "0"
	}

	storageHash := new(StorageHash)
	storageHash.qname = qname
	storageHash.qtype = qtype
	storageHash.do = do
	storageHash.uhash = 0
	storageHash.strhash = fmt.Sprintf("%s-%d-%d", qname, qtype, doC)
	return storageHash
}

// Add an item to the cache
func (s storageRistretto) Add(key *StorageHash, el interface{}) {
	s.cache.Set(key.strhash, el, 1)
}

// Attempt to get an item from the cache
func (s storageRistretto) Get(key *StorageHash) (interface{}, bool) {
	return s.cache.Get(key.strhash)
}

// Retrieve the current cache storage usage
func (s storageRistretto) Len() int {
	return 0
}

// Remove an item from the cache
func (s storageRistretto) Remove(key *StorageHash) {
	s.cache.Del(key.strhash)
}
