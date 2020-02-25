package storage

import (
	"hash/fnv"

	"github.com/coredns/coredns/plugin/pkg/cache"
)

type storageInternal struct {
	cache *cache.Cache
}

// Create a new cache using the internal cache package
func NewStorageInternal(size int) Storage {
	storage := new(storageInternal)
	storage.cache = cache.New(size)
	return storage
}

var one = []byte("1")
var zero = []byte("0")

// Hash key parameters using FNV to uint64
func (s storageInternal) Hash(qname string, qtype uint16, do bool) *StorageHash {
	h := fnv.New64()

	if do {
		h.Write(one)
	} else {
		h.Write(zero)
	}

	h.Write([]byte{byte(qtype >> 8)})
	h.Write([]byte{byte(qtype)})
	h.Write([]byte(qname))
	hash64 := h.Sum64()

	storageHash := new(StorageHash)
	storageHash.qname = qname
	storageHash.qtype = qtype
	storageHash.do = do
	storageHash.uhash = hash64

	return storageHash
}

// Add an item to the cache
func (s storageInternal) Add(key *StorageHash, el interface{}) {
	s.cache.Add(key.uhash, el)
}

// Attempt to get an item from the cache
func (s storageInternal) Get(key *StorageHash) (interface{}, bool) {
	return s.cache.Get(key.uhash)
}

// Retrieve the current cache storage usage
func (s storageInternal) Len() int {
	return s.cache.Len()
}

// Remove an item from the cache
func (s storageInternal) Remove(key *StorageHash) {
	s.cache.Remove(key.uhash)
}
