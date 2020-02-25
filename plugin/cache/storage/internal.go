package storage

import (
	"hash/fnv"

	"github.com/coredns/coredns/plugin/pkg/cache"
)

type StorageInternal struct {
	cache *cache.Cache
}

func NewStorageInternal(size int) Storage {
	storage := new(StorageInternal)
	storage.cache = cache.New(size)
	return storage
}

var one = []byte("1")
var zero = []byte("0")

func (s StorageInternal) Hash(qname string, qtype uint16, do bool) *StorageHash {

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

func (s StorageInternal) Add(key *StorageHash, el interface{}) {
	s.cache.Add(key.uhash, el)
}

func (s StorageInternal) Get(key *StorageHash) (interface{}, bool) {
	return s.cache.Get(key.uhash)
}

func (s StorageInternal) Len() int {
	return s.cache.Len()
}

func (s StorageInternal) Remove(key *StorageHash) {
	s.cache.Remove(key.uhash)
}
