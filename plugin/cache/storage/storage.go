package storage

// Storage is an interface that provides normalized access to many cache implementations
type Storage interface {
	Hash(qname string, qtype uint16, do bool) *StorageHash
	Add(key *StorageHash, el interface{})
	Get(key *StorageHash) (interface{}, bool)
	Len() int
	Remove(key *StorageHash)
}

var one = []byte("1")
var zero = []byte("0")

// StorageHash contains the inputs to the hash and the string or uint64 hash
// This allows different cache implementations to have different cache implementations
// It will also allow a cache to store the input parameters for detection of collisions on reads
type StorageHash struct {
	qname string
	qtype uint16
	do    bool
	uhash uint64
}
