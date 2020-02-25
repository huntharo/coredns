package storage

type Storage interface {
	Hash(qname string, qtype uint16, do bool) *StorageHash
	Add(key *StorageHash, el interface{})
	Get(key *StorageHash) (interface{}, bool)
	Len() int
	Remove(key *StorageHash)
}

type StorageHash struct {
	qname   string
	qtype   uint16
	do      bool
	uhash   uint64
	strhash string
}
