package bard

import (
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

type Store struct {
	bolt      *bolt.DB
	raft      *raft.Raft
	numShards int64
	sharding  bool
}

func (s *Store) Ready() bool {
	return s.bolt != nil && s.numShards != 0 && !s.sharding && s.raft != nil
}
