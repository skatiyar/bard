package bard

import "github.com/boltdb/bolt"

type Store struct {
	bolt       *bolt.DB
	numShards  int64
	reSharding bool
}

func (s *Store) Ready() bool {
	return s.bolt != nil && s.numShards != 0 && !s.reSharding
}
