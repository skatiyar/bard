package bard

import "errors"

var (
	ErrInvalidShardNumber = errors.New("Invalid number of shards")
	ErrDB                 = errors.New("DB not open")
	ErrShardNumber        = errors.New("Unable to get shard number")
)
