package bard

import (
	"encoding/binary"
	"github.com/boltdb/bolt"
	"time"
)

const (
	BardConfigBucket = "bard-config-bucket"
	KeyShardNumber   = "number-of-shards"
)

var (
	db = &Store{}
)

func Open(path, port string, shards int64) error {
	if shards < 1 {
		return ErrInvalidShardNumber
	}

	boltDB, boltDBErr := bolt.Open(path, 0644, nil)
	if boltDBErr != nil {
		return boltDBErr
	}

	iRaft, iRaftErr := newRaft(port, path)
	if iRaftErr != nil {
		return iRaftErr
	}

	db.raft = iRaft
	db.bolt = boltDB
	db.numShards = shards
	db.sharding = true

	if configErr := checkExistingShards(shards); configErr != nil {
		return configErr
	}

	db.sharding = false

	/*
		return db.bolt.View(func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				log.Println("-- name ->", string(name))
				i := 0
				b.ForEach(func(key, value []byte) error {
					i++
					return nil
				})
				log.Println("-- -- pair ->", i)
				return nil
			})
		})
	*/

	return nil
}

func Close() error {
	if !db.Ready() {
		return ErrDB
	}

	// TODO shutdown raft
	return db.bolt.Close()
}

func checkExistingShards(shards int64) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		var i int64
		for i = 0; i < shards; i++ {
			buffer := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
			bytesRead := binary.PutVarint(buffer, i)
			if bytesRead <= 0 {
				return ErrShardNumber
			}

			_, nextBucketErr := tx.CreateBucketIfNotExists(buffer)
			if nextBucketErr != nil {
				return nextBucketErr
			}
		}

		return tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			bucketIndex, bucketIndexErr := binary.Varint(name)
			if bucketIndexErr < 0 {
				return nil
			}

			return bucket.ForEach(func(key, val []byte) error {
				if newShard := jch(keyToHash(key), shards); newShard != bucketIndex {
					buffer := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
					bytesRead := binary.PutVarint(buffer, newShard)
					if bytesRead <= 0 {
						return ErrShardNumber
					}

					nextBucket, nextBucketErr := bucket.Tx().CreateBucketIfNotExists(buffer)
					if nextBucketErr != nil {
						return nextBucketErr
					}

					nextBucket.Put(key, val)
					bucket.Delete(key)

					return nil
				}

				return nil
			})
		})

		return nil
	})
}

func Put(key, val []byte) error {
	if !db.Ready() {
		return ErrDB
	}

	data, dataErr := marshalCmd(&cmd{"put", [][]byte{key, val}})
	if dataErr != nil {
		return dataErr
	}

	if futureErr := db.raft.Apply(data, 5*time.Second).Error(); futureErr != nil {
		return futureErr
	}

	return nil
}

func Get(key []byte) ([]byte, error) {
	var value []byte

	if !db.Ready() {
		return value, ErrDB
	}

	data, dataErr := marshalCmd(&cmd{"get", [][]byte{key}})
	if dataErr != nil {
		return value, dataErr
	}

	future := db.raft.Apply(data, 5*time.Second)
	if futureErr := future.Error(); futureErr != nil {
		return value, futureErr
	}

	cmdRes, cmdResErr := unmarshalCmd(future.Response())
	if cmdResErr != nil {
		return value, cmdResErr
	}

	if len(cmdRes.Data) > 0 && cmdRes.Action == "response" {
		value = cmdRes.Data[0]
	}

	return value, nil
}
