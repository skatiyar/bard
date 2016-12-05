package bard

import (
	"encoding/binary"
	"github.com/boltdb/bolt"
)

const (
	BardConfigBucket = "bard-config-bucket"
	KeyShardNumber   = "number-of-shards"
)

var (
	db = &Store{}
)

func Open(path string, shards int64) error {
	if shards < 1 {
		return ErrInvalidShardNumber
	}

	boltDB, err := bolt.Open(path, 0644, nil)
	if err != nil {
		return err
	}

	db.bolt = boltDB
	db.numShards = shards
	db.reSharding = true

	if configErr := checkExistingShards(shards); configErr != nil {
		return configErr
	}

	db.reSharding = false

	return nil
}

func Close() error {
	if !db.Ready() {
		return ErrDB
	}

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

	return db.bolt.Update(func(tx *bolt.Tx) error {
		buffer := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
		bytesRead := binary.PutVarint(buffer, jch(keyToHash(key), db.numShards))
		if bytesRead <= 0 {
			return ErrShardNumber
		}

		return tx.Bucket(buffer).Put(key, val)
	})
}

func Get(key []byte) ([]byte, error) {
	var value []byte

	if !db.Ready() {
		return value, ErrDB
	}

	return value, db.bolt.View(func(tx *bolt.Tx) error {
		buffer := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
		bytesRead := binary.PutVarint(buffer, jch(keyToHash(key), db.numShards))
		if bytesRead <= 0 {
			return ErrShardNumber
		}

		value = tx.Bucket(buffer).Get(key)
		return nil
	})
}
