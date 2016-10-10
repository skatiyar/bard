package bard

import (
	"encoding/binary"
	"errors"
	"github.com/boltdb/bolt"
	"hash/fnv"
	"os"
	"strconv"
)

type DB struct {
	bolt      *bolt.DB
	numShards int64
}

const (
	BardConfigBucket = "bard-config-bucket"
	KeyShardNumber   = "shards"
)

var (
	ErrInvalidShardNumber = errors.New("Invalid number of shards")
	ErrDB                 = errors.New("DB not open")
)

var (
	db *DB
)

func Open(path string, shards int64) error {
	if shards < 1 {
		return ErrInvalidShardNumber
	}

	boltDB, err := bolt.Open(path, os.ModeExclusive, nil)
	if err != nil {
		return err
	}

	db = &DB{boltDB, shards}

	if configErr := checkExistingConfig(shards); configErr != nil {
		return configErr
	}

	return nil
}

func Put(key, val []byte) error {
	if db != nil {
		return db.bolt.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte(strconv.FormatInt(jch(keyToHash(key), db.numShards), 10))).Put(key, val)
		})
	}

	return ErrDB
}

func Get(key []byte) ([]byte, error) {
	var value []byte

	if db != nil {
		readErr := db.bolt.View(func(tx *bolt.Tx) error {
			value = tx.Bucket([]byte(strconv.FormatInt(jch(keyToHash(key), db.numShards), 10))).Get(key)
			return nil
		})

		return value, readErr
	}

	return value, ErrDB
}

func checkExistingConfig(shards int64) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		bucket, bucketErr := tx.CreateBucketIfNotExists([]byte(BardConfigBucket))
		if bucketErr != nil {
			return bucketErr
		}

		numShards, numShardsErr := binary.Varint(bucket.Get([]byte(KeyShardNumber)))
		if numShardsErr <= 0 {
			if err := bucket.Put([]byte(KeyShardNumber), []byte(strconv.FormatInt(shards, 10))); err != nil {
				return err
			}

			var i int64
			for i = 0; i < shards; i++ {
				_, createBucketErr := tx.CreateBucket([]byte(strconv.FormatInt(i, 10)))
				if createBucketErr != nil {
					if rollbackErr := tx.Rollback(); rollbackErr != nil {
						return rollbackErr
					}

					return createBucketErr
				}
			}
		}
		if numShards != shards {
			if err := bucket.Put([]byte(KeyShardNumber), []byte(strconv.FormatInt(shards, 10))); err != nil {
				return err
			}

			// redistribute
		}

		return nil
	})
}

func keyToHash(key []byte) uint64 {
	hasher := fnv.New64a()
	hasher.Write(key)

	return hasher.Sum64()
}
