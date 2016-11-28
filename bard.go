package bard

import (
	"encoding/binary"
	"errors"
	"github.com/boltdb/bolt"
	"hash/fnv"
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
	ErrInvalidShardNumber      = errors.New("Invalid number of shards")
	ErrDB                      = errors.New("DB not open")
	ErrUnableToSaveShardConfig = errors.New("Unable to save shard number in config")
)

var (
	db *DB
)

func Open(path string, shards int64) error {
	if shards < 1 {
		return ErrInvalidShardNumber
	}

	boltDB, err := bolt.Open(path, 0644, nil)
	if err != nil {
		return err
	}

	db = &DB{boltDB, shards}

	if configErr := checkExistingConfig(shards); configErr != nil {
		return configErr
	}

	return nil
}

func Close() error {
	if db != nil {
		return db.bolt.Close()
	}

	return ErrDB
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

		var numShards int64
		var numShardsErr int
		var updateShardNumber bool

		binaryValueNumShards := bucket.Get([]byte(KeyShardNumber))
		if len(binaryValueNumShards) == 0 {
			updateShardNumber = true
		} else {
			numShards, numShardsErr = binary.Varint(binaryValueNumShards)
			if numShardsErr <= 0 || numShards <= 0 {
				updateShardNumber = true
			}
		}
		if numShards != shards {
			updateShardNumber = true
		}

		if rebalanceErr := rebalanceShards(tx, numShards, shards); rebalanceErr != nil {
			return rebalanceErr
		}

		if updateShardNumber {
			buffer := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
			bytesRead := binary.PutVarint(buffer, shards)
			if bytesRead <= 0 {
				return ErrUnableToSaveShardConfig
			}
			if err := bucket.Put([]byte(KeyShardNumber), buffer); err != nil {
				return err
			}
		}

		return nil
	})
}

func rebalanceShards(tx *bolt.Tx, old, new int64) error {
	if itrErr := tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
		if string(name) == BardConfigBucket {
			return nil
		}

		bucketIndex, bucketIndexErr := binary.Varint(name)
		if bucketIndexErr < 0 {
		}
		return bucket.ForEach(func(key, val []byte) error {
		})
	}); itrErr != nil {
		return itrErr
	}
	if old < new {
		for i := old + 1; i < new; i++ {
			println(i)
		}
		return nil
	} else if old > new {
		return nil
	} else {
		return nil
	}
}

func keyToHash(key []byte) uint64 {
	hasher := fnv.New64a()
	hasher.Write(key)

	return hasher.Sum64()
}
