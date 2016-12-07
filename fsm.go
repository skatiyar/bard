package bard

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	"io"
)

const (
	put      = "put"
	get      = "get"
	response = "response"
	cmdError = "cmdError"
)

var (
	ErrCmdUnmarshal     = errors.New("interface type not supported")
	ErrCmdNilReceived   = errors.New("received nil instead of []byte")
	ErrCmdInvalidAction = errors.New("invalid command action")
)

type fsm struct {
}

type cmd struct {
	Action string
	Data   [][]byte
}

func marshalCmd(c *cmd) ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	if encodeErr := gob.NewEncoder(buffer).Encode(c); encodeErr != nil {
		return nil, encodeErr
	}

	return buffer.Bytes(), nil
}

func unmarshalCmd(src interface{}) (*cmd, error) {
	c := &cmd{}

	if buffer, ok := src.([]byte); ok {
		if decodeErr := gob.NewDecoder(bytes.NewReader(buffer)).Decode(c); decodeErr != nil {
			return nil, decodeErr
		}

		return c, nil
	} else if src == nil {
		return c, ErrCmdNilReceived
	} else if tc, ok := src.(*cmd); ok {
		c = tc
		return c, nil
	} else if tc, ok := src.(cmd); ok {
		c = &tc
		return c, nil
	} else if err, ok := src.(error); ok {
		return c, err
	} else {
		return c, ErrCmdUnmarshal
	}
}

func newFSM() raft.FSM {
	return &fsm{}
}

func (f *fsm) Apply(fsmLog *raft.Log) interface{} {
	if fsmLog.Type == raft.LogCommand {
		cmdRec, cmdRecErr := unmarshalCmd(fsmLog.Data)
		if cmdRecErr != nil {
			return &cmd{cmdError, [][]byte{[]byte(cmdRecErr.Error())}}
		}

		switch cmdRec.Action {
		case get:
			var value []byte
			if viewErr := db.bolt.View(func(tx *bolt.Tx) error {
				buffer := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
				bytesRead := binary.PutVarint(buffer, jch(keyToHash(cmdRec.Data[0]), db.numShards))
				if bytesRead <= 0 {
					return ErrShardNumber
				}

				value = tx.Bucket(buffer).Get(cmdRec.Data[0])
				return nil
			}); viewErr != nil {
				return viewErr
			}

			return &cmd{response, [][]byte{value}}
		case put:
			return db.bolt.Update(func(tx *bolt.Tx) error {
				buffer := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64)
				bytesRead := binary.PutVarint(buffer, jch(keyToHash(cmdRec.Data[0]), db.numShards))
				if bytesRead <= 0 {
					return ErrShardNumber
				}

				return tx.Bucket(buffer).Put(cmdRec.Data[0], cmdRec.Data[1])
			})
		}

		return &cmd{cmdError, [][]byte{[]byte(ErrCmdInvalidAction.Error())}}
	} else if fsmLog.Type == raft.LogNoop {
		return nil
	} else if fsmLog.Type == raft.LogAddPeer {
		return nil
	} else if fsmLog.Type == raft.LogBarrier {
		return nil
	} else if fsmLog.Type == raft.LogRemovePeer {
		return nil
	} else {
		return nil
	}
}

func (f *fsm) Restore(state io.ReadCloser) error {
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}
