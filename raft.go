package bard

import (
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"net"
	"os"
)

func newRaft(port, master, dbPathPrefix string) (*raft.Raft, error) {
	network, networkErr := newNetwork(port, master)
	if networkErr != nil {
		return nil, networkErr
	}

	logStore, logStoreErr := newLogStore(dbPathPrefix)
	if logStoreErr != nil {
		return nil, logStoreErr
	}

	stableStore, stableStoreErr := newStableStore(dbPathPrefix)
	if stableStoreErr != nil {
		return nil, stableStoreErr
	}

	config := raft.DefaultConfig()
	config.StartAsLeader = true

	iRaft, iRaftErr := raft.NewRaft(
		config,
		newFSM(),
		logStore,
		stableStore,
		newSnapshotStore(),
		newPeerStore(network),
		network)
	if iRaftErr != nil {
		return nil, iRaftErr
	}

	return iRaft, nil
}

func newNetwork(port, master string) (raft.Transport, error) {
	var slaveAddr net.Addr
	var slaveAddrErr error

	if len(master) != 0 {
		slaveAddr, slaveAddrErr = net.ResolveTCPAddr("tcp", master)
		if slaveAddrErr != nil {
			return nil, slaveAddrErr
		}
	}

	return raft.NewTCPTransport(port, slaveAddr, 10, 10, os.Stdout)
}

func newPeerStore(trans raft.Transport) raft.PeerStore {
	return raft.NewJSONPeers("bard-peers", trans)
}

func newSnapshotStore() raft.SnapshotStore {
	return raft.NewDiscardSnapshotStore()
}

func newStableStore(pathPrefix string) (raft.StableStore, error) {
	return raftboltdb.NewBoltStore(pathPrefix + "-stable-store")
}

func newLogStore(pathPrefix string) (raft.LogStore, error) {
	return raftboltdb.NewBoltStore(pathPrefix + "-log-store")
}
