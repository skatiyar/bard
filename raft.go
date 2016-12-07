package bard

import (
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"log"
	"os"
)

func newRaft(port, dbPathPrefix string) (*raft.Raft, error) {
	network, networkErr := newNetwork(port)
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
	// config.EnableSingleNode = true
	// config.StartAsLeader = true
	config.Logger = log.New(os.Stdout, "[ Raft ] ", log.LstdFlags)

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

func newNetwork(port string) (raft.Transport, error) {
	return raft.NewTCPTransportWithLogger(port, nil, 10, 10, log.New(os.Stdout, "[ Net ] ", log.LstdFlags))
}

func newPeerStore(trans raft.Transport) raft.PeerStore {
	return raft.NewJSONPeers("db", trans)
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
