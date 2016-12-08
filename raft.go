package bard

import (
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"log"
	"os"
	"time"
)

func newRaft(port, dbPathPrefix string, peers []string) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	// config.EnableSingleNode = true
	// config.StartAsLeader = true
	config.Logger = log.New(os.Stdout, "[ Raft ] ", log.LstdFlags)

	network, networkErr := newNetwork(port, config.Logger)
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

	peerStore, peerStoreErr := newPeerStore(peers, dbPathPrefix, network)
	if peerStoreErr != nil {
		return nil, peerStoreErr
	}

	iRaft, iRaftErr := raft.NewRaft(
		config,
		newFSM(),
		logStore,
		stableStore,
		newSnapshotStore(),
		peerStore,
		network)
	if iRaftErr != nil {
		return nil, iRaftErr
	}

	return iRaft, nil
}

func newNetwork(port string, log *log.Logger) (raft.Transport, error) {
	return raft.NewTCPTransportWithLogger(port, nil, 10, 5*time.Second, log)
}

func newPeerStore(peers []string, dbPathPrefix string, trans raft.Transport) (raft.PeerStore, error) {
	peerStore := raft.NewJSONPeers(dbPathPrefix, trans)

	if len(peers) != 0 {
		if addErr := peerStore.SetPeers(peers); addErr != nil {
			return nil, addErr
		}
	}

	return peerStore, nil
}

func newSnapshotStore() raft.SnapshotStore {
	return raft.NewDiscardSnapshotStore()
}

func newStableStore(pathPrefix string) (raft.StableStore, error) {
	return raftboltdb.NewBoltStore(pathPrefix + "/stable-store")
}

func newLogStore(pathPrefix string) (raft.LogStore, error) {
	return raftboltdb.NewBoltStore(pathPrefix + "/log-store")
}
