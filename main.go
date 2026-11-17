package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mouloud240/raft/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//This implementation of raft will use Grpc as the rpc transport layer , and will store a key-value of [string]interger for the state machine for the sake of simplicty.

//A raft node will allow Set,Get,Increment and Delete operations on the key-value store.

// Client server communication will be RESTfull for the time being for simplicity, and will  not matter much either

// Note for now this type will change since those are only for mapping concepts , they will eventually use the generated types from .proto files

// NOTE: To self:  On commited vs applied
// Commited entry means an entry that has been replicated on the majority of the nodes in the cluster.
// Applied entry: Is a "Commited" Entry that was applied to the state machine
// Entry states: Received (ig) --> Commited --> Applied (this is an asyclic one directional graph, so once an entry is applied it will never be un-applied, but it can be commited and then uncommited if the leader changes and the new leader doesn't have that entry in its log)
type Operation int

const (
	SET Operation = iota
	INCREMENT
	DELETE
)

type NodeState int

const (
	FOLLOWER NodeState = iota
	CANDIDATE
	LEADER
)

type RaftPeer struct {
	id   int // The id of the peer node
	host string
	port int
}
type Command struct {
	key       string
	value     int
	operation Operation
}

func OperationFromInt(op int) Operation {
	switch op {
	case 0:
		return SET
	case 1:
		return INCREMENT
	case 2:
		return DELETE
	default:
		panic("Invalid operation")
	}
}

func CommandFromString(cmd string) (*Command, error) {
	parts := strings.Split(cmd, "|")
	if len(parts) != 3 {
		return nil, errors.New("Invalid command string")
	}

	value, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, errors.New("Invalid command string")
	}
	op, err := strconv.Atoi(parts[2])

	if err != nil {
		return nil, errors.New("Invalid command string")
	}
	return &Command{key: parts[0], value: value, operation: OperationFromInt(op)}, nil
}
func (c Command) String() string {
	return fmt.Sprintf("%s|%d|%d", c.key, c.value, c.operation)
}

// Http body
type ExecuteCommandClientRequest struct {
	Key       string    `json:"key" binding:"required ,min=1"`
	Value     int       `json:"value" binding:"required ,min=1"`
	Operation Operation `json:"operation" binding:"required"`
}

const HEARTBEAT_INTERVAL = 50 * time.Millisecond

// This will also represent the server that will be running the node
// I mean we could have a seperate server type that will have RaftNode as a field but for now this will do
//
// # All int entries will be 0 indexed, so the first entry will be at index 0, -1 will represent null values
//
// The four structs below group RaftNode's state following the paper's Figure 2
// "State" table. They are embedded anonymously, so field promotion keeps the
// accessor syntax identical (r.term, r.log, r.commitIndex, ...) and no call
// site had to change. Whatever the table does not define lives in runtimeState.
type persistentState struct {
	// latest term server has seen (initialized to 0 on first boot, increases
	// monotonically). Figure 2: updated on stable storage before responding
	// to RPCs (persistence not wired yet).
	term int
	// candidateId that received vote in current term (or null if none; -1 here)
	votedFor int
	// log entries; each entry contains command for state machine, and term
	// when entry was received by leader
	log []LogEntry
}

// storedLogEntry is the exported-field DTO the log is persisted through:
// LogEntry/Command have unexported fields, which json.Marshal silently
// drops. The command travels in its existing "key|value|op" string form.
type storedLogEntry struct {
	Index   int    `json:"index"`
	Term    int    `json:"term"`
	Command string `json:"command"`
}

// storedPersistentState is the on-disk shape of persistentState.
type storedPersistentState struct {
	Term     int              `json:"term"`
	VotedFor int              `json:"votedFor"`
	Log      []storedLogEntry `json:"log"`
}

func (p *persistentState) ToStorageFormat() ([]byte, error) {
	stored := storedPersistentState{
		Term:     p.term,
		VotedFor: p.votedFor,
		Log:      make([]storedLogEntry, 0, len(p.log)),
	}
	for _, entry := range p.log {
		stored.Log = append(stored.Log, storedLogEntry{
			Index:   entry.index,
			Term:    entry.term,
			Command: entry.cmd.String(),
		})
	}
	res, err := json.Marshal(stored)
	if err != nil {
		return []byte{}, err
	}
	return res, nil
}

func PersistenStateFromStorageFormat(data []byte) (*persistentState, error) {
	var stored storedPersistentState
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, err
	}
	loaded := &persistentState{
		term:     stored.Term,
		votedFor: stored.VotedFor,
		log:      make([]LogEntry, 0, len(stored.Log)),
	}
	for _, entry := range stored.Log {
		cmd, err := CommandFromString(entry.Command)
		if err != nil {
			return nil, fmt.Errorf("log entry %d: %w", entry.Index, err)
		}
		loaded.log = append(loaded.log, LogEntry{cmd: *cmd, index: entry.Index, term: entry.Term})
	}
	return loaded, nil
}

func NewPersistentState(path string) persistentState {

	if _, err := os.Stat(path); err != nil {
		//Default to the initial state if the file doesn't exist (first boot)
		if os.IsNotExist(err) {
			return persistentState{
				term:     0, // Figure 2: initialized to 0 on first boot
				votedFor: -1,
				log:      make([]LogEntry, 0),
			}
		}
	}

	loaded, err := LoadPersistentState(path)

	if err != nil {
		//In case of error return the default state, but log the error
		fmt.Println("Error loading persistent state:", err)
		return persistentState{
			term:     0, // Figure 2: initialized to 0 on first boot
			votedFor: -1,
			log:      make([]LogEntry, 0),
		}
	}
	return *loaded
}
func (p *persistentState) save(path string) error {
	st, err := p.ToStorageFormat()
	if err != nil {
		fmt.Println("Error converting to storage format:", err)
		return err
	}
	// Write to a temp file then rename: a crash mid-write leaves the
	// previous snapshot intact instead of a truncated or appended-garbage
	// file (the old O_APPEND mode accumulated concatenated snapshots).
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	if _, err := file.Write(st); err != nil {
		file.Close()
		os.Remove(tmp)
		fmt.Println("Error writing to file:", err)
		return err
	}
	if err := file.Close(); err != nil {
		os.Remove(tmp)
		fmt.Println("Error closing file:", err)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		fmt.Println("Error renaming file:", err)
		return err
	}
	return nil
}
func LoadPersistentState(path string) (*persistentState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	loaded, err := PersistenStateFromStorageFormat(data)
	if err != nil {
		return nil, err
	}
	return loaded, nil
}

// volatileState — Figure 2 "Volatile state on all servers"
type volatileState struct {
	commitIndex int // index of highest log entry known to be committed (paper inits to 0; -1 here because this code is 0-indexed with -1 as null)
	lastApplied int // index of highest log entry applied to the state machine (same -1 note as commitIndex)
	// the state machine itself; not part of Figure 2's table but volatile
	// derived state: recomputed by replaying the log (§5.3) once persistence lands
	stateMachine map[string]int
}

// leaderState — Figure 2 "Volatile state on leaders" (reinitialized after election)
type leaderState struct {
	// Figure 2 wants one nextIndex[] per server: "index of the next log entry
	// to send to that server (initialized to leader last log index + 1)".
	// The single value plus the empty per-peer maps below are pre-existing
	// quirks kept unchanged by this refactor (see plan item 3).
	nextIndexMap  map[int]int // map of nodeId to nextIndex for each node
	matchIndexMap map[int]int // map of nodeId to highest log entry known to be replicated on server
}

// runtimeState — everything the Figure 2 table does not define:
// identity, configuration, role bookkeeping, servers, and goroutine plumbing.
type runtimeState struct {
	// identity & configuration
	id int
	// path of this node's persistent-state file (see nodePathFromId)
	statePath       string
	peerInfo        RaftPeer         // the peer info of the node itself
	nodes           map[int]RaftPeer // all nodes in the cluster, excluding itself, key is the nodeId
	electionTimeout time.Duration    // randomized between 150-300ms

	// role bookkeeping (§5.1/§5.2)
	leaderId     int // id of the current leader; if this node is the leader, its own id; -1 if unknown
	state        NodeState
	currentVotes int // votes received in the current term; resets to 0 when the term changes

	// servers & goroutine plumbing
	apiClient       http.ServeMux
	client          *http.ServeMux // the restful http server
	server          *grpc.Server
	resetElectionCh chan struct{}
	stopHeartbeatCh chan struct{}
}

type RaftNode struct {
	persistentState // Figure 2: persistent state on all servers
	volatileState   // Figure 2: volatile state on all servers
	leaderState     // Figure 2: volatile state on leaders
	runtimeState
	mu sync.Mutex // guards all state groups above
	raft.UnimplementedRaftServiceServer
}
type AppendResult struct {
	PeerId int
	Res    *raft.AppendEntriesResponse
	Err    error
}

func (r *RaftNode) majority() int { return (len(r.nodes)+1)/2 + 1 }

func (r *RaftNode) GetNodeById(id int) (RaftPeer, bool) {
	peer, ok := r.nodes[id]
	return peer, ok
}
func (req *ExecuteCommandClientRequest) ToCommand() Command {
	return Command{
		key:       req.Key,
		value:     req.Value,
		operation: req.Operation,
	}
}

type nodeError struct {
	error
	NodeId int //For retries

}

// persistLocked saves the node's persistent state (term, votedFor, log)
// to disk. Caller must hold r.mu. Called at every mutation point of the
// persistent state, per Figure 2: "updated on stable storage before
// responding to RPCs".
func (r *RaftNode) persistLocked() {
	if err := r.save(r.statePath); err != nil {
		slog.Error("Failed to persist state", "node_id", strconv.Itoa(r.id), "error", err)
	}
}

// StepDown reverts the node to follower. newTerm is the term seen in a request
// or response (Figure 2: "If RPC request or response contains term T >
// currentTerm: set currentTerm = T, convert to follower").
func (r *RaftNode) StepDown(newTerm int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.term = newTerm
	r.state = FOLLOWER
	r.leaderId = -1
	r.votedFor = -1
	r.currentVotes = 0
	//The stepped-down term must survive a crash
	r.persistLocked()
	slog.Info("Node is stepping down to follower", "node_id", strconv.Itoa(r.id), "term", strconv.Itoa(r.term))
}

// Function to handle appendEntries Clients requests
func (r *RaftNode) HandleAppendEntry(body ExecuteCommandClientRequest) {
	r.mu.Lock()
	if r.state != LEADER {
		r.mu.Unlock()
		//Forward to the leader
		//NOTE: I think how will the forwarding work , Grpc? Modifiny the rest hosts ? sending an err to the client ?
		//NOte: Candidates Are either grpc or putting everything behind  a reversy proxy that knows the leader
		return
	}
	//The entry's index is simply where it lands in the log (0-indexed, -1 is the empty log)
	entryIdx := len(r.log)
	//Snapshot the term while still holding the lock: the goroutines below
	//must not read r.term unlocked while elections can change it
	termAtEntry := r.term
	r.log = append(r.log, LogEntry{index: entryIdx, term: termAtEntry, cmd: body.ToCommand()})
	//The leader's own log entry is durable before replication starts
	r.persistLocked()
	r.mu.Unlock()

	successCount := 1 //We count the leader's own append
	resChan := make(chan AppendResult, len(r.nodes))
	for _, peer := range r.nodes {
		go func(peer RaftPeer) {
			resChan <- r.AppendEntriesRpcCall([]*raft.LogEntry{&raft.LogEntry{
				Command: body.ToCommand().String(),
				Term:    int32(termAtEntry),
				Index:   int32(entryIdx),
			}}, peer)
		}(peer)
	}

	timeout := time.After(5 * time.Second)
	committed := false
	applyResult := func(res AppendResult) {
		if res.Err != nil {
			//A network error teaches nothing about the log, only a rejection does
			//It also must not abort the whole replication: the remaining peers
			//can still tip the quorum (previously this returned early and the
			//command silently never committed)
			fmt.Println("Replication failed for peer", res.PeerId, ":", res.Err)
			return
		}
		if res.Res.Success {
			successCount++
			//Every success advances that peer's maps, not only the one that tips the quorum
			r.mu.Lock()
			r.matchIndexMap[res.PeerId] = entryIdx
			r.nextIndexMap[res.PeerId] = entryIdx + 1
			r.mu.Unlock()
		} else if res.Res.Term > int32(termAtEntry) {
			r.StepDown(int(res.Res.Term))
		} else {
			r.mu.Lock()
			if r.nextIndexMap[res.PeerId] > 0 {
				r.nextIndexMap[res.PeerId]--
			}
			r.mu.Unlock()
		}
	}
	//Drain EVERY response: returning right at quorum used to leave late
	//acks unread in resChan, so their nextIndex/matchIndex updates never
	//ran, the next command went out with a stale prevLogIndex, and lagging
	//followers could never catch up through the commit heartbeat.
	for processed := 0; processed < len(r.nodes); {
		select {
		case res := <-resChan:
			processed++
			applyResult(res)
			if !committed && successCount >= r.majority() {
				committed = true
				//Commit the command to the state machine; the remaining
				//responses are still drained below
				r.mu.Lock()
				r.MarkAsCommited(entryIdx)
				r.ApplyCommitedEntries()
				r.mu.Unlock()
				fmt.Println("Command committed successfully")
			}

		case <-timeout:
			//Quorum didn't happen, the entry stays in our log uncommitted
			//(a future leader will commit or overwrite it). Drain the
			//responses that already arrived so their map updates survive.
			for {
				select {
				case res := <-resChan:
					applyResult(res)
				default:
					return
				}
			}
		}
	}

}

// MarkAsCommited advances commitIndex.
// Caller must hold r.mu.
func (r *RaftNode) MarkAsCommited(index int) {
	if index > r.commitIndex {
		r.commitIndex = index
	}
}

// Apply a single command to the state machine and doens't update the applyIndex (doesn't check if the command were commited or not, that should be done before calling this function)
func (r *RaftNode) ApplyCommand(cmd Command) error {
	if cmd.operation == SET {
		r.stateMachine[cmd.key] = cmd.value
	} else if cmd.operation == INCREMENT {
		_, ok := r.stateMachine[cmd.key]
		if !ok {
			return errors.New("key does not exist")
		}
		r.stateMachine[cmd.key] += cmd.value

	} else if cmd.operation == DELETE {
		delete(r.stateMachine, cmd.key)
	} else {
		return errors.New("invalid operation")
	}
	return nil
}

func NewRaftApiClient() *http.ServeMux {

	mux := http.NewServeMux()
	mux.HandleFunc("POST /set", func(w http.ResponseWriter, r *http.Request) {
		rawBody := make([]byte, r.ContentLength)
		r.Body.Read(rawBody)
		defer r.Body.Close()
		var body ExecuteCommandClientRequest
		if err := json.Unmarshal(rawBody, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

	})
	mux.HandleFunc("GET /get/{key}", func(w http.ResponseWriter, r *http.Request) {
		rawKey := r.PathValue("key")
		key, err := strconv.ParseInt(rawKey, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		fmt.Print(key)
	})
	return mux

}

// nodePathFromId returns the state file path for a node. RAFT_DATA_DIR
// overrides the directory (used by the sandbox to isolate test runs so
// stale files from previous runs are never loaded); empty means the
// current working directory.
func nodePathFromId(id int) string {
	dir := os.Getenv("RAFT_DATA_DIR")
	if dir == "" {
		return fmt.Sprintf("node_%d_state.json", id)
	}
	return fmt.Sprintf("%s/node_%d_state.json", dir, id)
}
func NewRaftNode(id int, host string, port int, peers []RaftPeer) *RaftNode {
	nodes := make(map[int]RaftPeer, len(peers))
	resetTimerCh := make(chan struct{}, 1)
	stopHeartBeatCh := make(chan struct{}, 1)

	for _, peer := range peers {
		nodes[peer.id] = peer
	}
	timeout := rand.IntN(150)
	timeout += 150
	return &RaftNode{
		persistentState: NewPersistentState(nodePathFromId(id)),
		volatileState: volatileState{
			// -1 instead of the paper's 0 because this code is 0-indexed with -1 as null
			commitIndex:  -1,
			lastApplied:  -1,
			stateMachine: make(map[string]int),
		},
		leaderState: leaderState{
			nextIndexMap:  make(map[int]int),
			matchIndexMap: make(map[int]int),
		},
		runtimeState: runtimeState{
			id:              id,
			statePath:       nodePathFromId(id),
			peerInfo:        RaftPeer{id: id, host: host, port: port},
			nodes:           nodes,
			electionTimeout: time.Duration(timeout) * time.Millisecond,
			leaderId:        -1,
			state:           FOLLOWER,
			currentVotes:    0,
			client:          http.NewServeMux(),
			resetElectionCh: resetTimerCh,
			stopHeartbeatCh: stopHeartBeatCh,
		},
	}
}

// TODO:review this logic because it is a bit iffy
// Rpc Handlers  of a raftNode
func (r *RaftNode) RequestVote(ctx context.Context, in *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if in.Term < int32(r.term) {
		return &raft.RequestVoteResponse{Term: int32(r.term), VoteGranted: false}, nil
	} else {
		//Only a genuinely higher term resets the vote state (Figure 2:
		//"if term T > currentTerm, set currentTerm = T ..."). An
		//equal-term request must not clobber votedFor, or the node could
		//grant a second vote within the same term.
		newTerm := in.Term > int32(r.term)
		if newTerm {
			r.term = int(in.Term)
			r.votedFor = -1
			r.state = FOLLOWER
			r.leaderId = -1
			//A new term must be durable before responding, even when the
			//vote ends up refused
			r.persistLocked()
		}
	}

	if r.votedFor != -1 && r.votedFor != int(in.CandidateId) {
		return &raft.RequestVoteResponse{Term: int32(r.term), VoteGranted: false}, nil
	}
	//Verify what node is more "up-to-date"
	myIdx := r.LastLogIndex() // -1 means empty log
	myTerm := -1
	if myIdx >= 0 {
		myTerm = r.log[myIdx].term
	}
	if int(in.LastLogTerm) < myTerm ||
		(int(in.LastLogTerm) == myTerm && int(in.LastLogIndex) < myIdx) {
		return &raft.RequestVoteResponse{Term: int32(r.term), VoteGranted: false}, nil
	}

	r.votedFor = int(in.CandidateId)
	r.term = int(in.Term)
	//Persist the granted vote before answering the candidate
	r.persistLocked()
	// When gratting vote reset the election timer
	//To keep it non-blocking we will use a select statement
	select {
	case r.resetElectionCh <- struct{}{}:
	default:
	}
	// If the node is more up-to-date, vote for the candidate
	return &raft.RequestVoteResponse{Term: int32(r.term), VoteGranted: true}, nil
}

// LastLogIndex returns the index of the last log entry, or -1 when the log is
// empty (this code is 0-indexed with -1 as null; the paper is 1-indexed).
func (r *RaftNode) LastLogIndex() int {
	return len(r.log) - 1
}

func (r *RaftNode) RequestVoteRpc() {
	//NOTE: I want to do this with a waitgroup that only adds on first len/2 +1 and only wg.done on success
	//But it has a bit of edge cases to handle so we will wait for this at the moment
	r.mu.Lock()
	defer r.mu.Unlock()
	//Every node that wants to become a leader MUST set its state to CANDIDATE before starting the election , this helps tracking all places where the node want to become a leader , and in the future we can abstract the r.STATE to a function to help gating the election better
	if r.state != CANDIDATE {
		return
	}
	r.term++
	r.currentVotes = 1 //Vote for self
	r.votedFor = r.id
	//The candidate state must be durable before the vote requests go out
	r.persistLocked()

	errChan := make(chan error)
	resChan := make(chan *raft.RequestVoteResponse)
	for _, peer := range r.nodes {
		go func() {
			conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", peer.host, peer.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				errChan <- err
				fmt.Println("Error creating grpc client:", err)
				return
			}
			defer conn.Close()
			client := raft.NewRaftServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			var lastLogTerm int32
			if len(r.log) > 0 {
				lastLogTerm = int32(r.log[len(r.log)-1].term)
			} else {
				lastLogTerm = -1
			}

			request := &raft.RequestVoteRequest{
				Term:         int32(r.term),
				CandidateId:  int32(r.id),
				LastLogIndex: int32(r.LastLogIndex()),
				LastLogTerm:  lastLogTerm,
			}
			response, err := client.RequestVote(ctx, request)
			if err != nil {
				fmt.Println("Error calling RequestVote:", err)
				errChan <- err
				return
			}
			fmt.Println("RequestVote response:", response)
			resChan <- response
		}()
	}
	for range r.nodes {
		select {
		case err := <-errChan:
			fmt.Println("Error received:", err)
		case res := <-resChan:
			if res.VoteGranted {
				r.currentVotes++
			}
			if r.currentVotes >= r.majority() {
				//Exit Early
				r.promotSelfToLeader()
				fmt.Println("Node", r.id, "became the leader for term", r.term)
				//Other go routines will still be running but we will ignore their responses
				return

			}
		}
	}
	// Keep this in case we exit the loop on the last item ig (I think I can remove this but I will come back to it )
	if r.currentVotes >= r.majority() {
		r.promotSelfToLeader()
		fmt.Println("Node", r.id, "became the leader for term", r.term)
	}

}
func (r *RaftNode) promotSelfToLeader() {
	slog.Info(
		"Node is promoting itself to leader",
		"node_id", strconv.Itoa(r.id),
		"term", strconv.Itoa(r.term),
	)
	r.state = LEADER
	r.leaderId = r.id
	r.votedFor = -1
	r.currentVotes = 0
	for p := range r.nodes {
		r.nextIndexMap[p] = r.LastLogIndex() + 1
		r.matchIndexMap[p] = -1
	}

	go r.startHearbeat()
	go r.UpdateRoutingConfig()
}

// For the current approach this will update the caddy reverse proxy config to point to the new leader, and will reload caddy
func (r *RaftNode) UpdateRoutingConfig() {
	//leaderId is written under the mutex by elections; read it the same way
	r.mu.Lock()
	leader := r.leaderId
	r.mu.Unlock()
	slog.Info("Updating routing config to point to new leader", "leader_id", strconv.Itoa(leader))

}
func (r *RaftNode) startElectionTimer() {
	r.mu.Lock()
	isLeader := r.state == LEADER
	r.mu.Unlock()
	if isLeader {
		return
	}
	timer := time.NewTimer(r.electionTimeout)
	defer timer.Stop()
	for {
		select {
		case <-r.resetElectionCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(r.electionTimeout)
		case <-timer.C:
			//state is guarded by the mutex everywhere else; check and set
			//it under the lock too (the write here used to race with RPC
			//handlers and the heartbeat loop)
			r.mu.Lock()
			notLeader := r.state != LEADER
			if notLeader {
				//A timeout promotes the node to candidate before it can start the election
				r.state = CANDIDATE
			}
			r.mu.Unlock()
			if notLeader {
				fmt.Println("Election timeout reached, starting election for node", r.id)
				r.RequestVoteRpc()
			}
			//Always re-arm, a leader that skips the campaign must not leave the timer dead
			//or the Stop/drain dance below will wedge this loop forever after a demotion
			timer.Reset(r.electionTimeout)
		}
	}
}
func sendHeartBeatToAllPeers(r *RaftNode) {
	for _, peer := range r.nodes {
		go func(peer RaftPeer) {
			res := r.AppendEntriesRpcCall([]*raft.LogEntry{}, peer)
			if res.Err != nil {
				//TODO: handle error
				return
			}
			if res.Res.Success {
				fmt.Println("Heartbeat sent successfully to peer", peer.id)
			} else {
				//Read our term under the lock: response goroutines race with
				//elections stepping the term up
				r.mu.Lock()
				myTerm := r.term
				r.mu.Unlock()
				if res.Res.Term > int32(myTerm) {
					r.StepDown(int(res.Res.Term))
				}
				fmt.Println("Heartbeat failed for peer", peer.id, "with term", res.Res.Term)
			}
		}(peer)
	}
}

// TODO maybe move the ticker as part of the RaftNode struct and have a stop channel to stop it when the node is no longer a leader
func (r *RaftNode) startHearbeat() {
	ticker := time.NewTicker(HEARTBEAT_INTERVAL)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopHeartbeatCh:
			return
		case <-ticker.C:
			r.mu.Lock()
			isLeader := r.state == LEADER
			r.mu.Unlock()
			if !isLeader {
				return
			}
			sendHeartBeatToAllPeers(r)

		}
	}
}
func StopHeartBeat(r *RaftNode) {
	r.stopHeartbeatCh <- struct{}{}
}
func (r *RaftNode) AppendEntries(ctx context.Context, in *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// We exit early first then start mutating the object
	if int(in.Term) < r.term {
		// For now we won't reset other metrics
		return &raft.AppendEntriesResponse{Term: int32(r.term), Success: false}, nil
	} else {
		//Only a genuinely new term hits the disk: re-saving on every 50ms
		//heartbeat would be pure write spam
		newTerm := in.Term > int32(r.term)
		if in.Term != int32(r.term) {
			r.votedFor = -1
		}
		r.state = FOLLOWER
		r.leaderId = int(in.LeaderId)
		r.currentVotes = 0
		r.term = int(in.Term)
		if newTerm {
			//The new term must be durable before we act on this RPC
			r.persistLocked()
		}
	}
	prev := int(in.PrevLogIndex)
	if prev >= 0 && (prev >= len(r.log) || r.log[prev].term != int(in.PrevLogTerm)) {
		return &raft.AppendEntriesResponse{Term: int32(r.term), Success: false}, nil
	}

	if in.Entries == nil || len(in.Entries) == 0 {
		//Heartbeat: nothing new, but everything through prev matches the
		//leader, so the commit index can advance through prev. This runs
		//after the prev check, so prev is guaranteed to be in our log.
		if newCommit := min(int(in.LeaderCommit), prev); newCommit > r.commitIndex {
			r.commitIndex = newCommit
			r.ApplyCommitedEntries()
		}
		slog.Info("Received heartbeat from leader", "leader_id", in.LeaderId, "term", in.Term)
		//To keep it non-blocking we will use a select statement to reset the election timer

		select {
		case r.resetElectionCh <- struct{}{}:
		default:
		}
		return &raft.AppendEntriesResponse{Term: int32(r.term), Success: true}, nil
	}

	for _, entry := range in.Entries {

		idx := int(entry.Index)
		if idx < len(r.log) {
			if r.log[idx].term != int(entry.Term) {
				//Conflict, delete the entry and all that follow it
				r.log = r.log[:idx]
			} else {
				continue //Entry already exists, no need to append
			}
		}
		cmd, err := CommandFromString(entry.Command)
		if err != nil {
			return nil, err
		}

		r.log = append(r.log, LogEntry{cmd: *cmd, index: int(entry.Index), term: int(entry.Term)})
	}

	//Advance the commit index forward, AFTER appending: committing up to
	//the last new entry is only safe once those entries are actually in
	//our log (ApplyCommitedEntries indexes r.log by commitIndex, and the
	//old position of this block indexed past the log when LeaderCommit
	//covered an entry that arrived in this same RPC)
	lastNew := int(in.Entries[len(in.Entries)-1].Index)
	if newCommit := min(int(in.LeaderCommit), lastNew); newCommit > r.commitIndex {
		r.commitIndex = newCommit
		r.ApplyCommitedEntries()
	}

	//The log changed (appended or truncated on conflict); make it durable
	//before acking success to the leader
	r.persistLocked()

	select {
	case r.resetElectionCh <- struct{}{}:
	default:
	}

	return &raft.AppendEntriesResponse{Term: int32(r.term), Success: true}, nil

}

// Caller should hold r.mu
func (r *RaftNode) ApplyCommitedEntries() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		entry := r.log[r.lastApplied]
		if err := r.ApplyCommand(entry.cmd); err != nil {
			fmt.Println("Error applying committed entry:", err)
		}
	}
}
func (r *RaftNode) AppendEntriesRpcCall(entries []*raft.LogEntry, peer RaftPeer) AppendResult {
	//Snapshot the request fields under the lock: this is called from
	//response goroutines while command replication writes nextIndexMap,
	//commitIndex and term concurrently
	r.mu.Lock()
	term := r.term
	prevLogIndex := r.nextIndexMap[peer.id] - 1
	//Term of the entry at prevLogIndex, for the receiver's log-consistency
	//check (AppendEntries handler). Never send 0 here: every real entry has
	//term >= 1, so a zero would make the receiver reject perfectly matching
	//logs, which blocked election-timer resets and caused election churn.
	prevLogTerm := -1 // -1 = null, like everywhere else; the check is skipped when prevLogIndex < 0
	if prevLogIndex >= 0 && prevLogIndex < len(r.log) {
		prevLogTerm = r.log[prevLogIndex].term
	}
	commitIndex := r.commitIndex
	r.mu.Unlock()
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", peer.host, peer.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("Error creating grpc client:", err)
		return AppendResult{PeerId: peer.id, Res: nil, Err: err}
	}
	defer conn.Close()
	client := raft.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := &raft.AppendEntriesRequest{
		Term:         int32(term),
		LeaderId:     int32(r.id),
		PrevLogIndex: int32(prevLogIndex),
		PrevLogTerm:  int32(prevLogTerm),
		Entries:      entries, // Empty for heartbeat
		LeaderCommit: int32(commitIndex),
	}
	response, err := client.AppendEntries(ctx, request)
	if err != nil {
		fmt.Println("Error calling AppendEntries:", err)
		return AppendResult{PeerId: peer.id, Res: nil, Err: err}
	}
	return AppendResult{PeerId: peer.id, Res: response, Err: nil}
}
func (r *RaftNode) Run() error {

	go r.startElectionTimer()
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", r.peerInfo.host, r.peerInfo.port))
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	raft.RegisterRaftServiceServer(server, r)
	r.server = server

	if err := server.Serve(lis); err != nil {
		return err
	}

	return nil

}

type RequestVoteRpcBody struct {
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

type LogEntry struct {
	cmd   Command
	index int //Position in the log
	term  int
}

func (l LogEntry) GetIndex() int {
	return l.index

}
func (l LogEntry) ToRpcFormat() *raft.LogEntry {
	return &raft.LogEntry{
		Command: l.cmd.String(),
		Term:    int32(l.term),
	}
}

type AppendEntriesRpcBody struct {
	term         int //Leaders term
	leaderId     int //so follower can redirect clients
	prevLogIndex int
	entries      []LogEntry //empty for heartbeat
	leaderCommit int
}

// Determines if the AppendEntriesRpcBody is a heartbeat or not
func (r *AppendEntriesRpcBody) isHeartbeat() bool {
	return len(r.entries) == 0
}

// Interface for Raft RPCs
type RaftRpc interface {
	RequestVoteRpc(body RequestVoteRpcBody) (term int, voteGranted bool)
	AppendEntriesRpc(body AppendEntriesRpcBody) (term int, success bool)
}

func main() {
	node := NewRaftNode(
		1,
		"127.0.0.1",
		50051,
		[]RaftPeer{
			{id: 2, host: "127.0.0.1", port: 50052},
			{id: 3, host: "127.0.0.1", port: 50053},
		},
	)

	if err := node.Run(); err != nil {
		panic(err)
	}
}
