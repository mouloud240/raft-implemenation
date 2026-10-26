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

func CommandFromString(cmd string) Command {
	parts:=strings.Split(cmd, "|")
	if (len(parts)!=3){
		panic("Invalid command string")
	}

	value, err := strconv.Atoi(parts[1])
	if err != nil {
		panic("Invalid command string")
	}
	op,err:= strconv.Atoi(parts[2])

  if err!=nil{
		panic("Invalid command string")
	}
	return Command{key: parts[0], value:value,operation: OperationFromInt(op)}
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
	nextIndex     int
	nextIndexMap  map[int]int // map of nodeId to nextIndex for each node
	matchIndexMap map[int]int // map of nodeId to highest log entry known to be replicated on server
}

// runtimeState — everything the Figure 2 table does not define:
// identity, configuration, role bookkeeping, servers, and goroutine plumbing.
type runtimeState struct {
	// identity & configuration
	id              int
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

// Function to handle appendEntries Clients requests
func (r *RaftNode) HandleAppendEntry(body ExecuteCommandClientRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != LEADER {
		//Forward to the leader
		//NOTE: I think how will the forwarding work , Grpc? Modifiny the rest hosts ? sending an err to the client ?
		//NOte: Candidates Are either grpc or putting everything behind  a reversy proxy that knows the leader
	}
	successCount := 0
	resChan := make(chan *raft.AppendEntriesResponse)
	errChan := make(chan nodeError)
	r.log = append(r.log, LogEntry{index: r.nextIndex, term: r.term, cmd: body.ToCommand()})
	successCount++ //We count the leader's own append
	for _, peer := range r.nodes {
		go func(peer RaftPeer) {
			r.AppendEntriesRpcCall([]*raft.LogEntry{&raft.LogEntry{
				Command: body.ToCommand().String(),
				Term:    int32(r.term),
				Index:   int32(r.nextIndex),
			}}, peer, errChan, resChan)
		}(peer)
	}
	r.nextIndex++

	for {
		select {
		case err := <-errChan:
			//TODO : should retry here
			fmt.Println("Error received:", err)
		case res := <-resChan:
			if res.Success {
				successCount++
			}
			if successCount > r.majority() {
				//Commit the command to the state machine
				r.MarkAsCommited(r.nextIndex - 1)
				r.ApplyCommand(body.ToCommand())
				r.lastApplied++
				fmt.Println("Command committed successfully")
				//Return to the client
				return
			}
		}
	}

}

// MarkAsCommited advances commitIndex.
// Caller must hold r.mu (HandleAppendEntry holds it for its whole run).
func (r *RaftNode) MarkAsCommited(index int) {
	r.commitIndex++
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
		persistentState: persistentState{
			term:     0, // Figure 2: initialized to 0 on first boot
			votedFor: -1,
			log:      make([]LogEntry, 0),
		},
		volatileState: volatileState{
			// -1 instead of the paper's 0 because this code is 0-indexed with -1 as null
			commitIndex:  -1,
			lastApplied:  -1,
			stateMachine: make(map[string]int),
		},
		leaderState: leaderState{
			nextIndex:     0,
			nextIndexMap:  make(map[int]int),
			matchIndexMap: make(map[int]int),
		},
		runtimeState: runtimeState{
			id:              id,
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
		r.term = int(in.Term)
		r.votedFor = -1
		r.state = FOLLOWER
		r.leaderId = -1
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
	r.term++
	r.currentVotes = 1 //Vote for self
	r.votedFor = r.id

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
			if r.currentVotes >r.majority(){
				//Exit Early
				r.promotSelfToLeader()
				fmt.Println("Node", r.id, "became the leader for term", r.term)
				//Other go routines will still be running but we will ignore their responses
				return

			}
		}
	}
	// Keep this in case we exit the loop on the last item ig (I think I can remove this but I will come back to it )
	if r.currentVotes > len(r.nodes)/2 {
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

	go r.startHearbeat()
	go r.UpdateRoutingConfig()
}
//For the current approach this will update the caddy reverse proxy config to point to the new leader, and will reload caddy
func (r *RaftNode) UpdateRoutingConfig(){
	slog.Info("Updating routing config to point to new leader", "leader_id", strconv.Itoa(r.leaderId))

}
func (r *RaftNode) startElectionTimer() {
	if r.state == LEADER {
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
			if r.state != LEADER {
				fmt.Println("Election timeout reached, starting election for node", r.id)
				r.RequestVoteRpc()
				timer.Reset(r.electionTimeout)
			}
		}
	}
}
func sendHeartBeatToAllPeers(r *RaftNode) {
	for _, peer := range r.nodes {
		go func(peer RaftPeer) {
			errChan := make(chan nodeError)
			resChan := make(chan *raft.AppendEntriesResponse)
			r.AppendEntriesRpcCall([]*raft.LogEntry{}, peer, errChan, resChan)
			select {
			case err := <-errChan:
				//TODO: should retry here
				fmt.Println("Error received:", err)
				return
			case res := <-resChan:
				if res.Success {
					fmt.Println("Heartbeat sent successfully to peer", peer.id)
				} else {
					fmt.Println("Heartbeat failed for peer", peer.id, "with term", res.Term)
				}
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
			if r.state != LEADER {
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
		if in.Term != int32(r.term) {
			r.votedFor = -1
		}
		r.state = FOLLOWER
		r.leaderId = int(in.LeaderId)
		r.currentVotes = 0
		r.term = int(in.Term)

	}
	prev := int(in.PrevLogIndex)
	if prev >= 0 && (prev >= len(r.log) || r.log[prev].term != int(in.PrevLogTerm)) {
		return &raft.AppendEntriesResponse{Term: int32(r.term), Success: false}, nil
	}

	//Extracted here since this is a common logic either for hearbeat or for log replication

	// Advance the commit index forward
	lastNew := prev // heartbeat: nothing new, but everything through prev matches the leader
	if len(in.Entries) > 0 {
		lastNew = int(in.Entries[len(in.Entries)-1].Index)
	}
	if newCommit := min(int(in.LeaderCommit), lastNew); newCommit > r.commitIndex {
		r.commitIndex = newCommit
		r.ApplyCommitedEntries()
	}
	if in.Entries == nil || len(in.Entries) == 0 {
		slog.Info("Received heartbeat from leader", in.LeaderId, "for term", in.Term)
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

		r.log = append(r.log, LogEntry{cmd: CommandFromString(entry.Command), index: int(entry.Index), term: int(entry.Term)})
	}

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
func (r *RaftNode) AppendEntriesRpcCall(entries []*raft.LogEntry, peer RaftPeer, errChan chan nodeError, resChan chan *raft.AppendEntriesResponse) {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", peer.host, peer.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		errChan <- nodeError{error: err, NodeId: r.id}
		fmt.Println("Error creating grpc client:", err)
		return
	}
	defer conn.Close()
	client := raft.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := &raft.AppendEntriesRequest{
		Term:         int32(r.term),
		LeaderId:     int32(r.id),
		PrevLogIndex: int32(r.nextIndex),
		Entries:      entries, // Empty for heartbeat
		LeaderCommit: int32(r.commitIndex),
	}
	response, err := client.AppendEntries(ctx, request)
	if err != nil {
		fmt.Println("Error calling AppendEntries:", err)
		errChan <- nodeError{error: err, NodeId: r.id}
		return
	}
	resChan <- response
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
