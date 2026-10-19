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


func CommandFromString(cmd string) Command {

	return Command{}
}
func (c Command) String() string {
	return ""
}


//Http body
type ExecuteCommandClientRequest struct {
	Key   string `json:"key" binding:"required ,min=1"`
	Value int    `json:"value" binding:"required ,min=1"`
	Operation Operation  `json:"operation" binding:"required"`
}

const HEARTBEAT_INTERVAL = 50 * time.Millisecond

// This will also represent the server that will be running the node
// I mean we could have a seperate server type that will have RaftNode as a field but for now this will do
// All int entries will be 0 indexed, so the first entry will be at index 0, -1 will represent null values
// TODO: group fileds into structs , example persistenState, volatileState, etc
type RaftNode struct {
	//For now just an int
	id              int
	apiClient       http.ServeMux
	server          *grpc.Server
	resetElectionCh chan struct{}
	stopHeartbeatCh chan struct{}
	term            int            //Current term of the node
	votedFor        int            //To keep track who the node voted for in the current term, -1 if it hasn't voted yet
	currentVotes    int            //To keep track of the votes received in the current term , //Resets to 0 when the term changes
	client          *http.ServeMux // The restfull http server
	commitIndex     int            //Index of the highest log entry known to be committed
	lastApplied     int            //Index of the highest log entry applied to the state machine
	peerInfo        RaftPeer       //The peer info of the node itself
	nextIndex       int
	leaderId        int //Id of the current leader // if the node is a leader, this will be its own id
	state           NodeState
	electionTimeout time.Duration       // Randomized between 150-300ms
	nodes           map[int]RaftPeer // A map of all nodes in the cluster , excluding itself, key is the nodeId
	log             []LogEntry          //Log entries
	nextIndexMap    map[int]int         //Map of nodeId to nextIndex for each node
	matchIndexMap   map[int]int         //Map of nodeId to matchIndex for each node
	stateMachine    map[string]int      //Key-value store
	mu              sync.Mutex
	raft.UnimplementedRaftServiceServer
}
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

//Function to handle appendEntries Clients requests
func (r *RaftNode)HandleAppendEntry(body ExecuteCommandClientRequest){
	if (r.state != LEADER) {
		//Forward to the leader 
		//NOTE: I think how will the forwarding work , Grpc? Modifiny the rest hosts ? sending an err to the client ?
		//NOte: Candidates Are either grpc or putting everything behind  a reversy proxy that knows the leader
	}
	successCount:=0
	resChan := make(chan *raft.AppendEntriesResponse)
	errChan := make(chan nodeError)
	for _, peer := range r.nodes {
		go func(peer RaftPeer) {
			r.AppendEntriesRpcCall([]*raft.LogEntry{&raft.LogEntry{
				Command:body.ToCommand().String(),
				Term: int32(r.term),
				Index: int32(r.nextIndex),
			}}, peer, errChan, resChan)
		}(peer)
	}
	for {
		select {
		case err := <-errChan:
			//TODO : should retry here
			fmt.Println("Error received:", err)
		case res := <-resChan:
			if res.Success {
				successCount++
			}
			if successCount > len(r.nodes)/2 {
				//Commit the command to the state machine
				r.CommitCommand(body.ToCommand())
				fmt.Println("Command committed successfully")
				//Return to the client 
				return
			}
		}
	}

}

func (r *RaftNode) CommitCommand(cmd Command) error {
	r.mu.Lock()
	defer r.mu.Unlock()
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
	r.commitIndex++
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
		resetElectionCh: resetTimerCh,
		stopHeartbeatCh: stopHeartBeatCh,
		id:              id,
		term:            0,
		votedFor:        -1,
		currentVotes:    0,
		client:          http.NewServeMux(),
		commitIndex:     -1,
		lastApplied:     -1,
		peerInfo:        RaftPeer{id: id, host: host, port: port},
		nextIndex:       0,
		leaderId:        -1,
		state:           FOLLOWER,
		electionTimeout: time.Duration(timeout) * time.Millisecond,
		nodes:           nodes,
		log:             make([]LogEntry, 0),
		nextIndexMap:    make(map[int]int),
		matchIndexMap:   make(map[int]int),
		stateMachine:    make(map[string]int),
	}
}

// TODO:review this logic because it is a bit iffy
// Rpc Handlers  of a raftNode
func (r *RaftNode) RequestVote(ctx context.Context, in *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
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
	if r.nextIndex < int(in.LastLogIndex) {
		return &raft.RequestVoteResponse{Term: int32(r.term), VoteGranted: false}, nil
	}

	if r.nextIndex > 0 && r.log[r.nextIndex].term > int(in.LastLogIndex) {
		return &raft.RequestVoteResponse{Term: int32(r.term), VoteGranted: false}, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.votedFor = int(in.CandidateId)
	r.term = int(in.Term)
	// If the node is more up-to-date, vote for the candidate
	return &raft.RequestVoteResponse{Term: int32(r.term), VoteGranted: true}, nil
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
				LastLogIndex: int32(r.nextIndex),
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
			if r.currentVotes > len(r.nodes)/2 {
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
func (r *RaftNode) AppendEntriesRpcCall(entries []*raft.LogEntry, peer RaftPeer, errChan chan nodeError, resChan chan *raft.AppendEntriesResponse) {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", peer.host, peer.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		errChan <- nodeError{error: err,NodeId: r.id}
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
		errChan <- nodeError{error: err,NodeId: r.id}
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
