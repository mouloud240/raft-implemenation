package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/mouloud240/raft/raft"
	"google.golang.org/grpc"
)

//This implementation of raft will use Grpc as the rpc transport layer , and will store a key-value of [string]interger for the state machine for the sake of simplicty.

//A raft node will allow Set,Get,Increment and Delete operations on the key-value store.

// Client server communication will be RESTfull for the time being for simplicity, and will  not matter much either

//Note for now this type will change since those are only for mapping concepts , they will eventually use the generated types from .proto files
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
type RaftPeer struct{
	id int // The id of the peer node
	host string
	port int
}
//This will also represent the server that will be running the node
//I mean we could have a seperate server type that will have RaftNode as a field but for now this will do
//All int entries will be 0 indexed, so the first entry will be at index 0, -1 will represent null values
//TODO: group fileds into structs , example persistenState, volatileState, etc
type RaftNode struct{
	//For now just an int
	id int
	term int //Current term of the node
	votedFor int//To keep track who the node voted for in the current term, -1 if it hasn't voted yet
	currentVotes int //To keep track of the votes received in the current term , //Resets to 0 when the term changes
	client *http.ServeMux // The restfull http server
	commitIndex int  //Index of the highest log entry known to be committed
	lastApplied int //Index of the highest log entry applied to the state machine
  peerInfo RaftPeer //The peer info of the node itself
	nextIndex int 
	leaderId int //Id of the current leader // if the node is a leader, this will be its own id
  state NodeState	
	electionTimeout time.Duration // Randomized between 150-300ms
	nodes map[string]RaftPeer // A map of all nodes in the cluster , excluding itself, key is the nodeId
	log []LogEntry //Log entries
	nextIndexMap map[int]int //Map of nodeId to nextIndex for each node
	matchIndexMap map[int]int //Map of nodeId to matchIndex for each node
	stateMachine map[string]int //Key-value store
	raft.UnimplementedRaftServiceServer
}
func NewRaftNode(id int, host string, port int, peers []RaftPeer) *RaftNode {
	nodes := make(map[string]RaftPeer, len(peers))

	for _, peer := range peers {
		nodes[fmt.Sprint(peer.id)] = peer
	}

	return &RaftNode{
		id:               id,
		term:             0,
		votedFor:         -1,
		currentVotes:     0,
		client:            http.NewServeMux(),
		commitIndex:      -1,
		lastApplied:      -1,
		peerInfo:         RaftPeer{id: id, host: host, port: port},
		nextIndex:        0,
		leaderId:         -1,
		state:            FOLLOWER,
		electionTimeout:  250 * time.Millisecond,
		nodes:            nodes,
		log:              make([]LogEntry, 0),
		nextIndexMap:     make(map[int]int),
		matchIndexMap:    make(map[int]int),
		stateMachine:     make(map[string]int),
	}
}

//Rpc Handlers  of a raftNode
func (r *RaftNode) RequestVoteRpcHandler(body RequestVoteRpcBody) (term int, voteGranted bool){
	if r.votedFor != -1 && r.votedFor != body.candidateId {
		return r.term,false
	}
	//Verify what node is more "up-to-date"
	if body.term< r.term {
		return r.term,false
	}
	if r.nextIndex < body.lastLogIndex { 
	return  r.term,false
}
if r.log[r.nextIndex].term > body.lastLogTerm {
	return r.term,false
}
r.votedFor = body.candidateId
r.term = body.term
//If the node is more up-to-date, vote for the candidate
return body.term,true

}
func (r *RaftNode) RequestVote(ctx context.Context, in *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error){

	return &raft.RequestVoteResponse{Term: int32(r.term),VoteGranted: true},nil
}

func (r *RaftNode) Run() error{
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d",r.peerInfo.host,r.peerInfo.port))
		if err!=nil{
			return err
		}

	server:=grpc.NewServer()
	
	
	raft.RegisterRaftServiceServer(server,r)

	if err:=server.Serve(lis);err!=nil{
		return err;
	}

	return nil;

}


type RequestVoteRpcBody struct{
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}
type Command struct {
	key string
	value int 
	operation  Operation
}
func CommandFromString(cmd string) Command{

	return Command{}
}
func (c Command) String() string{
	return ""
}

type LogEntry struct {
	cmd Command
	index int //Position in the log
	term int
}
type AppendEntriesRpcBody struct{
	term int //Leaders term
	leaderId int //so follower can redirect clients
	prevLogIndex int 
	entries []LogEntry //empty for heartbeat
	leaderCommit int
}
//Determines if the AppendEntriesRpcBody is a heartbeat or not
func (r *AppendEntriesRpcBody ) isHeartbeat() bool{
	return len(r.entries) == 0
}
//Interface for Raft RPCs
type RaftRpc interface{
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
