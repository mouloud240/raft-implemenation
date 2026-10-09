package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/mouloud240/raft/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
const HEARTBEAT_INTERVAL = 50 * time.Millisecond
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
	mu sync.Mutex
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
//TODO:review this logic because it is a bit iffy
//Rpc Handlers  of a raftNode
func (r *RaftNode) RequestVote(ctx context.Context, in *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error){

	if r.votedFor != -1 && r.votedFor != int(in.CandidateId){
		return &raft.RequestVoteResponse{Term: int32(r.term),VoteGranted: false},nil 	
	}
	//Verify what node is more "up-to-date"
	if in.Term< int32(r.term) {
		return &raft.RequestVoteResponse{Term: int32(r.term),VoteGranted: false},nil
	}
	if r.nextIndex < int(in.LastLogIndex) { 
	return  &raft.RequestVoteResponse{Term: int32(r.term),VoteGranted: false},nil

}

if r.nextIndex>0 && r.log[r.nextIndex].term > int(in.LastLogIndex){
	return &raft.RequestVoteResponse{Term: int32(r.term),VoteGranted: false},nil
}
r.mu.Lock()
defer r.mu.Unlock()
r.votedFor = int(in.CandidateId)
r.term = int(in.Term)
//If the node is more up-to-date, vote for the candidate
return &raft.RequestVoteResponse{Term: int32(r.term),VoteGranted: true},nil
}

func (r *RaftNode) RequestVoteRpc(){
	//NOTE: I want to do this with a waitgroup that only adds on first len/2 +1 and only wg.done on success
	//But it has a bit of edge cases to handle so we will wait for this at the moment
	r.mu.Lock()
defer r.mu.Unlock()
	r.term++
	errChan:=make(chan error)
	resChan:=make(chan *raft.RequestVoteResponse)
	for _, peer := range r.nodes {
 go func(){
  conn,err:=grpc.NewClient(fmt.Sprintf("%s:%d",peer.host,peer.port),grpc.WithTransportCredentials(insecure.NewCredentials()))
  if err!=nil{
		errChan<-err
	fmt.Println("Error creating grpc client:",err)
	return
  }
defer conn.Close()
client:=raft.NewRaftServiceClient(conn)
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
request := &raft.RequestVoteRequest{
	Term:         int32(r.term),
	CandidateId:  int32(r.id),
	LastLogIndex: int32(r.nextIndex),
	LastLogTerm:  int32(r.log[len(r.log)-1].term),
}
response, err := client.RequestVote(ctx, request)
if err != nil {
	fmt.Println("Error calling RequestVote:", err)
	errChan<-err
	return
}
fmt.Println("RequestVote response:", response)
resChan<-response
	}()
	}
	select {
	case err := <-errChan:
		fmt.Println("Error received:", err)
		return
	case res := <-resChan:
		if res.VoteGranted {
			r.currentVotes++
			if r.currentVotes > len(r.nodes)/2 {
				r.promotSelfToLeader()
				fmt.Println("Node", r.id, "became the leader for term", r.term)
			}
	}
}

}
func (r *RaftNode) promotSelfToLeader(){
			r.state = LEADER
			r.leaderId = r.id
			r.currentVotes = 0
			//Starts the heartbeat ticker to send heartbeats to all peers
     r.startHearbeat()
}
func (r *RaftNode) startElectionTimer(){
	if (r.state == LEADER) {
			return;
	}
	timer:= time.NewTimer(r.electionTimeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if r.state != LEADER {
				fmt.Println("Election timeout reached, starting election for node", r.id)
				r.RequestVoteRpc()
				timer.Reset(r.electionTimeout)
			}
		}
	}
}
func sendHeartBeatToAllPeers(r *RaftNode){
	 for _, peer := range r.nodes {
		go func(peer RaftPeer) {
			errChan := make(chan error)
			resChan := make(chan *raft.AppendEntriesResponse)
			r.AppendEntriesRpcCall([]*raft.LogEntry{}, peer, errChan, resChan)
			select {
			case err := <-errChan:
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
//TODO maybe move the ticker as part of the RaftNode struct and have a stop channel to stop it when the node is no longer a leader
func (r *RaftNode) startHearbeat(){
	ticker:= time.NewTicker(HEARTBEAT_INTERVAL)
	defer ticker.Stop()
	for _ = range ticker.C {
			sendHeartBeatToAllPeers(r)
		}
}
func (r *RaftNode) AppendEntriesRpcCall(entries []*raft.LogEntry, peer RaftPeer, errChan chan error, resChan chan *raft.AppendEntriesResponse){
  conn,err:=grpc.NewClient(fmt.Sprintf("%s:%d",peer.host,peer.port),grpc.WithTransportCredentials(insecure.NewCredentials()))
  if err!=nil{
		errChan<-err
	fmt.Println("Error creating grpc client:",err)
	return
  }
defer conn.Close()
client:=raft.NewRaftServiceClient(conn)
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
request := &raft.AppendEntriesRequest{
	Term: 			 int32(r.term),
	LeaderId: 	 int32(r.id),
	PrevLogIndex: int32(r.nextIndex),
	Entries:entries, // Empty for heartbeat
	LeaderCommit: int32(r.commitIndex),
}
response, err := client.AppendEntries(ctx, request)
if err != nil {
	fmt.Println("Error calling AppendEntries:", err)
	errChan<-err
	return}
resChan<-response

}



 
func (r *RaftNode) Run() error{
	   r.startElectionTimer() 

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
func (l LogEntry) ToRpcFormat() *raft.LogEntry{
	return &raft.LogEntry{
		Command:   l.cmd.String(),
		Term:      int32(l.term),
	}
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
