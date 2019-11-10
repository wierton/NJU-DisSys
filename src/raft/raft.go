package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "math/rand"
import "time"
// import "bytes"
// import "encoding/gob"

// import "runtime/debug"
import "fmt"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
  Index       int
  Command     interface{}
  UseSnapshot bool   // ignore for lab2; only used in lab3
  Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
  mu        sync.Mutex
  peers     []*labrpc.ClientEnd
  persister *Persister
  me        int // index into peers[]

  // Your data here.
  // Look at the paper's Figure 2 for a description of what
  // state a Raft server must maintain.
  term      int
  LeaderId  int
  ElectionTimeout time.Time // miniseconds
}

const (
  RaftModeFollower = 0
  RaftModeCandidate = 1
  RaftModeLeader = 2
)

func (rf *Raft) log(format string, args ...interface{}) {
  s := fmt.Sprintf("[S:%d,T:%d] ", rf.me, rf.term)
  s += fmt.Sprintf(format, args...)
  // fmt.Printf("%s", s)
}

func (rf *Raft) rand(st, ed int) int {
  r := rand.New(rand.NewSource(time.Now().UnixNano()))
  if st >= ed { st, ed = ed, st }
  return r.Intn(ed - st) + st;
}

func (rf *Raft) isLeader() bool {
  return rf.me == rf.LeaderId
}

func (rf *Raft) incTerm() {
  rf.term ++
  rf.LeaderId = -1
}

func (rf *Raft) hasVoteInThisTerm() bool {
  return rf.LeaderId != -1
}

func (rf *Raft) ResetTimeout() {
  amount := time.Duration(rf.rand(150, 300))
  interval := amount * time.Millisecond
  rf.log("timeout %v\n", interval)
  rf.ElectionTimeout = time.Now().Add(interval)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
  // Your code here.
  if rf.isLeader() {
    rf.log("isLeader\n")
  }
  return rf.term, rf.isLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
  // Your code here.
  // Example:
  // w := new(bytes.Buffer)
  // e := gob.NewEncoder(w)
  // e.Encode(rf.xxx)
  // e.Encode(rf.yyy)
  // data := w.Bytes()
  // rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
  // Your code here.
  // Example:
  // r := bytes.NewBuffer(data)
  // d := gob.NewDecoder(r)
  // d.Decode(&rf.xxx)
  // d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
  // Your data here.
  Term     int
  ClientId int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
  // Your data here.
  VoteYou  bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
  // Your code here.
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if rf.hasVoteInThisTerm() { rf.term = args.Term }
  reply.VoteYou = !rf.hasVoteInThisTerm()
  rf.LeaderId = args.ClientId
  rf.ResetTimeout()
  rf.log("from %d, vote %v\n", args.ClientId, reply.VoteYou)
}


type AppendEntriesArgs RequestVoteArgs
type AppendEntriesReply struct {
  Ok bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.ResetTimeout()
  rf.term = args.Term
  rf.LeaderId = args.ClientId
  reply.Ok = true
  rf.log("from %d, heart beat\n", args.ClientId)
}

func (rf *Raft) WaitElectionTimeout() {
  for ; time.Now().Before(rf.ElectionTimeout); {
    time.Sleep(1 * time.Millisecond)
  }
}

func (rf *Raft) LeaderElection() {
  for ;; {
    rf.LeaderId = -1
    rf.ResetTimeout()
    rf.WaitElectionTimeout();

    // as candidate
    rf.log("candidate mode\n")
    rf.term ++;
    req := RequestVoteArgs { Term: rf.term, ClientId: rf.me }
    reply := &RequestVoteReply{}
    total := 0
    count := 0
    for i := 0; i < len(rf.peers); i ++ {
      if i == rf.me { continue }
      ok := rf.peers[i].Call("Raft.RequestVote", req, reply);
      if ok {
        total ++
        if reply.VoteYou { count ++ }
      }
    }

    // as leader
    if count > total / 2 {
      rf.log("leader mode\n")
      rf.LeaderId = rf.me
      for ;; {
        for i := 0; i < len(rf.peers); i ++ {
          if i == rf.me { continue }
            req := AppendEntriesArgs { Term: rf.term, ClientId: rf.me }
            reply := &AppendEntriesReply{}
            rf.peers[i].Call("Raft.AppendEntries", req, reply);
            time.Sleep(10 * time.Millisecond)
        }
      }
    }

    // as follower
    rf.log("follower mode\n")
  }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
/*
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  return ok
}
*/


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
  index := -1

  return index, rf.term, rf.isLeader()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
  // Your code here, if desired.
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
  persister *Persister, applyCh chan ApplyMsg) *Raft {
  rf := &Raft{}
  rf.peers = peers
  rf.persister = persister
  rf.me = me

  // Your initialization code here.
  go rf.LeaderElection()

  // initialize from state persisted before a crash
  rf.readPersist(persister.ReadRaftState())

  return rf
}
