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
  Killed    bool
  Term      int
  LeaderId  int
  ElectionTimeout time.Time // miniseconds
}

const (
  RaftModeFollower = 0
  RaftModeCandidate = 1
  RaftModeLeader = 2
)

func (rf *Raft) log(format string, args ...interface{}) {
  nowStr := time.Now().Format("15:04:05.000")
  s := fmt.Sprintf("%s [S:%d,T:%d] ", nowStr, rf.me, rf.Term)
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
  rf.mu.Lock()
  defer rf.mu.Unlock()

  rf.Term ++
  rf.LeaderId = -1
}

func (rf *Raft) hasVoteInThisTerm() bool {
  return rf.LeaderId != -1
}

func (rf *Raft) ResetTimeout() time.Duration {
  amount := time.Duration(rf.rand(150, 300))
  interval := amount * time.Millisecond
  rf.ElectionTimeout = time.Now().Add(interval)
  return interval
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
  // Your code here.
  if rf.isLeader() {
    rf.log("isLeader\n")
  }
  return rf.Term, rf.isLeader()
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

  if rf.Term > args.Term { return }
  if rf.Term < args.Term { rf.LeaderId = -1 }

  reply.VoteYou = !rf.hasVoteInThisTerm()
  if reply.VoteYou { rf.LeaderId = args.ClientId }
  timeout := rf.ResetTimeout()
  rf.log("from %d, vote %v, n-timeout %v, n-term: %d\n", args.ClientId, reply.VoteYou,
    timeout, args.Term)
  rf.Term = args.Term
}


type AppendEntriesArgs RequestVoteArgs
type AppendEntriesReply struct {
  Ok bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  timeout := rf.ResetTimeout()
  rf.log("from %d, heart beat, n-timeout %v, n-term %d\n", args.ClientId, timeout,
    args.Term)
  rf.Term = args.Term
  rf.LeaderId = args.ClientId
  reply.Ok = true
}

func (rf *Raft) WaitElectionTimeout() {
  for ; time.Now().Before(rf.ElectionTimeout); {
    if rf.Killed { rf.log("Killed\n"); return; }
    time.Sleep(1 * time.Millisecond)
  }
}

func (rf *Raft) RPC(i int, svcMeth string, args interface{}, reply interface{}) bool {
  replyOk := make(chan bool)
  go func() {
    replyOk <- rf.peers[i].Call(svcMeth, args, reply)
  } ()

  select {
    case ok := <-replyOk:
      return ok
    case <- time.After(15 * time.Millisecond):
      rf.log("wait.%s(%d) timeout\n", svcMeth, i)
      return false
  }
}

func (rf *Raft) asCandidate() int {
  // as candidate
  rf.log("candidate mode, update term to %d\n", rf.Term + 1)
  rf.incTerm()
  req := RequestVoteArgs { Term: rf.Term, ClientId: rf.me }
  reply := &RequestVoteReply{}
  count := 1
  for i := 0; i < len(rf.peers); i ++ {
    if i == rf.me { continue }
    if rf.Killed { rf.log("Killed\n"); return 0; }
    rf.log("wait.RequestVote(%d)\n", i)
    ok := rf.RPC(i, "Raft.RequestVote", req, reply);
    if ok {
      if reply.VoteYou { count ++ }
    }
  }
  return count
}

func (rf *Raft) asLeader() {
  rf.mu.Lock()
  rf.LeaderId = rf.me
  rf.mu.Unlock()

  for ; rf.isLeader(); {
    for i := 0; i < len(rf.peers) && rf.isLeader(); i ++ {
      if i == rf.me { continue }
      if rf.Killed { rf.log("Killed\n"); return; }
      req := AppendEntriesArgs { Term: rf.Term, ClientId: rf.me }
      reply := &AppendEntriesReply{}

      rf.log("wait.AppendEntries(%d)\n", i)
      rf.RPC(i, "Raft.AppendEntries", req, reply);
    }
    time.Sleep(10 * time.Millisecond)
  }
}

func (rf *Raft) asFollower() {
  rf.mu.Lock()
  rf.LeaderId = -1
  rf.mu.Unlock()

  rf.ResetTimeout()
  rf.log("WaitElectionTimeout\n")
  rf.WaitElectionTimeout();
}

func (rf *Raft) MainLoop() {
  for ;; {
    rf.asFollower();
    if rf.Killed { rf.log("Killed\n"); return; }

    count := rf.asCandidate()
    if rf.Killed { rf.log("Killed\n"); return; }

    total := len(rf.peers)
    if count > total / 2 {
      // as leader
      rf.log("leader mode %d/%d\n", count, total)
      rf.asLeader()
    } else {
      // as follower
      rf.log("follower mode\n")
    }
    if rf.Killed { rf.log("Killed\n"); return; }
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

  return index, rf.Term, rf.isLeader()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
  // Your code here, if desired.
  rf.Killed = true
  rf.log("I'm Killed\n")
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
  rf.Killed = false
  go rf.MainLoop()

  // initialize from state persisted before a crash
  rf.readPersist(persister.ReadRaftState())

  return rf
}
