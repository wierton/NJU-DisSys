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
import "bytes"
import "encoding/gob"

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
  applyCh   chan ApplyMsg
  persisted bool

  // Your data here.
  // Look at the paper's Figure 2 for a description of what
  // state a Raft server must maintain.
  Killed    bool
  Term      int
  LeaderId  int
  Logs      []interface{}
  Index     int
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
  w := new(bytes.Buffer)
  e := gob.NewEncoder(w)
  rf.persisted=true
  e.Encode(rf.persisted)
  e.Encode(rf.me)
  e.Encode(rf.Term)
  e.Encode(rf.LeaderId)
  e.Encode(rf.Logs)
  e.Encode(rf.Index)
  rf.persister.SaveRaftState(w.Bytes())
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
  r := bytes.NewBuffer(data)
  d := gob.NewDecoder(r)
  d.Decode(&rf.persisted)
  if ! rf.persisted {
    return
  }
  d.Decode(&rf.me)
  d.Decode(&rf.Term)
  d.Decode(&rf.LeaderId)
  d.Decode(&rf.Logs)
  d.Decode(&rf.Index)
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

const (
  AEMSG_1 = 1
  AEMSG_2 = 2
)

type AppendEntriesArgs struct {
  Msg      int
  Term     int
  ClientId int
  Index    int
  Logs     []interface{}
}
type AppendEntriesReply struct {
  Ok bool
  Index    int
  Logs     []interface{}
}

func dumpLogs(logs []interface{}) string {
  s := fmt.Sprintf("{");
  for i := 0; i < len(logs); i ++ {
    c, ok := logs[i].(int)
    if ok {
      s += fmt.Sprintf("%d, ", c);
    } else {
      s += fmt.Sprintf("?, ");
    }
  }
  s += fmt.Sprintf("}");
  return s
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if rf.Term > args.Term {
    rf.log("from %d, bad heart beat\n", args.ClientId)
    reply.Ok = false
    return
  }

  timeout := rf.ResetTimeout()
  rf.log("from %d, heart beat %d, n-timeout %v, n-term %d\n", args.ClientId,
    args.Msg, timeout, args.Term)
  if args.Msg == AEMSG_1 {
    reply.Logs = rf.Logs
    reply.Index = rf.Index
  } else if args.Msg == AEMSG_2 {
    for i := rf.Index; i < len(args.Logs); i ++ {
      rf.log("Commit %d, Index %d, Logs %d\n", args.Logs[i], rf.Index, len(args.Logs))
      if i < len(rf.Logs) {
        rf.Logs[i] = args.Logs[i]
      } else {
        rf.Logs = append(rf.Logs, args.Logs[i])
      }
      rf.persist()
      rf.Index ++
      ap := ApplyMsg { Index:rf.Index, Command:args.Logs[i] }
      rf.applyCh <- ap;
      rf.persist()
    }
  }
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

func (rf *Raft) findMaxLogQueue(i int, max *int, q *[]interface{}) bool {
  req := AppendEntriesArgs {
    Msg: AEMSG_1,
    Term: rf.Term, ClientId: rf.me,
    Index: rf.Index, Logs: rf.Logs }
  reply := &AppendEntriesReply{}

  rf.log("wait.findMaxLogQueue(%d)\n", i)
  ok := rf.RPC(i, "Raft.AppendEntries", req, reply);
  if ok {
    if reply.Index > *max {
      *max = reply.Index
      *q = reply.Logs
    }
    rf.log("findMaxLogQueue(%d) suc\n", i)
  } else {
    rf.log("findMaxLogQueue(%d) failed\n", i)
  }
  return ok
}

func (rf *Raft) commitLogs(i int, Index int) {
  req := AppendEntriesArgs {
    Msg: AEMSG_2,
    Term: rf.Term, ClientId: rf.me,
    Index: rf.Index, Logs: rf.Logs }
  reply := &AppendEntriesReply{}

  rf.log("wait.commitLogs(%d), Index: %d, logs: %s\n", i, Index, dumpLogs(rf.Logs))
  ok := rf.RPC(i, "Raft.AppendEntries", req, reply);
  if ok {
    rf.log("commitLogs(%d) suc\n", i)
  } else {
    rf.log("commitLogs(%d) failed\n", i)
  }
}

func (rf *Raft) asLeader() {
  rf.mu.Lock()
  rf.LeaderId = rf.me
  rf.mu.Unlock()

  for ; rf.isLeader(); {
    var max int = 0
    var queue []interface{}
    rf.persist()
    count := 1
    for i := 0; i < len(rf.peers) && rf.isLeader(); i ++ {
      if i == rf.me { continue }
      if rf.Killed { rf.log("Killed\n"); return; }
      ok := rf.findMaxLogQueue(i, &max, &queue)
      if ok { count ++ }
    }

    if count <= len(rf.peers) / 2 { continue }

    rf.log("max is %d, logs is %s\n", max, dumpLogs(queue))
    // sync logs firstly
    if max > rf.Index {
      for i := rf.Index; i < max; i ++ {
        if i < len(rf.Logs) {
          rf.mu.Lock()
          rf.Logs[i] = queue[i]
          rf.mu.Unlock()
        } else {
          rf.mu.Lock()
          rf.Logs = append(rf.Logs, queue[i])
          rf.mu.Unlock()
        }
        rf.persist()
        rf.Index ++
        ap := ApplyMsg { Index:rf.Index, Command:rf.Logs[i] }
        rf.log("Commit %d, Index %d, Logs %d\n", rf.Logs[i], rf.Index, len(rf.Logs))
        rf.applyCh <- ap;
        rf.persist()
      }
    } else {
      max = len(rf.Logs)
    }

    for i := 0; i < len(rf.peers) && rf.isLeader(); i ++ {
      if i == rf.me { continue }
      if rf.Killed { rf.log("Killed\n"); return; }
      rf.commitLogs(i, max)
    }

    time.Sleep(10 * time.Millisecond)
  }
}

func (rf *Raft) asFollower() {
  rf.mu.Lock()
  rf.LeaderId = -1
  rf.mu.Unlock()

  rf.ResetTimeout()
  rf.persist()
  rf.log("WaitElectionTimeout\n")
  rf.WaitElectionTimeout();
}

func (rf *Raft) MainLoop() {
  isLeader := rf.isLeader()
  for ;; {
    if (isLeader) {
      rf.asLeader()
      isLeader = false
    } else {
      rf.asFollower();
      count := rf.asCandidate()
      total := len(rf.peers)
      if count > total / 2 {
        isLeader = true
        rf.asLeader()
        continue
      }
    }
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
  isLeader := rf.isLeader()
  if isLeader {
    rf.mu.Lock()
    rf.Logs = append(rf.Logs, command)
    c, _ := command.(int)
    rf.log("Start %d, index %d, %s\n", c, rf.Index, dumpLogs(rf.Logs))
    rf.mu.Unlock()
    return len(rf.Logs), rf.Term, isLeader
  }
  return rf.Index, rf.Term, isLeader
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
  rf.Index = 0
  rf.Term = 0
  rf.LeaderId = -1
  rf.applyCh = applyCh
  rf.Logs = make([]interface{}, 0)

  // Your initialization code here.
  rf.Killed = false

  // initialize from state persisted before a crash
  rf.readPersist(persister.ReadRaftState())
  go rf.MainLoop()

  return rf
}
