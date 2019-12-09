package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Dlog entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Dlog, each Raft peer
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
// as each Raft peer becomes aware that successive Dlog entries are
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
  LastApplied int
  ElectionTimeout time.Time // miniseconds
}

const (
  RaftModeFollower = 0
  RaftModeCandidate = 1
  RaftModeLeader = 2
)

func (rf *Raft) Dlog(format string, args ...interface{}) {
  nowStr := time.Now().Format("15:04:05.000")
  s := fmt.Sprintf("%s [S:%d,T:%d] ", nowStr, rf.me, rf.Term)
  s += fmt.Sprintf(format, args...)
  fmt.Printf("%s", s)
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
    rf.Dlog("isLeader\n")
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
  e.Encode(rf.LastApplied)
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
  d.Decode(&rf.LastApplied)
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
  Index    int
  Logs     []interface{}
}

func copyLogs(to *[]interface{}, from []interface{}, B, E int) {
  for i := B; i < E; i ++ {
    if i < len(*to) {
      (*to)[i] = from[i]
    } else {
      *to = append(*to, from[i])
    }
  }
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
  if reply.VoteYou {
    rf.LeaderId = args.ClientId
    reply.Index = rf.Index
    reply.Logs = rf.Logs
  }
  timeout := rf.ResetTimeout()
  rf.Dlog("from %d, vote %v, n-timeout %v, n-term: %d\n", args.ClientId, reply.VoteYou,
    timeout, args.Term)
  rf.Term = args.Term
}

type AppendEntriesArgs struct {
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

func (rf *Raft) ApplyChangesLoop() {
  for ; !rf.Killed; {
    for ; rf.LastApplied < rf.Index; {
      ap := ApplyMsg { Index:rf.LastApplied + 1, Command:rf.Logs[rf.LastApplied] }
      rf.Dlog("Commit %d, Index %d %d, Logs %d\n", rf.Logs[rf.LastApplied],
          rf.LastApplied, rf.Index, len(rf.Logs))
      rf.persist()
      rf.LastApplied ++
      rf.applyCh <- ap
      rf.persist()
    }
    time.Sleep(1 * time.Millisecond)
  }
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if rf.Term > args.Term {
    rf.Dlog("from %d, bad heart beat\n", args.ClientId)
    reply.Ok = false
    return
  }

  timeout := rf.ResetTimeout()
  rf.Dlog("from %d, n-timeout %v, n-term %d, Index %d %d\n", args.ClientId,
      timeout, args.Term, rf.Index, args.Index)
  if len(args.Logs) > rf.Index {
    copyLogs(&rf.Logs, args.Logs, rf.Index, len(args.Logs))
  }
  if args.Index > rf.Index {
    rf.Index = args.Index
  }
  rf.Term = args.Term
  rf.LeaderId = args.ClientId
  reply.Ok = true
}

func (rf *Raft) WaitElectionTimeout() {
  for ; time.Now().Before(rf.ElectionTimeout); {
    if rf.Killed { rf.Dlog("Killed\n"); return; }
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
      rf.Dlog("wait.%s(%d) timeout\n", svcMeth, i)
      return false
  }
}

func (rf *Raft) asCandidate() int {
  // as candidate
  rf.Dlog("candidate mode, update term to %d\n", rf.Term + 1)
  rf.incTerm()
  req := RequestVoteArgs { Term: rf.Term, ClientId: rf.me }
  reply := &RequestVoteReply{}
  count := 1
  for i := 0; i < len(rf.peers); i ++ {
    if i == rf.me { continue }
    if rf.Killed { rf.Dlog("Killed\n"); return 0; }
    rf.Dlog("wait.RequestVote(%d)\n", i)
    ok := rf.RPC(i, "Raft.RequestVote", req, reply);
    if ok && reply.VoteYou {
      count ++
      if reply.Index > rf.Index {
        copyLogs(&rf.Logs, reply.Logs, rf.Index, reply.Index)
        rf.Index = reply.Index
      }
    }
  }
  rf.Dlog("candidate mode end %d, logs %s\n", count, dumpLogs(rf.Logs))
  return count
}

func (rf *Raft) syncLogs(i int, Index int) bool {
  req := AppendEntriesArgs {
    Term: rf.Term, ClientId: rf.me,
    Index: rf.Index, Logs: rf.Logs }
  reply := &AppendEntriesReply{}

  rf.Dlog("wait.syncLogs(%d), Index: %d, logs: %s\n", i, Index, dumpLogs(rf.Logs))
  ok := rf.RPC(i, "Raft.AppendEntries", req, reply);
  ok = ok && reply.Ok
  if ok {
    rf.Dlog("syncLogs(%d) suc\n", i)
  } else {
    rf.Dlog("syncLogs(%d) failed\n", i)
  }
  return ok
}

func (rf *Raft) asLeader() {
  rf.mu.Lock()
  rf.LeaderId = rf.me
  rf.mu.Unlock()

  for ; rf.isLeader(); {
    rf.persist()

    count := 1
    for i := 0; i < len(rf.peers) && rf.isLeader(); i ++ {
      if i == rf.me { continue }
      ok := rf.syncLogs(i, len(rf.Logs))
      if ok { count ++ }
    }
    if count > len(rf.peers) / 2 {
      rf.Index = len(rf.Logs)
    }

    if rf.Killed { rf.Dlog("Killed\n"); return; }
    time.Sleep(10 * time.Millisecond)
  }
}

func (rf *Raft) asFollower() {
  rf.mu.Lock()
  rf.LeaderId = -1
  rf.mu.Unlock()

  rf.ResetTimeout()
  rf.persist()
  rf.Dlog("WaitElectionTimeout\n")
  rf.WaitElectionTimeout();
}

func (rf *Raft) MainLoop() {
  isLeader := rf.isLeader()
  go rf.ApplyChangesLoop()
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
// agreement on the next command to be appended to Raft's Dlog. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Dlog, since the leader
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
    rf.Dlog("Start %d, index %d, %s\n", c, rf.Index, dumpLogs(rf.Logs))
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
  rf.Dlog("I'm Killed\n")
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
  rf.LastApplied = 0
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
