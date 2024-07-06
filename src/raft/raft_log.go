package raft

import "fmt"

type Entry struct {
	Term    int
	Index   int // 第一个entry的index是 1，此时commitIndex是 0
	Command interface{}
}

type RaftLogs struct {
	logs []Entry
}

func NewRaftLogs() RaftLogs {
	return RaftLogs{
		logs: []Entry{{Term: 0, Index: 0, Command: nil}},
	}
}

func (rl *RaftLogs) String() string {
	s := ""
	for i := range rl.logs {
		s += fmt.Sprintf("{%v %v %v} ", rl.logs[i].Index, rl.logs[i].Term, rl.logs[i].Command)
	}
	return s
}

func (rl *RaftLogs) CopyLogsFrom(index int) []Entry {
	entries := []Entry{}
	k := rl.findEntryPosByIndex(index)
	if k == -1 {
		return entries
	}
	for i := k; i < len(rl.logs); i++ {
		entries = append(entries, rl.logs[i])
	}
	return entries
}

func (rl *RaftLogs) CopyLogsForCommit(prevCommit, currCommit int) []Entry {
	start := rl.findEntryPosByIndex(prevCommit) + 1
	// 如果snapshot覆盖了log，完全可能会出现被覆盖的部分之前
	// 没有发送过ApplyMsg。这里该如何处理？暂时先只发送拥有的log吧
	// 注释掉这里的panic
	if start == 0 {
		// panic("cannot find prevIdx")
		start = 1
	}
	j := len(rl.logs)
	for i := start; i < len(rl.logs); i++ {
		if rl.logs[i].Index > currCommit {
			j = i
			break
		}
	}
	forReport := make([]Entry, j-start)
	copy(forReport, rl.logs[start:j])
	return forReport
}

func (rl *RaftLogs) GetLastIndex() int {
	return rl.logs[len(rl.logs)-1].Index
}

func (rl *RaftLogs) GetLastTerm() int {
	return rl.logs[len(rl.logs)-1].Term
}

func (rl *RaftLogs) GetFirstIndex() int {
	return rl.logs[0].Index
}

func (rl *RaftLogs) GetFirstTerm() int {
	return rl.logs[0].Term
}

func (rl *RaftLogs) AppendLog(term int, command interface{}) {
	lastIdx := rl.GetLastIndex()
	rl.logs = append(rl.logs, Entry{Index: lastIdx + 1, Term: term, Command: command})
}

func (rl *RaftLogs) ContainLog(index, term int) bool {
	i := rl.findEntryPosByIndex(index)
	return i != -1 && rl.logs[i].Term == term
}

// returns ok or not
func (rl *RaftLogs) Overwrite(prevLogIndex, prevLogTerm int, entries []Entry) bool {
	// TODO: 如果 rf.log 中有 dummy 数值的话，重新考虑下这里的逻辑
	idx := rl.findEntryPosByIndex(prevLogIndex)

	if idx == -1 || prevLogTerm != rl.logs[idx].Term {
		return false
	}

	// overwrite
	rl.logs = append(rl.logs[:idx+1], entries...)
	return true
}

func (rl *RaftLogs) TrimFrom(index int) {
	j := rl.findEntryPosByIndex(index)
	if j == -1 {
		DPrintf("cannot find index\n")
		return
	}
	rl.logs = rl.logs[j:]
	rl.logs[0].Command = nil
}
func (rl *RaftLogs) TrimForSnapshot(lastIncludedIndex, lastIncludedTerm int) {
	j := rl.findEntryPosByIndex(lastIncludedIndex)
	if j != -1 {
		tmp := rl.logs[j+1:]
		rl.logs = []Entry{{Term: lastIncludedTerm, Index: lastIncludedIndex}} // dummy log
		rl.logs = append(rl.logs, tmp...)
	} else {
		rl.logs = []Entry{{Term: lastIncludedTerm, Index: lastIncludedIndex}} // dummy log
	}
}

func (rl *RaftLogs) findEntryPosByIndex(index int) int {
	// consider binary search
	firstLogIndex := rl.logs[0].Index
	i := index - firstLogIndex
	if i >= 0 && i < len(rl.logs) {
		return i
	} else {
		return -1
	}
}
