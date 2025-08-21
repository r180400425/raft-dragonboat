// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package raft is a distributed consensus package that implements the Raft
protocol.

This package is internally used by Dragonboat, applications are not expected
to import this package.

实现raft协议
不对外提供API
应用程序不应直接导入此包，而应通过Dragonboat的高层接口（如NodeHost）使用Raft功能。
*/
package raft

import (
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/errors"  // 增强型错误处理库，提供堆栈跟踪等功能
	"github.com/lni/goutils/logutil" // 日志格式化工具
	"github.com/lni/goutils/random"  // 随机数生成工具

	"github.com/lni/dragonboat/v4/config"            // Raft节点配置定义
	"github.com/lni/dragonboat/v4/internal/server"   // 内部服务器接口
	"github.com/lni/dragonboat/v4/internal/settings" // 内部配置参数
	"github.com/lni/dragonboat/v4/logger"            // 日志工具
	pb "github.com/lni/dragonboat/v4/raftpb"         // Raft协议相关的protobuf定义
)

// 日志
var (
	plog = logger.GetLogger("raft")
)

const (
	// NoLeader is the flag used to indcate that there is no leader or the leader is unknown.
	// 无领导者 或 领导者未知
	NoLeader uint64 = 0
	// NoNode is the flag used to indicate that the node id field is not set.
	// 未设置节点id
	NoNode uint64 = 0
	//无限制的阈值（使用math.MaxUint64实现）
	noLimit uint64 = math.MaxUint64
	//// numMessageTypes 是Raft协议支持的消息类型总数（固定为29种）
	numMessageTypes uint64 = 29
)

var (
	// emptyState 是空的Raft状态实例，用于默认值或初始化
	emptyState = pb.State{}
	// maxEntrySize 是Raft日志条目的最大允许大小，从settings.Soft读取配置
	maxEntrySize = settings.Soft.MaxEntrySize
	// inMemGcTimeout 是内存日志的垃圾回收超时时间，从settings.Soft读取配置
	inMemGcTimeout = settings.Soft.InMemGCTimeout
)

// 枚举模拟：Go 无原生枚举，通过 type State uint64 + iota 实现类似枚举功能，用于表示 Raft 节点状态（ follower/leader 等）。

// State is the state of a raft node defined in the raft thesis.
// 节点状态
type State uint64

// 状态定义
const (
	// follower 是节点的默认状态，节点在此状态下跟随Leader并接收日志复制
	follower State = iota // iota 从 0 开始自增，依次赋值给后续常量
	// candidate 是节点发起选举时的状态，正在竞选Leader
	candidate
	// preVoteCandidate 是节点处于PreVote阶段的状态（Raft论文9.7节），用于避免网络分区导致的频繁选举
	preVoteCandidate
	// leader 是节点作为Leader的状态，负责日志复制和心跳发送
	leader
	// nonVoting 是非投票节点的状态（Raft论文4.2.1节），不参与选举和投票，仅同步日志
	nonVoting
	// witness 是见证节点的状态（Raft论文11.7.2节），仅参与Quorum计数，不存储完整日志和状态机
	witness
	// numStates 是Raft节点状态的总数，用于状态数组边界检查
	numStates
)

// 字符串映射：stateNames 数组配合 String() 方法，实现枚举值的可读性转换（如 leader.String() → "Leader"）。

// 字符串映射：将枚举值转换为可读名称（如 "Leader"），用于日志和调试
var stateNames = [...]string{
	"Follower",         // follower状态的字符串表示
	"Candidate",        // candidate状态的字符串表示
	"PreVoteCandidate", // preVoteCandidate状态的字符串表示
	"Leader",           // leader状态的字符串表示
	"NonVoting",        // nonVoting状态的字符串表示
	"Witness",          // witness状态的字符串表示
}

// 值接收器方法（Value Receiver）
// (st State) 表示方法作用于 State 类型的副本，不修改原对象，常用于只读操作（如实现 Stringer 接口）。
// func是关键字，st是参数，State是参数类型，String是方法名，返回值是string类型
func (st State) String() string {
	return stateNames[uint64(st)]
}

// ReplicaID returns a human friendly form of ReplicaID for logging purposes.
func ReplicaID(replicaID uint64) string {
	return logutil.ReplicaID(replicaID)
}

// ShardID returns a human friendly form of ShardID for logging purposes.
func ShardID(shardID uint64) string {
	return logutil.ShardID(shardID)
}

// handlerFunc 是处理Raft消息的函数类型，入参为pb.Message（Raft协议消息），返回处理结果错误。
// 用于不同状态（如Leader/Follower）下的消息处理逻辑分发。
type handlerFunc func(pb.Message) error

// stepFunc 是Raft节点状态机的消息处理函数类型，入参为*raft（节点实例）和pb.Message（消息），返回处理结果错误。
// 用于定义节点在不同状态下对消息的处理流程。
type stepFunc func(*raft, pb.Message) error

// Status is the struct that captures the status of a raft node.
// Status 用于捕获Raft节点的【当前】状态信息，包含节点身份、角色、日志进度等核心状态数据。
type Status struct {
	ReplicaID uint64 // 节点在Raft分片中的唯一标识（非零）
	ShardID   uint64 // Raft组（分片）的唯一标识
	Applied   uint64 // 已应用到状态机的日志条目索引
	LeaderID  uint64 // 当前Leader节点的ReplicaID（NoLeader表示无Leader）
	NodeState State  // 节点当前状态（如Leader/Follower/Candidate等）
	pb.State         // 嵌入的Raft核心状态（包含Term、Vote、CommitIndex等，定义于raftpb）
}

// IsLeader returns a boolean value indicating whether the node is leader.
func (s *Status) IsLeader() bool {
	return s.NodeState == leader
}

// IsFollower returns a boolean value indicating whether the node is a follower.
func (s *Status) IsFollower() bool {
	return s.NodeState == follower
}

// getLocalStatus gets a copy of the current raft status.
// getLocalStatus 获取当前Raft节点的状态快照，返回封装后的Status实例。
// 用于外部（如监控、日志）获取节点运行时状态。
func getLocalStatus(r *raft) Status {
	return Status{
		ReplicaID: r.replicaID,     // 节点自身的ReplicaID
		ShardID:   r.shardID,       // 节点所属的ShardID
		NodeState: r.state,         // 节点当前状态（State类型）
		Applied:   r.log.processed, // 日志中已应用到状态机的条目索引
		LeaderID:  r.leaderID,      // 当前Leader的ReplicaID（可能为NoLeader）
		State:     r.raftState(),   // 嵌入Raft核心状态（Term、Vote、Commit等）
	}
}

//
// Struct raft implements the raft protocol published in Diego Ongarno's PhD
// thesis. Almost all features covered in Diego Ongarno's thesis have been
// implemented, including -
//  * leader election with pre-vote
//  * log replication
//  * flow control
//  * membership configuration change
//  * snapshotting and streaming
//  * log compaction
//  * ReadIndex protocol for read-only queries
//  * leadership transfer
//  * non-voting members
//  * witness members
//  * idempotent updates
//  * quorum check
//  * batching
//  * pipelining
//  * witness
//
// Features currently being worked on -
//  * pre-vote
//

//
// This implementation made references to etcd raft's design in the following
// aspects:
//  * it models the raft protocol state as a state machine
//  * restricting to at most one pending leadership change request at a time
//  * replication flow control
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//
// When compared with etcd raft, this implementation is quite different,
// including in areas that we made reference to etcd raft -
// * brand new implementation
// * better bootstrapping procedure
// * log entries are partitioned based on whether they are required in
//   immediate future rather than whether they have been persisted to disk
// * zero disk read when replicating raft log entries
// * committed entries are applied in a fully asynchronous manner
// * snapshots are applied in a fully asynchronous manner
// * replication messages can be asynchronously serialized and sent
// * pagination support when applying committed entries
// * making proposals are fully batched
// * ReadIndex protocol implementation are fully batched
// * unsafe read-only queries that rely on local clock is not supported
// * non-voting members are implemented as a special raft state
// * non-voting members can initiate both new proposal and ReadIndex requests
// * simplified flow control
//

//
// Struct raft 实现了 Diego Ongaro 博士论文中描述的 Raft 协议。几乎所有在 Diego Ongaro 论文中涉及的特性都已实现，包括 -
//  * 带预投票的领导者选举
//  * 日志复制
//  * 流量控制
//  * 成员配置变更
//  * 快照和流传输
//  * 日志压缩
//  * 用于只读查询的 ReadIndex 协议
//  * 领导权转移
//  * 非投票成员
//  * 见证成员
//  * 幂等更新
//  * 法定人数检查
//  * 批处理
//  * 流水线
//  * 见证者
//
// 目前正在开发的功能 -
//  * 预投票
//

//
// 此实现在以下方面参考了 etcd raft 的设计：
//  * 将 Raft 协议状态建模为状态机
//  * 限制同时最多只能有一个待处理的领导权变更请求
//  * 复制流量控制
//
// 版权所有 2015 etcd 作者
//
// 根据 Apache 许可证 2.0 版（"许可证"）授权；除非符合许可证要求，否则您不得使用此文件。
// 您可以获得许可证的副本于
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// 除非适用法律要求或书面同意，根据许可证分发的软件是基于"按原样"分发的，
// 不提供任何明示或暗示的担保或条件。有关管辖权限和限制的详细信息，请参见许可证。
//

//
// 与 etcd raft 相比，此实现有很大不同，
// 包括我们在参考 etcd raft 的领域 -
// * 全新的实现
// * 更好的引导程序
// * 日志条目根据它们是否在近期需要而不是是否已持久化到磁盘进行分区
// * 复制 Raft 日志条目时零磁盘读取
// * 提交的条目以完全异步的方式应用
// * 快照以完全异步的方式应用
// * 复制消息可以异步序列化和发送
// * 应用提交条目时支持分页
// * 提案完全批处理
// * ReadIndex 协议实现完全批处理
// * 不支持依赖本地时钟的不安全只读查询
// * 非投票成员作为特殊的 Raft 状态实现
// * 非投票成员可以发起新的提案和 ReadIndex 请求
// * 简化的流量控制
//


// dn 是 logutil.DescribeNode 的简写，用于生成节点描述字符串（如 "shardID:replicaID"）
var dn = logutil.DescribeNode

// raft 是 Raft 协议的核心实现结构体，封装了节点的状态、行为及与其他节点的交互逻辑。
// 每个 Raft 节点对应一个 raft 实例，负责日志复制、领导者选举、成员变更等核心功能。
type raft struct {
	
	// 索引为[状态][消息类型]，值为对应的处理函数
	// 例如：handlers[follower][pb.RequestVote] 指向 follower 状态下处理投票请求的函数。
	handlers [numStates][numMessageTypes]handlerFunc

	// Raft 事件监听器接口，用于向外部模块（如监控）发送事件通知（如领导者变更、日志复制失败）。
	events server.IRaftEventListener

	// 检查是否存在已提交但未应用的配置变更的函数。
	// 配置变更未应用时，节点会拒绝发起新选举，避免因成员集不一致导致的安全问题。
	hasNotAppliedConfigChange func() bool

	// 选举投票记录，键为节点 ID，值为是否投票给当前候选人（仅在 candidate/preVoteCandidate 状态有效）。
	votes map[uint64]bool

	// 当前状态下的消息处理入口函数（stepFunc 类型），根据节点状态动态切换。
	handle stepFunc

	// log 是 Raft 日志管理器，负责日志条目的存储、追加、压缩及快照管理。
	log *entryLog

	// 内存日志速率限制器，控制消息发送速率，防止未应用日志占用过多内存（通过 MaxInMemLogSize 配置）。
	rl *server.InMemRateLimiter

	// 投票节点（Voting Member）的复制进度映射，键为节点 ID，值为 remote 实例（记录匹配索引、下一个待发索引等）。
	remotes map[uint64]*remote

	// nonVotings 是非投票节点（Non-Voting Member）的复制进度映射，节点同步日志但不参与投票和选举。
	nonVotings map[uint64]*remote

	// witnesses 是见证节点（Witness Member）的复制进度映射，节点仅参与 quorum 计数，不存储完整日志和状态机。
	witnesses map[uint64]*remote

	// logQueryResult 存储日志查询结果（如调试时查询特定范围日志），仅在查询期间非 nil。
	logQueryResult *pb.LogQueryResult

	// leaderUpdate 存储领导者变更信息，用于通知其他模块（如 NodeHost）当前领导者 ID 和任期。
	leaderUpdate *pb.LeaderUpdate

	// readIndex 是 ReadIndex 协议的状态管理器，负责线性一致性读请求的跟踪和确认。
	readIndex *readIndex

	// matched 是投票节点的已匹配日志索引切片，用于计算多数派已提交的日志索引（commit index）。
	matched []uint64

	// msgs 是待发送的消息队列，存储当前周期内生成的 Raft 协议消息（如心跳、投票请求）。
	msgs []pb.Message

	// droppedReadIndexes 是被丢弃的 ReadIndex 请求上下文列表（如因领导者未准备就绪而丢弃）。
	droppedReadIndexes []pb.SystemCtx

	// droppedEntries 是被丢弃的提案条目列表（如因内存限制或领导者转移而丢弃）。
	droppedEntries []pb.Entry

	// readyToRead 是已确认可执行读操作的 ReadIndex 请求列表，包含提交索引和上下文信息。
	readyToRead []pb.ReadyToRead

	// prevLeader 存储上一任领导者的信息，用于检测领导者变更事件。
	prevLeader server.LeaderInfo

	// state 是当前节点的状态（follower/candidate/leader 等，State 枚举类型）。
	state State

	// leaderTransferTarget 是领导者转移的目标节点 ID（非 0 表示正在进行领导者转移）。
	leaderTransferTarget uint64

	// leaderID 是当前领导者的节点 ID（NoLeader 表示无领导者）。
	leaderID uint64

	// shardID 是当前 Raft 组（分片）的唯一标识。
	shardID uint64

	// replicaID 是当前节点在 Raft 组内的唯一标识。
	replicaID uint64

	// term 是当前任期号（Raft 协议核心字段，单调递增）。
	term uint64

	// applied 是已应用到状态机的日志条目索引（已提交日志中最新被状态机执行的索引）。
	applied uint64

	// vote 是当前任期内投票给的节点 ID（NoNode 表示未投票）。
	vote uint64

	// tickCount 是累计的逻辑时钟滴答数（每个滴答对应 NodeHostConfig.RTTMillisecond 毫秒）。
	tickCount uint64

	// electionTick 是距离上一次选举相关事件的滴答数（达到 electionTimeout 时触发选举）。
	electionTick uint64

	// heartbeatTick 是距离上一次心跳事件的滴答数（领导者用，达到 heartbeatTimeout 时发送心跳）。
	heartbeatTick uint64

	// heartbeatTimeout 是心跳超时阈值（单位：滴答数，来自 Config.HeartbeatRTT）。
	heartbeatTimeout uint64

	// electionTimeout 是选举超时阈值（单位：滴答数，来自 Config.ElectionRTT）。
	electionTimeout uint64

	// randomizedElectionTimeout 是随机化的选举超时（electionTimeout ~ 2*electionTimeout 之间，避免同时选举）。
	randomizedElectionTimeout uint64

	// snapshotting 标识是否正在生成快照（防止并发快照操作）。
	snapshotting bool

	// checkQuorum 标识是否启用领导者定期检查 quorum（来自 Config.CheckQuorum）。
	checkQuorum bool

	// quiesce 标识是否启用静默模式（无操作时停止发送心跳以节省带宽，实验性功能）。
	quiesce bool

	// isLeaderTransferTarget 标识当前节点是否是领导者转移的目标节点。
	isLeaderTransferTarget bool

	// pendingConfigChange 标识是否存在已提交但未应用的成员配置变更（防止配置变更期间发起选举）。
	pendingConfigChange bool

	// preVote 标识是否启用 PreVote 协议（来自 Config.PreVote，避免网络分区导致的频繁选举）。
	preVote bool
}

// 创建初始化Raft节点实例，是 raft 结构体的构造函数。
// 参数 c：节点配置
//
//	logdb：日志数据库接口，用于持久化存储Raft日志和状态
//
// 返回值： *raft：初始化完成的Raft节点实例
func newRaft(c config.Config, logdb ILogDB) *raft {
	// 1.验证配置的合法性
	if err := c.Validate(); err != nil {
		panic(err)
	}
	// 2.检查日志数据库是否为空，Raft节点必须依赖日志存储，故为空时panic
	if logdb == nil {
		panic("logdb is nil")
	}
	// 3.创建一个内存限速器，防止未应用日志占用过多内存（受 MaxInMemLogSize 配置限制）
	rl := server.NewInMemRateLimiter(c.MaxInMemLogSize)

	// 4.初始化raft结构体核心字段
	r := &raft{
		shardID:          c.ShardID,                // 当前 Raft 分片（组）的唯一标识
		replicaID:        c.ReplicaID,              // 当前节点在分片中的唯一标识
		leaderID:         NoLeader,                 // 初始无领导者
		msgs:             make([]pb.Message, 0),    // 待发送消息队列（初始化空切片）
		droppedEntries:   make([]pb.Entry, 0),      // 被丢弃的日志条目列表（初始化空切片）
		log:              newEntryLog(logdb, rl),   // 日志管理器（封装日志存储、压缩、快照等功能）
		remotes:          make(map[uint64]*remote), // 投票节点（Voting Member）的复制进度映射
		nonVotings:       make(map[uint64]*remote), // 非投票节点的复制进度映射
		witnesses:        make(map[uint64]*remote), // 见证节点的复制进度映射
		electionTimeout:  c.ElectionRTT,            // 选举超时阈值（单位：RTT 滴答数）
		heartbeatTimeout: c.HeartbeatRTT,           // 心跳超时阈值（单位：RTT 滴答数）
		checkQuorum:      c.CheckQuorum,            // 是否启用领导者定期检查 quorum（防止孤立领导者）
		preVote:          c.PreVote,                // 是否启用 PreVote 协议（避免网络分区导致的频繁选举）
		readIndex:        newReadIndex(),           // ReadIndex 协议状态管理器（处理线性一致性读）
		rl:               rl,                       // 内存日志速率限制器实例
	}

	// 5.记录速率限制器启动状态日志（用于调试与监控）
	plog.Infof("%s raft log rate limit enabled: %t, %d",
		dn(r.shardID, r.replicaID), r.rl.Enabled(), c.MaxInMemLogSize)

	// 6.从日志数据库中加载节点状态st和成员配置members
	st, members := logdb.NodeState()

	// 7.初始化复制进度跟踪（remote实例）
	// remote 结构体记录节点的日志匹配索引、下一个待发送索引等复制状态
	for p := range members.Addresses {
		r.remotes[p] = &remote{next: 1} // 投票节点：初始下一个待发索引为 1
	}
	for p := range members.NonVotings {
		r.nonVotings[p] = &remote{next: 1} // 非投票节点：初始下一个待发索引为 1
	}
	for p := range members.Witnesses {
		r.witnesses[p] = &remote{next: 1} // 见证节点：初始下一个待发索引为 1
	}

	// 8. 重置匹配值数组（用于计算多数派已提交日志索引，长度为投票节点数量）
	r.resetMatchValueArray()

	// 9. 若持久化状态不为空，加载任期、投票、提交索引等核心状态（从上次关闭处恢复）
	if !pb.IsEmptyState(st) {
		r.loadState(st)
	}

	// 10. 根据配置设置节点初始状态（Raft 协议核心状态机初始化）
	// 非投票节点/见证节点/普通节点的初始状态不同，影响选举与日志复制行为
	// Set node initial state.
	if c.IsNonVoting {
		r.state = nonVoting
		r.becomeNonVoting(r.term, NoLeader)
	} else if c.IsWitness {
		r.state = witness
		r.becomeWitness(r.term, NoLeader)
	} else {
		// see first paragraph section 5.2 of the raft paper
		r.becomeFollower(r.term, NoLeader)
	}

	// 11. 初始化状态-消息处理函数映射表（状态机核心：不同状态处理不同类型消息）
	r.initializeHandlerMap()
	// 12. 检查 handler 映射表完整性（确保所有状态-消息类型组合都有对应处理函数，避免运行时 panic）
	r.checkHandlerMap()
	// 13. 设置默认消息处理入口函数（根据节点状态动态调度消息处理逻辑）
	r.handle = defaultHandle

	// 返回初始化完成的 Raft 节点实例
	return r
}

// setTestPeers 用于测试场景，为当前 Raft 节点设置初始投票成员（Voting Members）。
// 仅当节点的投票成员映射（remotes）为空时生效，避免覆盖已有成员配置。
// 参数：
//   - peers: 测试环境中投票成员的节点 ID 列表
func (r *raft) setTestPeers(peers []uint64) {
	// 仅在 remotes 为空时初始化（防止测试中重复设置覆盖已有成员）
	if len(r.remotes) == 0 {
		// 遍历测试节点 ID 列表，初始化每个节点的复制进度跟踪（remote）
		for _, p := range peers {
			// remote.next=1 表示初始待发送日志索引为 1（日志从索引 1 开始）
			r.remotes[p] = &remote{next: 1}
		}
	}
}

// setApplied 设置已应用到状态机的日志条目索引（已提交日志中最新被状态机执行的索引）。
func (r *raft) setApplied(applied uint64) {
	r.applied = applied
}

// getApplied 返回已应用到状态机的日志条目索引（已提交日志中最新被状态机执行的索引）。
func (r *raft) getApplied() uint64 {
	return r.applied
}

// resetMatchValueArray 重新初始化 raft 节点的 matched 数组，这个数组用于存储所有投票成员的已匹配日志索引。。
// 该数组在计算已提交日志索引时使用，确保只有大多数节点都复制了的日志才会被提交。
//
// 工作原理：
// 1. 调用 r.numVotingMembers() 获取当前集群中投票成员的数量
// 2. 创建一个新的 uint64 切片，长度等于投票成员数量
// 3. 将新创建的切片赋值给 r.matched，替换旧的数组
//
// 注意事项：
// - 该方法只应在集群成员发生变化或节点启动时调用
// - matched 数组的长度必须与投票成员数量保持一致，否则会影响提交索引的计算
// - 数组中的每个元素初始值为 0，会在日志复制过程中逐步更新
func (r *raft) resetMatchValueArray() {
	// 创建一个新的切片，长度等于投票成员数量
	r.matched = make([]uint64, r.numVotingMembers())
}

// describe 生成当前 Raft 节点的状态描述字符串，用于日志和调试。
// 包含日志索引范围、当前任期、提交进度等核心状态信息，便于问题定位。
func (r *raft) describe() string {
	li := r.log.lastIndex()  // 获取日志最后一条条目的索引
	t, err := r.log.term(li) // 获取最后一条日志条目的任期号
	// 处理日志任期获取失败：仅忽略 "日志已压缩" 错误，其他错误触发 panic（数据不一致）
	if err != nil && !errors.Is(err, ErrCompacted) {
		plog.Panicf("%s failed to get term, %v", dn(r.shardID, r.replicaID), err)
	}
	// 格式化字符串模板：[f:首索引,l:尾索引,t:尾索引任期,c:已提交索引,a:已应用索引] 节点标识 t当前任期
	fmtstr := "[f:%d,l:%d,t:%d,c:%d,a:%d] %s t%d"
	return fmt.Sprintf(fmtstr,
		r.log.firstIndex(),  // 日志第一条条目的索引（日志范围起始）
		r.log.lastIndex(),   // 日志最后一条条目的索引（日志范围结束）
		t,                   // 最后一条日志的任期号
		r.log.committed,     // 已提交的日志索引（多数节点已复制）
		r.log.processed,     // 已应用到状态机的日志索引（已执行）
		dn(r.shardID, r.replicaID), // 节点标识（shardID:replicaID）
		r.term               // 当前任期号
	)
}

//判断节点类型
func (r *raft) isCandidate() bool {
	return r.state == candidate
}

func (r *raft) isLeader() bool {
	return r.state == leader
}

func (r *raft) isNonVoting() bool {
	return r.state == nonVoting
}

func (r *raft) isWitness() bool {
	return r.state == witness
}
//判断，用于确保只有领导者节点才能执行某些操作。
func (r *raft) mustBeLeader() {
	if !r.isLeader() {
		plog.Panicf("%s is not leader", r.describe())
	}
}

// 
// setLeaderID : 更新当前节点记录的领导者 ID，并触发领导者变更事件通知（若需要）。
// 该函数是 Raft 协议中领导者身份传播的关键入口，确保节点状态与集群领导者信息同步。
// 参数：
//   - leaderID: 新的领导者节点 ID（NoLeader 表示无领导者）
func (r *raft) setLeaderID(leaderID uint64) {
	// 1. 更新本地领导者 ID 记录
	r.leaderID = leaderID

	// 2. 创建领导者变更信息结构体，包含新领导者 ID 和当前任期
	r.leaderUpdate = &pb.LeaderUpdate{
		LeaderID: leaderID,//指定值
		Term:     r.term,
	}

	// 3. 若存在事件监听器，检查是否需要触发领导者变更事件
	if r.events != nil {
		// 触发条件：
		//   a. 初始状态（任期 0 且无领导者）
		//   b. 领导者 ID 发生变化
		//   c. 任期发生变化（即使领导者 ID 相同，任期变化也需通知）
		if (r.term == 0 && leaderID == NoLeader) ||
			leaderID != r.prevLeader.LeaderID || r.term != r.prevLeader.Term {
			// 构建领导者信息（包含分片 ID、当前节点 ID、领导者 ID、任期）
			info := server.LeaderInfo{ //使用短变量声明:=创建并初始化局部变量
				ShardID:   r.shardID,
				ReplicaID: r.replicaID,
				LeaderID:  leaderID,
				Term:      r.term,
			}
			// 更新上一任领导者信息（用于下次变更检测）
			r.prevLeader = info
			// 通知外部监听器（如监控模块）领导者已变更
			r.events.LeaderUpdated(info)
		}
	}
}

// 当前节点是否正在进行领导者转移
func (r *raft) leaderTransfering() bool {
	// 领导者转移是指当前领导者主动将领导权交给目标节点的过程，期间领导者会停止处理新请求并推动日志同步。
	// 1.设置了转移的目标节点 && 2.当前节点是领导者
	return r.leaderTransferTarget != NoNode && r.isLeader()
}
// 取消当前的领导者转移操作，将转移目标设置为 NoNode。
func (r *raft) abortLeaderTransfer() {
	r.leaderTransferTarget = NoNode
}
//计算具有投票权的成员总数
// Raft 协议中，投票成员包括：
//   - 普通投票节点（Voting Member，存储在 remotes 中）
//   - 见证节点（Witness Member，存储在 witnesses 中，仅参与法定人数计数，不存储完整日志）
// 返回值：
//   - int: 投票成员总数（remotes 长度 + witnesses 长度）
func (r *raft) numVotingMembers() int {
	return len(r.remotes) + len(r.witnesses)
}

//计算达成法定人数（多数派）的数目
func (r *raft) quorum() int {
	return r.numVotingMembers()/2 + 1
}

//判断是否为单节点器群（法定人数为1）
func (r *raft) isSingleNodeQuorum() bool {
	return r.quorum() == 1
}

// 判断领导者是否拥有法定人数的投票
func (r *raft) leaderHasQuorum() bool {
	c := 0 //计数器

	for nid, member := range r.votingMembers() {//遍历投票成员
		//如果节点是领导者自己，或者节点是活跃的，则计数器加1
		//领导者总认为自己活跃
		if nid == r.replicaID || member.isActive() {
			c++ //计数
			member.setNotActive() //设置成员非活动，防止重复计数
		}
	}
	return c >= r.quorum()
}

//所有节点的id列表 未排序
func (r *raft) nodes() []uint64 {
	// 预分配切片容量：投票成员数 + 非投票成员数（见证节点数量动态追加）
	nodes := make([]uint64, 0, r.numVotingMembers()+len(r.nonVotings))
	for id := range r.remotes { //遍历所有投票节点
		nodes = append(nodes, id)
	}
	for id := range r.nonVotings { //遍历所有非投票节点
		nodes = append(nodes, id)
	}
	for id := range r.witnesses { //遍历所有见证节点
		nodes = append(nodes, id)
	}
	return nodes 
}

// 获取所有节点的id列表，并升序排序
func (r *raft) nodesSorted() []uint64 {
	nodes := r.nodes()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

//返回所有具有投票权的成员的复制进度映射。
func (r *raft) votingMembers() map[uint64]*remote {
	// 预分配 map 容量为投票成员总数（避免动态扩容开销）
	// Raft 协议中，投票成员包括：
	//   - 普通投票节点（Voting Member，存储在 remotes 中，参与日志复制和选举）
	//   - 见证节点（Witness Member，存储在 witnesses 中，仅参与法定人数计数，不存储完整日志）
	nodes := make(map[uint64]*remote, r.numVotingMembers())
	for id, rm := range r.remotes {
		nodes[id] = rm
	}
	for id, wt := range r.witnesses {
		nodes[id] = wt
	}
	// 返回值：键为节点 ID，值为对应节点的复制进度跟踪实例（*remote）
	return nodes
}

// raftState 获取当前 Raft 节点的核心状态，包含协议关键元数据。
// 该状态需持久化存储（通过 logdb），用于节点重启后恢复状态。
// 返回值：pb.State 结构体，包含当前任期（Term）、投票对象（Vote）和已提交日志索引（Commit）
func (r *raft) raftState() pb.State {
	return pb.State{
		Term:   r.term,
		Vote:   r.vote,
		Commit: r.log.committed,
	}
}
// loadState 从持久化存储加载 Raft 核心状态，用于节点重启或状态恢复。
func (r *raft) loadState(st pb.State) {
	// 检查提交索引是否有效：必须 >= 当前已提交索引且 <= 日志最后索引
	if st.Commit < r.log.committed || st.Commit > r.log.lastIndex() {
		plog.Panicf("%s got out of range state, st.commit %d, range[%d,%d]",
			r.describe(), st.Commit, r.log.committed, r.log.lastIndex())
	}
	// 更新已提交日志索引（来自持久化状态）
	r.log.committed = st.Commit
	// 更新当前任期号
	r.term = st.Term
	// 更新当前任期的投票对象
	r.vote = st.Vote
}

/*

restore 函数：

核心定位：快照恢复的主入口，负责快照合法性验证（索引/任期/节点类型）和日志状态更新。
关键逻辑：分步骤解释“快照跳过条件”（索引过期）、“任期匹配检查”（避免重复恢复）、“日志状态覆盖”（快照内容替换本地日志）。
协议关联：引用 Raft 论文中快照索引与任期匹配的规则，说明为何任期不匹配时必须执行恢复。

*/

// restore 处理快照恢复逻辑，验证快照合法性并更新本地日志状态。
// 快照恢复是 Raft 协议中日志压缩的关键机制，用于快速同步节点状态（而非逐条复制历史日志）。

//   - ss: 待恢复的快照数据（包含索引、任期、成员配置等元信息）
func (r *raft) restore(ss pb.Snapshot) (bool, error) {
	// 1. 若快照索引 <= 已提交索引，快照已过时，无需恢复（避免覆盖更新的日志）
	if ss.Index <= r.log.committed {
		plog.Warningf("%s, restore aborted, ss.Index <= committed", r.describe())
		return false, nil
	}

	// 2. 验证节点类型一致性：非投票节点不能通过快照转为投票节点
	if !r.isNonVoting() {
		for nid := range ss.Membership.NonVotings {
			if nid == r.replicaID {
				plog.Panicf("%s converting to nonVoting, index %d, committed %d, %+v",
					r.describe(), ss.Index, r.log.committed, ss)
			}
		}
	}
	// 3. 验证节点类型一致性：非见证节点不能通过快照转为见证节点
	if !r.isWitness() {
		for nid := range ss.Membership.Witnesses {
			if nid == r.replicaID {
				plog.Panicf("%s converting to witness, index %d, committed %d, %+v",
					r.describe(), ss.Index, r.log.committed, ss)
			}
		}
	}
	// p52 of the raft thesis
	// 4. 检查快照与本地日志的一致性（Raft 论文 5.2 节）：匹配快照索引对应的任期
	// 若本地日志在快照索引处的任期与快照任期一致，说明快照内容已包含在本地日志中
	match, err := r.log.matchTerm(ss.Index, ss.Term)
	if err != nil {
		return false, err
	}
	// 5. 若任期匹配，直接提交到快照索引（无需恢复快照）
	if match {
		// a snapshot at index X implies that X has been committed
		// 快照索引 X 隐含 X 已被提交，更新本地提交索引
		r.log.commitTo(ss.Index)
		return false, nil
	}
	// 6. 任期不匹配，执行快照恢复：更新日志状态为快照内容
	plog.Infof("%s starts to restore snapshot index %d term %d",
		r.describe(), ss.Index, ss.Term)
	r.log.restore(ss)// 调用日志管理器执行快照恢复（覆盖本地日志状态）
	return true, nil // 返回 true 表示快照已成功恢复
}


/**
 * restoreRemotes 函数：

 核心定位：快照恢复的配套函数，确保节点对集群成员的复制进度跟踪与快照状态同步。
 成员类型区分：分别说明投票/非投票/见证节点的恢复逻辑，强调不同成员类型的复制进度初始化差异。
 状态修正：解释特殊场景处理（如节点自身状态修正、见证节点升级限制），避免恢复后出现状态不一致。
 */


// restoreRemotes 根据快照中的成员配置，恢复所有节点的复制进度跟踪状态。
// 快照恢复后，节点需重新初始化对集群成员的复制进度（match/next 索引），确保日志复制从快照后的日志开始。
// 参数：
//   - ss: 已恢复的快照数据（包含最新的成员配置信息）
func (r *raft) restoreRemotes(ss pb.Snapshot) {
	// -------------------------- 恢复投票节点（Voting Members）--------------------------
	r.remotes = make(map[uint64]*remote) // 重置投票节点复制进度映射
	for id := range ss.Membership.Addresses { // 遍历快照中的投票成员
		// 若当前节点是投票成员但自身状态为非投票节点，修正为跟随者状态
		if id == r.replicaID && r.isNonVoting() {
			r.becomeFollower(r.term, r.leaderID)
		}
		// 见证节点不能直接升级为投票节点（需通过配置变更流程）
		if _, ok := r.witnesses[id]; ok {
			plog.Panicf("Assumed witness could not promote to full member")
		}
		// 初始化复制进度：match 为 0（未匹配），next 为快照后第一条日志索引
		match := uint64(0)
		next := r.log.lastIndex() + 1 // 快照最后索引 + 1 = 下一条待复制日志索引
		// 若当前节点是该成员，match 设为 next-1（自身日志已匹配到快照最后索引）
		if id == r.replicaID {
			match = next - 1
		}
		r.setRemote(id, match, next) // 更新投票节点复制进度
		plog.Debugf("%s restored remote progress of %s [%s]",
			r.describe(), ReplicaID(id), r.remotes[id])
	}
	// 若当前节点已从投票成员中移除且自身是领导者，主动退化为跟随者
	if r.selfRemoved() && r.isLeader() {
		r.becomeFollower(r.term, NoLeader)
	}

	// -------------------------- 恢复非投票节点（Non-Voting Members）--------------------------
	r.nonVotings = make(map[uint64]*remote) // 重置非投票节点复制进度映射
	for id := range ss.Membership.NonVotings { // 遍历快照中的非投票成员
		// 初始化复制进度：逻辑同投票节点
		match := uint64(0)
		next := r.log.lastIndex() + 1
		if id == r.replicaID {
			match = next - 1
		}
		r.setNonVoting(id, match, next) // 更新非投票节点复制进度
		plog.Debugf("%s restored nonVoting progress of %s [%s]",
			r.describe(), ReplicaID(id), r.nonVotings[id])
	}

	// -------------------------- 恢复见证节点（Witness Members）--------------------------
	r.witnesses = make(map[uint64]*remote) // 重置见证节点复制进度映射
	for id := range ss.Membership.Witnesses { // 遍历快照中的见证成员
		// 初始化复制进度：逻辑同投票节点
		match := uint64(0)
		next := r.log.lastIndex() + 1
		if id == r.replicaID {
			match = next - 1
		}
		r.setWitness(id, match, next) // 更新见证节点复制进度
		plog.Debugf("%s restored witness progress of %s [%s]",
			r.describe(), ReplicaID(id), r.witnesses[id])
	}

	// 重置匹配值数组（长度需与新的投票成员数量一致，确保提交索引计算正确）
	r.resetMatchValueArray()
}

//
// tick related functions
//

// -------------------------- 定时任务触发条件判断 --------------------------

// timeForElection 判断是否达到选举超时时间。
// 当节点处于跟随者/候选者状态且未收到领导者心跳时，选举计时器会累积，达到阈值后触发新选举。
// 返回值：
//   - bool: 若选举计时器 >= 随机化选举超时阈值，则返回 true（触发选举）；否则返回 false
func (r *raft) timeForElection() bool {
	return r.electionTick >= r.randomizedElectionTimeout
}


// timeForHeartbeat 判断是否达到心跳发送时间。
// 领导者通过定期发送心跳维持领导权，心跳超时阈值通常远小于选举超时（如 1/10 选举超时）。
// 返回值：
//   - bool: 若心跳计时器 >= 心跳超时阈值，则返回 true（触发心跳发送）；否则返回 false
func (r *raft) timeForHeartbeat() bool {
	return r.heartbeatTick >= r.heartbeatTimeout
}

// p69 of the raft thesis mentions that check quorum is performed when an
// election timeout elapses
// timeForCheckQuorum 判断是否需要执行领导者定期 quorum 检查。
// 参考 Raft 论文 6.3 节，领导者需定期确认多数派节点仍存活，避免"孤立领导者"继续提交日志。
// 触发时机：选举计时器达到选举超时阈值（与选举触发周期一致）。
// 返回值：
//   - bool: 若选举计时器 >= 选举超时阈值，则返回 true（触发 quorum 检查）；否则返回 false
func (r *raft) timeForCheckQuorum() bool {
	return r.electionTick >= r.electionTimeout
}

// p29 of the raft thesis mentions that leadership transfer should abort
// when an election timeout elapses
// timeToAbortLeaderTransfer 判断是否需要中止领导者转移。
// 参考 Raft 论文 3.10 节，领导者转移过程中若超过选举超时未完成，需主动中止以避免集群不可用。
// 返回值：
//   - bool: 若正在进行领导者转移且选举计时器 >= 选举超时阈值，则返回 true（中止转移）；否则返回 false
func (r *raft) timeToAbortLeaderTransfer() bool {
	return r.leaderTransfering() && r.electionTick >= r.electionTimeout
}

// timeForRateLimitCheck 判断是否需要执行内存日志速率限制检查。
// 定期检查未应用日志的内存占用，避免节点因日志积压导致内存溢出。
// 触发时机：当前滴答数是选举超时阈值的整数倍（周期性检查，频率与选举周期一致）。
// 返回值：
//   - bool: 若滴答数 % 选举超时 == 0，则返回 true（触发速率检查）；否则返回 false
func (r *raft) timeForRateLimitCheck() bool {
	return r.tickCount%r.electionTimeout == 0
}

// timeForInMemGC 判断是否需要执行内存日志垃圾回收。
// 清理已持久化到磁盘的日志数据，释放内存空间（由 settings.Soft.InMemGCTimeout 控制周期）。
// 返回值：
//   - bool: 若滴答数 % 内存 GC 超时 == 0，则返回 true（触发 GC）；否则返回 false
func (r *raft) timeForInMemGC() bool {
	return r.tickCount%inMemGcTimeout == 0
}

// -------------------------- 核心定时任务调度 --------------------------

// tick 是 Raft 节点的核心定时任务入口，每个逻辑时钟滴答（对应配置的 RTT 时间）调用一次。
// 根据节点状态（领导者/非领导者）分发到不同的处理逻辑，并执行周期性维护任务（如内存 GC）。
// 返回值：
//   - error: 任务执行过程中遇到的错误（如日志操作失败）
func (r *raft) tick() error {
	r.quiesce = false       // 退出静默模式（若之前处于静默）
	r.tickCount++           // 全局滴答计数器自增
	// this is to work around the language limitation described in
	// https://github.com/golang/go/issues/9618
	// 处理内存日志 GC（周期性触发，避免频繁执行）
	// 注：此处规避 Go 语言限制（https://github.com/golang/go/issues/9618），通过函数调用隔离闭包逻辑
	if r.timeForInMemGC() {
		r.log.inmem.tryResize() // 尝试收缩内存日志缓冲区
	}

	
	// 根据节点状态分发到领导者/非领导者逻辑
	if r.isLeader() {
		return r.leaderTick()   // 领导者节点处理逻辑
	}
	return r.nonLeaderTick()  // 跟随者/候选者节点处理逻辑
}

// nonLeaderTick 是非领导者节点（跟随者/候选者）的定时任务处理逻辑。
// 负责选举计时器管理、速率限制检查及选举触发。
// 返回值：
//   - error: 任务执行过程中遇到的错误（如消息处理失败）
func (r *raft) nonLeaderTick() error {
	// 防御性检查：确保领导者节点不会调用此函数
	if r.isLeader() {
		panic("noleader tick called on leader node")
	}

	r.electionTick++  // 选举计时器自增（未收到心跳时累积，触发选举）

	// 周期性执行内存日志速率限制检查（若启用）
	if r.timeForRateLimitCheck() {
		if r.rl.Enabled() {
			r.rl.Tick()               // 更新速率限制器状态
			r.sendRateLimitMessage()  // 向领导者发送速率限制状态（如内存占用过高）
		}
	}
	// section 4.2.1 of the raft thesis
	// non-voting member or witness will not participate in election
	// Raft 论文 4.2.1 节：非投票节点/见证节点不参与选举
	if r.isNonVoting() || r.isWitness() {
		return nil
	}

	// Raft 论文 5.2 节第 6 段：若节点未被移除且选举超时，触发新选举
	// selfRemoved: 节点已被移出集群成员配置，不应发起选举
	if !r.selfRemoved() && r.timeForElection() {
		r.electionTick = 0  // 重置选举计时器（避免重复触发）
		// 发送本地选举消息，触发选举流程
		if err := r.Handle(pb.Message{
			From: r.replicaID,  // 消息发送者为当前节点
			Type: pb.Election,  // 消息类型：发起选举
		}); err != nil {
			return err
		}
	}
	return nil
}

/*
函数定位：明确 leaderTick 是领导者节点的核心定时任务调度器，周期性处理协议要求的领导者职责（如心跳维持、集群健康检查）。
关键步骤解析：
法定人数检查：引用 Raft 论文 6.3 节，说明其作用是避免“孤立领导者”（与集群多数节点失联后仍提交日志）。
心跳发送：解释心跳的核心作用——通过定期广播维持领导权，防止跟随者触发新选举。
领导者转移中止：关联 Raft 论文 3.10 节，说明选举超时后中止转移的必要性（避免转移停滞导致集群不可用）。
错误处理：强调消息处理失败（如 Handle 调用返回错误）会直接向上传播，确保异常被及时捕获。
注释结合代码逻辑与 Raft 协议规范，既解释“做什么”，也说明“为什么需要做”，帮助理解领导者节点的周期性行为设计。
*/
// leaderTick 是领导者节点的定时任务处理逻辑，每个 RTT 滴答调用一次。
// 负责执行领导者特有的周期性任务：法定人数检查、心跳发送、领导者转移中止及速率限制维护。
// 返回值：
//   - error: 任务执行过程中遇到的错误（如消息处理失败）
func (r *raft) leaderTick() error {
	// 防御性检查：确保当前节点是领导者（避免非领导者调用）
	r.mustBeLeader()
	
	// 1. 选举计时器自增（用于触发法定人数检查和领导者转移中止）
	r.electionTick++

	// 2. 周期性执行内存日志速率限制检查（若启用）
	// 检查周期由 timeForRateLimitCheck 控制（通常与选举超时周期一致）
	if r.timeForRateLimitCheck() {
		if r.rl.Enabled() {
			r.rl.Tick() // 更新速率限制器状态（如内存日志占用统计）
		}
	}

	// 3. 检查是否需要中止领导者转移（选举超时未完成转移时触发）
	timeToAbortLeaderTransfer := r.timeToAbortLeaderTransfer()

	// 4. 法定人数检查（Raft 论文 6.3 节）：定期确认多数派节点存活
	if r.timeForCheckQuorum() {
		r.electionTick = 0 // 重置选举计时器（避免重复触发检查）
		// 仅在启用 CheckQuorum 配置时执行检查
		if r.checkQuorum {
			// 发送本地 CheckQuorum 消息，触发法定人数验证流程
			if err := r.Handle(pb.Message{
				From: r.replicaID, // 消息发送者为当前领导者节点
				Type: pb.CheckQuorum, // 消息类型：触发法定人数检查
			}); err != nil {
				return err // 消息处理失败时返回错误
			}
		}
	}

	// 5. 若需中止领导者转移，重置转移目标（放弃转移）
	if timeToAbortLeaderTransfer {
		r.abortLeaderTransfer()
	}

	// 6. 心跳计时器自增（用于触发心跳发送）
	r.heartbeatTick++

	// 7. 发送领导者心跳（维持领导权，通知其他节点领导者存活）
	if r.timeForHeartbeat() {
		r.heartbeatTick = 0 // 重置心跳计时器（避免重复发送）
		// 发送本地 LeaderHeartbeat 消息，触发心跳广播
		if err := r.Handle(pb.Message{
			From: r.replicaID, // 消息发送者为当前领导者节点
			Type: pb.LeaderHeartbeat, // 消息类型：领导者心跳
		}); err != nil {
			return err // 消息处理失败时返回错误
		}
	}

	// 8. 检查待处理的快照确认（确保快照已成功发送给跟随者）
	return r.checkPendingSnapshotAck()
}


// quiescedTick 处理节点在静默模式（quiesce mode）下的定时任务。
// 静默模式是一种优化，当集群无操作时停止发送心跳以节省带宽。
// 触发条件：节点无日志复制、无提案且无领导者转移时自动进入。
func (r *raft) quiescedTick() {
	// 若未启用静默模式，则启用并调整内存日志大小（收缩以节省内存）
	if !r.quiesce {
		r.quiesce = true
		r.log.inmem.resize() // 收缩内存日志缓冲区
	}
	r.electionTick++ // 选举计时器仍需递增（防止静默期间错过选举超时）
}

// setRandomizedElectionTimeout 设置随机化选举超时时间，避免集群节点同时触发选举导致投票分裂。
// Raft 协议通过随机化超时实现：每个节点超时时间 = 基础选举超时 + [0, 基础选举超时) 随机值。
func (r *raft) setRandomizedElectionTimeout() {
	// 生成 [0, electionTimeout) 范围内的随机值
	randTime := random.LockGuardedRand.Uint64() % r.electionTimeout
	// 随机化超时 = 基础超时 + 随机值（确保超时时间在 [electionTimeout, 2*electionTimeout) 之间）
	r.randomizedElectionTimeout = r.electionTimeout + randTime
}

//
// send and broadcast functions
//

// finalizeMessageTerm 验证并设置消息的任期（Term），确保消息符合 Raft 协议的任期规则。
// 不同类型消息的任期处理逻辑不同（如投票请求需显式设置任期，心跳消息使用当前任期）。
// 参数：
//   - m: 待处理的消息
// 返回值：处理后的消息（已设置正确任期）
func (r *raft) finalizeMessageTerm(m pb.Message) pb.Message {
	// 1. 投票请求（RequestVote）必须携带有效的任期，否则 panic
	if m.Term == 0 && m.Type == pb.RequestVote {
		plog.Panicf("%s sending RequestVote with 0 term", r.describe())
	}
	// 2. 非投票请求消息不应提前设置任期，否则 panic（防止逻辑错误）
	if m.Term > 0 &&
		!isRequestVoteMessage(m.Type) && m.Type != pb.RequestPreVoteResp {
		plog.Panicf("%s term unexpectedly set for message type %d",
			r.describe(), m.Type)
	}
	// 3. 非请求类消息（如心跳、复制响应）使用当前节点任期
	if !isRequestMessage(m.Type) &&
		!isRequestVoteMessage(m.Type) && m.Type != pb.RequestPreVoteResp {
		m.Term = r.term
	}
	return m
}

// send 将消息加入待发送队列，完成消息的最终处理（设置发送者、任期）。
// 是所有 Raft 协议消息对外发送的统一入口。
// 参数：
//   - m: 待发送的消息（需指定接收者 To 和消息类型 Type）
func (r *raft) send(m pb.Message) {
	m.From = r.replicaID // 设置消息发送者为当前节点 ID
	m = r.finalizeMessageTerm(m) // 验证并设置消息任期
	r.msgs = append(r.msgs, m) // 将消息加入发送队列（由上层模块实际发送）
}

// sendRateLimitMessage 向领导者发送速率限制状态消息（仅非领导者节点调用）。
// 当节点因内存日志占用过高触发速率限制时，通知领导者暂停或减缓日志复制。
func (r *raft) sendRateLimitMessage() {
	// 防御性检查：领导者节点不应调用此函数
	if r.isLeader() {
		plog.Panicf("leader node called sendRateLimitMessage")
	}
	// 无领导者时跳过发送
	if r.leaderID == NoLeader {
		plog.Infof("%s rate limit message skipped, no leader", r.describe())
		return
	}
	// 未启用速率限制时跳过
	if !r.rl.Enabled() {
		return
	}

	// 计算内存使用提示：仅统计已提交但未应用的日志占用（需领导者优先同步此类日志）
	mv := uint64(0)
	if r.rl.RateLimited() {
		inmemSz := r.rl.Get() // 当前内存日志总占用
		notCommitedSz := getEntrySliceSize(r.log.getUncommittedEntries()) // 未提交日志占用
		mv = max(inmemSz-notCommitedSz, 0) // 已提交未应用日志占用 = 总占用 - 未提交占用
	}

	// 发送速率限制消息给领导者
	r.send(pb.Message{
		Type: pb.RateLimit, // 消息类型：速率限制通知
		To:   r.leaderID,   // 接收者：当前领导者
		Hint: mv,           // 内存使用提示（已提交未应用日志大小）
	})
}

// makeInstallSnapshotMessage 创建 InstallSnapshot 消息，用于向目标节点发送快照。
// 快照包含集群状态的完整备份，用于快速同步落后节点（避免逐条复制历史日志）。
// 参数：
//   - to: 目标节点 ID
//   - m: 待填充的消息结构体（输出参数）
// 返回值：
//   - uint64: 快照的日志索引
func (r *raft) makeInstallSnapshotMessage(to uint64, m *pb.Message) uint64 {
	m.To = to // 设置目标节点
	m.Type = pb.InstallSnapshot // 消息类型：安装快照
	snapshot := r.log.snapshot() // 获取当前节点的最新快照

	// 防御性检查：快照不能为空
	if pb.IsEmptySnapshot(snapshot) {
		plog.Panicf("%s got an empty snapshot", r.describe())
	}

	// 见证节点（Witness）仅需元数据快照（不含实际日志数据，节省带宽）
	if _, ok := r.witnesses[to]; ok {
		snapshot = makeWitnessSnapshot(snapshot)
	}

	m.Snapshot = snapshot // 填充快照数据
	return snapshot.Index // 返回快照的日志索引
}

// makeWitnessSnapshot 将完整快照转换为见证节点专用的元数据快照。
// 见证节点不存储完整日志和快照数据，仅需元数据（索引、任期、成员配置）参与法定人数计算。
// 参数：
//   - snapshot: 完整快照
// 返回值：
//   - pb.Snapshot: 仅含元数据的见证节点快照
func makeWitnessSnapshot(snapshot pb.Snapshot) pb.Snapshot {
	result := snapshot
	// 清除文件路径、大小和内容（见证节点无需存储实际快照文件）
	result.Filepath = ""
	result.FileSize = 0
	result.Files = nil
	result.Witness = true // 标记为见证节点快照
	result.Dummy = false
	return result
}

// makeReplicateMessage 创建 Replicate 消息，包含待复制的日志条目，用于领导者向跟随者同步日志。
// 根据目标节点类型（普通节点/见证节点）调整日志内容（见证节点仅需元数据条目）。
// 参数：
//   - to: 目标节点 ID
//   - next: 目标节点的下一条待复制日志索引（即从该索引开始发送日志）
//   - maxSize: 消息最大允许大小（防止单条消息过大）
// 返回值：
//   - pb.Message: 构造的 Replicate 消息
//   - error: 日志获取失败（如日志已压缩）
func (r *raft) makeReplicateMessage(to uint64,
	next uint64, maxSize uint64) (pb.Message, error) {
	// 获取 next-1 索引处日志的任期（用于日志一致性检查）
	term, err := r.log.term(next - 1)
	if err != nil {
		return pb.Message{}, err
	}
	// 获取从 next 开始的日志条目（不超过 maxSize 大小）
	entries, err := r.log.entries(next, maxSize)
	if err != nil {
		return pb.Message{}, err
	}

	// 验证日志条目连续性（防止日志损坏导致索引跳变）
	if len(entries) > 0 {
		lastIndex := entries[len(entries)-1].Index
		expected := next - 1 + uint64(len(entries)) // 预期最后索引 = next-1 + 条目数
		if lastIndex != expected {
			plog.Panicf("%s expected last index in Replicate %d, got %d",
				r.describe(), expected, lastIndex)
		}
	}

	// 见证节点仅需元数据条目（不含命令数据，除非是配置变更条目）
	if _, ok := r.witnesses[to]; ok {
		entries = makeMetadataEntries(entries)
	}

	// 构造并返回 Replicate 消息
	return pb.Message{
		To:       to,        // 目标节点
		Type:     pb.Replicate, // 消息类型：日志复制
		LogIndex: next - 1,  // 基准索引（目标节点已复制到该索引）
		LogTerm:  term,      // 基准索引对应的任期（用于一致性检查）
		Entries:  entries,   // 待复制的日志条目
		Commit:   r.log.committed, // 领导者当前的已提交索引（通知跟随者更新提交进度）
	}, nil
}

// makeMetadataEntries 将普通日志条目转换为元数据条目（仅保留索引、任期和类型，移除命令数据）。
// 用于向见证节点发送日志（见证节点无需执行命令，仅需元数据参与法定人数计算）。
// 参数：
//   - entries: 普通日志条目列表
// 返回值：
//   - []pb.Entry: 元数据条目列表（配置变更条目保留完整数据）
func makeMetadataEntries(entries []pb.Entry) []pb.Entry {
	me := make([]pb.Entry, 0, len(entries))
	for _, ent := range entries {
		if ent.Type != pb.ConfigChangeEntry {
			// 非配置变更条目：仅保留元数据（索引、任期、类型）
			me = append(me, pb.Entry{
				Type:  pb.MetadataEntry, // 标记为元数据条目
				Index: ent.Index,
				Term:  ent.Term,
			})
		} else {
			// 配置变更条目：保留完整数据（见证节点需知晓成员配置）
			me = append(me, ent)
		}
	}
	return me
}

// sendReplicateMessage 向目标节点发送 Replicate 消息（日志复制），根据节点复制进度动态调整内容。
// 若目标节点日志已压缩（无法获取历史条目），则触发快照发送。
// 参数：
//   - to: 目标节点 ID
func (r *raft) sendReplicateMessage(to uint64) {
	// 获取目标节点的复制进度跟踪实例（remote）
	var rp *remote
	if v, ok := r.remotes[to]; ok {
		rp = v // 投票节点
	} else if v, ok := r.nonVotings[to]; ok {
		rp = v // 非投票节点
	} else {
		rp, ok = r.witnesses[to] // 见证节点
		if !ok {
			plog.Panicf("%s failed to get the remote instance", r.describe())
		}
	}

	// 若节点复制进度暂停（如快照发送中），跳过本次复制
	if rp.isPaused() {
		return
	}

	// 尝试创建 Replicate 消息（包含待复制日志条目）
	m, err := r.makeReplicateMessage(to, rp.next, maxEntrySize)
	if err != nil {
		// 日志已压缩导致无法获取条目，需发送快照
		if !rp.isActive() {
			plog.Warningf("%s, %s is not active, sending snapshot is skipped",
				r.describe(), ReplicaID(to))
			return
		}
		// 创建并发送快照消息
		index := r.makeInstallSnapshotMessage(to, &m)
		plog.Infof("%s is sending snapshot (%d) to %s, r.Next %d, r.Match %d, %v",
			r.describe(), index, ReplicaID(to), rp.next, rp.match, err)
		rp.becomeSnapshot(index) // 标记节点为快照发送中状态
	} else if len(m.Entries) > 0 {
		// 更新节点复制进度（已发送的最后一条日志索引）
		lastIndex := m.Entries[len(m.Entries)-1].Index
		rp.progress(lastIndex)
	}

	// 发送消息（Replicate 或 InstallSnapshot）
	r.send(m)
}

// broadcastReplicateMessage 向集群所有节点广播 Replicate 消息（领导者定期同步日志）。
// 遍历所有节点（投票/非投票/见证），分别发送日志复制消息。
func (r *raft) broadcastReplicateMessage() {
	r.mustBeLeader() // 仅领导者可广播复制消息

	// 非投票节点不应广播复制消息（防御性检查）
	for nid := range r.nonVotings {
		if nid == r.replicaID {
			plog.Panicf("%s nonVoting is broadcasting Replicate msg", r.describe())
		}
	}

	// 向所有节点发送复制消息（排除自身）
	for _, nid := range r.nodes() {
		if nid != r.replicaID {
			r.sendReplicateMessage(nid)
		}
	}
}

// sendHeartbeatMessage 向目标节点发送 Heartbeat 消息（领导者维持领导权的心跳）。
// 心跳消息包含领导者的已提交索引，用于通知跟随者更新提交进度。
// 参数：
//   - to: 目标节点 ID
//   - hint: 系统上下文低 64 位（如 ReadIndex 请求 ID）
//   - match: 目标节点的已匹配日志索引（用于优化提交索引计算）
func (r *raft) sendHeartbeatMessage(to uint64,
	hint pb.SystemCtx, match uint64) {
	// 提交索引取目标节点已匹配索引与领导者已提交索引的最小值（确保安全性）
	commit := min(match, r.log.committed)
	r.send(pb.Message{
		To:       to,        // 目标节点
		Type:     pb.Heartbeat, // 消息类型：心跳
		Commit:   commit,    // 领导者建议的提交索引
		Hint:     hint.Low,  // 系统上下文低 64 位
		HintHigh: hint.High, // 系统上下文高 64 位
	})
}

// broadcastHeartbeatMessage 向集群所有投票成员广播心跳消息（领导者定期发送，默认每秒几次）。
// 若存在未完成的 ReadIndex 请求，心跳消息会携带 ReadIndex 上下文，用于线性一致性读确认。
// 参考 Raft 论文 6.4 节：ReadIndex 协议通过心跳消息传播已提交索引，确保读操作线性一致性。
func (r *raft) broadcastHeartbeatMessage() {
	r.mustBeLeader() // 仅领导者可广播心跳
	if r.readIndex.hasPendingRequest() {
		// 存在未完成的 ReadIndex 请求，携带上下文（用于确认读操作安全性）
		ctx := r.readIndex.peepCtx()
		r.broadcastHeartbeatMessageWithHint(ctx)
	} else {
		// 无 ReadIndex 请求，发送普通心跳
		r.broadcastHeartbeatMessageWithHint(pb.SystemCtx{})
	}
}

// broadcastHeartbeatMessageWithHint 向集群节点广播携带特定上下文的心跳消息。
// 用于 ReadIndex 协议：通过心跳传播 ReadIndex 上下文，收集集群多数派已提交索引的确认。
// 参数：
//   - ctx: 系统上下文（如 ReadIndex 请求 ID）
func (r *raft) broadcastHeartbeatMessageWithHint(ctx pb.SystemCtx) {
	zeroCtx := pb.SystemCtx{}
	// 向所有投票成员发送心跳（携带上下文，用于 ReadIndex 确认）
	for id, rm := range r.votingMembers() {
		if id != r.replicaID {
			r.sendHeartbeatMessage(id, ctx, rm.match)
		}
	}
	// 仅当上下文为空时，向非投票成员发送心跳（非投票成员不参与 ReadIndex 确认）
	if ctx == zeroCtx {
		for id, rm := range r.nonVotings {
			r.sendHeartbeatMessage(id, zeroCtx, rm.match)
		}
	}
}

// sendTimeoutNowMessage 发送 TimeoutNow 消息，触发目标节点立即发起选举（用于领导者转移）。
// Raft 领导者转移协议通过此消息通知目标节点提前超时并竞选领导者。
// 参数：
//   - replicaID: 目标节点 ID（期望成为新领导者的节点）
func (r *raft) sendTimeoutNowMessage(replicaID uint64) {
	r.send(pb.Message{
		Type: pb.TimeoutNow, // 消息类型：立即超时
		To:   replicaID,     // 目标节点
	})
}



func (r *raft) quiescedTick() {
	if !r.quiesce {
		r.quiesce = true
		r.log.inmem.resize()
	}
	r.electionTick++
}

func (r *raft) setRandomizedElectionTimeout() {
	randTime := random.LockGuardedRand.Uint64() % r.electionTimeout
	r.randomizedElectionTimeout = r.electionTimeout + randTime
}

//
// send and broadcast functions
//

func (r *raft) finalizeMessageTerm(m pb.Message) pb.Message {
	if m.Term == 0 && m.Type == pb.RequestVote {
		plog.Panicf("%s sending RequestVote with 0 term", r.describe())
	}
	if m.Term > 0 &&
		!isRequestVoteMessage(m.Type) && m.Type != pb.RequestPreVoteResp {
		plog.Panicf("%s term unexpectedly set for message type %d",
			r.describe(), m.Type)
	}
	if !isRequestMessage(m.Type) &&
		!isRequestVoteMessage(m.Type) && m.Type != pb.RequestPreVoteResp {
		m.Term = r.term
	}
	return m
}

func (r *raft) send(m pb.Message) {
	m.From = r.replicaID
	m = r.finalizeMessageTerm(m)
	r.msgs = append(r.msgs, m)
}

func (r *raft) sendRateLimitMessage() {
	if r.isLeader() {
		plog.Panicf("leader node called sendRateLimitMessage")
	}
	if r.leaderID == NoLeader {
		plog.Infof("%s rate limit message skipped, no leader", r.describe())
		return
	}
	if !r.rl.Enabled() {
		return
	}
	mv := uint64(0)
	if r.rl.RateLimited() {
		inmemSz := r.rl.Get()
		notCommitedSz := getEntrySliceSize(r.log.getUncommittedEntries())
		mv = max(inmemSz-notCommitedSz, 0)
	}
	r.send(pb.Message{
		Type: pb.RateLimit,
		To:   r.leaderID,
		Hint: mv,
	})
}

func (r *raft) makeInstallSnapshotMessage(to uint64, m *pb.Message) uint64 {
	m.To = to
	m.Type = pb.InstallSnapshot
	snapshot := r.log.snapshot()
	if pb.IsEmptySnapshot(snapshot) {
		plog.Panicf("%s got an empty snapshot", r.describe())
	}
	// For witness, snapshot message will be marked as dummy snapshot.
	if _, ok := r.witnesses[to]; ok {
		snapshot = makeWitnessSnapshot(snapshot)
	}
	m.Snapshot = snapshot
	return snapshot.Index
}

func makeWitnessSnapshot(snapshot pb.Snapshot) pb.Snapshot {
	result := snapshot
	result.Filepath = ""
	result.FileSize = 0
	result.Files = nil
	result.Witness = true
	result.Dummy = false
	return result
}

func (r *raft) makeReplicateMessage(to uint64,
	next uint64, maxSize uint64) (pb.Message, error) {
	term, err := r.log.term(next - 1)
	if err != nil {
		return pb.Message{}, err
	}
	entries, err := r.log.entries(next, maxSize)
	if err != nil {
		return pb.Message{}, err
	}
	if len(entries) > 0 {
		lastIndex := entries[len(entries)-1].Index
		expected := next - 1 + uint64(len(entries))
		if lastIndex != expected {
			plog.Panicf("%s expected last index in Replicate %d, got %d",
				r.describe(), expected, lastIndex)
		}
	}
	// Don't send actual log entry to witness as they won't replicate real message,
	// unless there is a config change.
	if _, ok := r.witnesses[to]; ok {
		entries = makeMetadataEntries(entries)
	}
	return pb.Message{
		To:       to,
		Type:     pb.Replicate,
		LogIndex: next - 1,
		LogTerm:  term,
		Entries:  entries,
		Commit:   r.log.committed,
	}, nil
}

func makeMetadataEntries(entries []pb.Entry) []pb.Entry {
	me := make([]pb.Entry, 0, len(entries))
	for _, ent := range entries {
		if ent.Type != pb.ConfigChangeEntry {
			me = append(me, pb.Entry{
				Type:  pb.MetadataEntry,
				Index: ent.Index,
				Term:  ent.Term,
			})
		} else {
			me = append(me, ent)
		}
	}
	return me
}

func (r *raft) sendReplicateMessage(to uint64) {
	var rp *remote
	if v, ok := r.remotes[to]; ok {
		rp = v
	} else if v, ok := r.nonVotings[to]; ok {
		rp = v
	} else {
		rp, ok = r.witnesses[to]
		if !ok {
			plog.Panicf("%s failed to get the remote instance", r.describe())
		}
	}
	if rp.isPaused() {
		return
	}
	m, err := r.makeReplicateMessage(to, rp.next, maxEntrySize)
	if err != nil {
		// log not available due to compaction, send snapshot
		if !rp.isActive() {
			plog.Warningf("%s, %s is not active, sending snapshot is skipped",
				r.describe(), ReplicaID(to))
			return
		}
		index := r.makeInstallSnapshotMessage(to, &m)
		plog.Infof("%s is sending snapshot (%d) to %s, r.Next %d, r.Match %d, %v",
			r.describe(), index, ReplicaID(to), rp.next, rp.match, err)
		rp.becomeSnapshot(index)
	} else if len(m.Entries) > 0 {
		lastIndex := m.Entries[len(m.Entries)-1].Index
		rp.progress(lastIndex)
	}
	r.send(m)
}

func (r *raft) broadcastReplicateMessage() {
	r.mustBeLeader()
	for nid := range r.nonVotings {
		if nid == r.replicaID {
			plog.Panicf("%s nonVoting is broadcasting Replicate msg", r.describe())
		}
	}
	for _, nid := range r.nodes() {
		if nid != r.replicaID {
			r.sendReplicateMessage(nid)
		}
	}
}

func (r *raft) sendHeartbeatMessage(to uint64,
	hint pb.SystemCtx, match uint64) {
	commit := min(match, r.log.committed)
	r.send(pb.Message{
		To:       to,
		Type:     pb.Heartbeat,
		Commit:   commit,
		Hint:     hint.Low,
		HintHigh: hint.High,
	})
}

// p72 of the raft thesis describe how to use Heartbeat message in the ReadIndex
// protocol.
func (r *raft) broadcastHeartbeatMessage() {
	r.mustBeLeader()
	if r.readIndex.hasPendingRequest() {
		ctx := r.readIndex.peepCtx()
		r.broadcastHeartbeatMessageWithHint(ctx)
	} else {
		r.broadcastHeartbeatMessageWithHint(pb.SystemCtx{})
	}
}

func (r *raft) broadcastHeartbeatMessageWithHint(ctx pb.SystemCtx) {
	zeroCtx := pb.SystemCtx{}
	for id, rm := range r.votingMembers() {
		if id != r.replicaID {
			r.sendHeartbeatMessage(id, ctx, rm.match)
		}
	}
	if ctx == zeroCtx {
		for id, rm := range r.nonVotings {
			r.sendHeartbeatMessage(id, zeroCtx, rm.match)
		}
	}
}


// sendTimeoutNowMessage 发送 TimeoutNow 消息，触发目标节点立即发起选举（用于领导者转移）。
// Raft 领导者转移协议通过此消息通知目标节点提前超时并竞选领导者，加速领导权交接。
// 参数：
//   - replicaID: 目标节点 ID（期望成为新领导者的节点）
func (r *raft) sendTimeoutNowMessage(replicaID uint64) {
	r.send(pb.Message{
		Type: pb.TimeoutNow, // 消息类型：立即超时（触发目标节点选举）
		To:   replicaID,     // 目标节点 ID
	})
}

//
// log append and commit
//

// sortMatchValues 对投票成员的日志匹配索引（match）进行排序，用于计算多数派提交索引。
// 采用手动展开的冒泡排序（非标准库 sort，避免内存分配），针对小规模数组（投票成员数）高效。
func (r *raft) sortMatchValues() {
	// 手动展开冒泡排序（针对小规模数组优化，避免 sort.Slice 的内存分配）
	if len(r.matched) == 3 {
		if r.matched[0] > r.matched[1] {
			v := r.matched[0]
			r.matched[0] = r.matched[1]
			r.matched[1] = v
		}
		if r.matched[1] > r.matched[2] {
			v := r.matched[1]
			r.matched[1] = r.matched[2]
			r.matched[2] = v
		}
		if r.matched[0] > r.matched[1] {
			v := r.matched[0]
			r.matched[0] = r.matched[1]
			r.matched[1] = v
		}
	} else if len(r.matched) == 1 {
		return // 单节点无需排序
	} else {
		// 标准排序（适用于成员数 >3 的场景）
		sort.Slice(r.matched, func(i, j int) bool {
			return r.matched[i] < r.matched[j]
		})
	}
}

// tryCommit 尝试提交日志条目，计算并更新集群的已提交索引（committed）。
// Raft 协议核心：领导者通过收集多数派节点的匹配索引（match），确定可安全提交的日志索引。
// 返回值：
//   - bool: 是否成功提交新日志
//   - error: 提交过程中遇到的错误（如日志操作失败）
func (r *raft) tryCommit() (bool, error) {
	r.mustBeLeader() // 仅领导者可执行提交逻辑

	// 若投票成员数与匹配值数组长度不匹配，重置匹配值数组（确保数据一致性）
	if r.numVotingMembers() != len(r.matched) {
		r.resetMatchValueArray()
	}

	// 收集所有投票成员的匹配索引（本地节点 + 远程节点）
	idx := 0
	for _, v := range r.remotes {
		r.matched[idx] = v.match
		idx++
	}
	for _, v := range r.witnesses {
		r.matched[idx] = v.match
		idx++
	}

	// 排序匹配索引，计算多数派阈值（quorum）对应的索引
	r.sortMatchValues()
	q := r.matched[r.numVotingMembers()-r.quorum()] // 多数派阈值位置 = 总成员数 - 法定人数

	// Raft 论文 5.4.2 节：仅当前任期的日志条目可通过计数副本提交，旧任期条目需通过当前任期条目间接提交
	return r.log.tryCommit(q, r.term)
}

// appendEntries 将新日志条目追加到本地日志，并触发单节点集群的自动提交。
// 条目追加时会自动填充任期（当前领导者任期）和索引（基于日志最后索引递增）。
// 参数：
//   - entries: 待追加的日志条目列表
// 返回值：
//   - error: 日志追加过程中遇到的错误
func (r *raft) appendEntries(entries []pb.Entry) error {
	lastIndex := r.log.lastIndex() // 获取当前日志最后索引

	// 填充每个条目的任期和索引（确保条目连续性）
	for i := range entries {
		entries[i].Term = r.term          // 条目任期 = 当前领导者任期
		entries[i].Index = lastIndex + 1 + uint64(i) // 索引 = 最后索引 + 1 + 条目偏移
	}

	r.log.append(entries) // 追加条目到本地日志
	// 更新本地节点的匹配索引（自身日志始终匹配）
	r.remotes[r.replicaID].tryUpdate(r.log.lastIndex())

	// 单节点集群（法定人数为1）：追加后立即提交
	if r.isSingleNodeQuorum() {
		if _, err := r.tryCommit(); err != nil {
			return err
		}
	}
	return nil
}

//
// state transition related functions
//

// toFollowerState 将节点转换为跟随者状态，重置任期和选举计时器。
// 内部状态转换函数，被 `becomeFollower` 等公开方法调用。
// 参数：
//   - term: 新任期号
//   - leaderID: 领导者节点 ID
//   - resetElectionTimeout: 是否重置选举计时器（避免立即触发新选举）
func (r *raft) toFollowerState(term uint64, leaderID uint64,
	resetElectionTimeout bool) {
	if r.isWitness() {
		panic("transitioning to follower from witness state") // 见证节点不能直接转为跟随者
	}
	r.state = follower // 更新状态为跟随者
	r.reset(term, resetElectionTimeout) // 重置任期、计时器等状态
	r.setLeaderID(leaderID) // 设置领导者 ID
	plog.Infof("%s became follower", r.describe())
}

// becomeNonVoting 将节点转换为非投票成员状态（仅适用于已是非投票节点的场景）。
// 非投票节点参与日志复制但不参与选举和投票，通常用于集群扩容时的预热。
// 参数：
//   - term: 新任期号
//   - leaderID: 领导者节点 ID
func (r *raft) becomeNonVoting(term uint64, leaderID uint64) {
	if !r.isNonVoting() {
		panic("transitioning to nonVoting state from other states") // 仅非投票节点可调用
	}
	if r.isWitness() {
		panic("transitioning to nonVoting from witness state") // 见证节点不能转为非投票节点
	}
	r.reset(term, true) // 重置任期和计时器
	r.setLeaderID(leaderID) // 设置领导者 ID
	plog.Infof("%s became nonVoting", r.describe())
}

// becomeWitness 将节点转换为见证节点状态（仅适用于已是见证节点的场景）。
// 见证节点仅参与法定人数计算，不存储完整日志，用于提升集群可用性（如跨区域部署）。
// 参数：
//   - term: 新任期号
//   - leaderID: 领导者节点 ID
func (r *raft) becomeWitness(term uint64, leaderID uint64) {
	if !r.isWitness() {
		panic("transitioning to witness state from non-witness") // 仅见证节点可调用
	}
	r.reset(term, true) // 重置任期和计时器
	r.setLeaderID(leaderID) // 设置领导者 ID
	plog.Infof("%s became witness", r.describe())
}

// becomeFollower 将节点转换为跟随者状态，并重置选举计时器（触发随机化超时）。
// 公开方法，用于正常状态转换（如收到更高任期的消息时）。
// 参数：
//   - term: 新任期号
//   - leaderID: 领导者节点 ID
func (r *raft) becomeFollower(term uint64, leaderID uint64) {
	r.toFollowerState(term, leaderID, true)
}

// becomeFollowerKE 将节点转换为跟随者状态，但不重置选举计时器（KE = Keep ElectionTimeout）。
// 用于特殊场景：避免因时钟偏差或调度延迟导致节点无法发起选举（如收到投票请求时）。
// 参数：
//   - term: 新任期号
//   - leaderID: 领导者节点 ID
func (r *raft) becomeFollowerKE(term uint64, leaderID uint64) {
	r.toFollowerState(term, leaderID, false) // 不重置选举计时器
}

// becomePreVoteCandidate 将节点转换为预选举候选者状态（仅在启用 PreVote 时调用）。
// PreVote 是 Raft 的优化，避免因网络分区导致的任期频繁增加，候选者需先通过预选举验证资格。
func (r *raft) becomePreVoteCandidate() {
	if !r.preVote {
		panic("becomePreVoteCandidate called when preVote not enabled") // PreVote 未启用时 panic
	}
	if r.isLeader() {
		panic("transitioning to candidate state from leader") // 领导者不能转为候选者
	}
	if r.isNonVoting() || r.isWitness() {
		panic("non-voting/witness node cannot become candidate") // 非投票/见证节点不参与选举
	}
	r.state = preVoteCandidate // 更新状态为预选举候选者
	r.reset(r.term, true) // 重置状态（任期不变，因 PreVote 使用 term+1）
	r.setLeaderID(NoLeader) // 清除领导者 ID
	plog.Warningf("%s became PreVote candidate", r.describe())
}

// becomeCandidate 将节点转换为正式选举候选者状态，开始竞选领导者。
// 触发条件：选举超时且未收到领导者心跳，或预选举成功后进入正式选举。
func (r *raft) becomeCandidate() {
	if r.isLeader() {
		panic("transitioning to candidate state from leader") // 领导者不能转为候选者
	}
	if r.isNonVoting() || r.isWitness() {
		panic("non-voting/witness node cannot become candidate") // 非投票/见证节点不参与选举
	}
	r.state = candidate // 更新状态为候选者
	// Raft 论文 5.2 节：成为候选者后，任期自增 1，重置计时器，投票给自己
	r.reset(r.term+1, true) // 任期 +1，重置计时器
	r.setLeaderID(NoLeader) // 清除领导者 ID
	r.vote = r.replicaID // 投票给自己
	plog.Warningf("%s became candidate", r.describe())
}

// becomeLeader 将节点转换为领导者状态，初始化日志复制进度并追加 dummy 条目。
// 领导者需初始化所有节点的 nextIndex（日志最后索引 +1），并通过追加条目确立领导权。
// 返回值：
//   - error: 成为领导者过程中遇到的错误（如日志追加失败）
func (r *raft) becomeLeader() error {
	// 状态转换合法性检查：仅候选者可成为领导者
	if !r.isLeader() && !r.isCandidate() {
		plog.Panicf("transitioning to leader state from %v", r.state.String())
	}
	r.state = leader // 更新状态为领导者
	r.reset(r.term, true) // 重置状态（任期不变，计时器重置）
	r.setLeaderID(r.replicaID) // 设置领导者 ID 为自身
	r.preLeaderPromotionHandleConfigChange() // 处理未提交的配置变更
	plog.Infof("%s became leader", r.describe())

	// Raft 论文 6.4 节：领导者需追加一条空日志条目（dummy entry）以提交旧任期日志
	return r.appendEntries([]pb.Entry{{Type: pb.ApplicationEntry, Cmd: nil}})
}

// reset 重置 Raft 节点的核心状态（任期、计时器、投票、复制进度等）。
// 用于状态转换（如成为候选者/跟随者）或异常恢复时的状态清理。
// 参数：
//   - term: 新任期号
//   - resetElectionTimeout: 是否重置选举计时器（触发随机化超时）
func (r *raft) reset(term uint64, resetElectionTimeout bool) {
	// 若任期变更，重置投票状态（当前任期未投票）
	if r.term != term {
		r.term = term
		r.vote = NoLeader
	}

	// 重置速率限制器（若启用）
	if r.rl.Enabled() {
		r.rl.Reset()
	}

	// 重置选举计时器（如成为跟随者时）
	if resetElectionTimeout {
		r.electionTick = 0
		r.setRandomizedElectionTimeout()
	}

	// 重置投票集合、心跳计时器、ReadIndex、领导者转移等临时状态
	r.votes = make(map[uint64]bool)
	r.heartbeatTick = 0
	r.readIndex = newReadIndex()
	r.clearPendingConfigChange()
	r.abortLeaderTransfer()

	// 重置复制进度跟踪（远程节点、非投票节点、见证节点）
	r.resetRemotes()
	r.resetNonVotings()
	r.resetWitnesses()
	r.resetMatchValueArray()
}


func (r *raft) sendTimeoutNowMessage(replicaID uint64) {
	r.send(pb.Message{
		Type: pb.TimeoutNow,
		To:   replicaID,
	})
}

//
// log append and commit
//

func (r *raft) sortMatchValues() {
	// unrolled bubble sort, sort.Slice is not allocation free
	if len(r.matched) == 3 {
		if r.matched[0] > r.matched[1] {
			v := r.matched[0]
			r.matched[0] = r.matched[1]
			r.matched[1] = v
		}
		if r.matched[1] > r.matched[2] {
			v := r.matched[1]
			r.matched[1] = r.matched[2]
			r.matched[2] = v
		}
		if r.matched[0] > r.matched[1] {
			v := r.matched[0]
			r.matched[0] = r.matched[1]
			r.matched[1] = v
		}
	} else if len(r.matched) == 1 {
		return
	} else {
		sort.Slice(r.matched, func(i, j int) bool {
			return r.matched[i] < r.matched[j]
		})
	}
}

func (r *raft) tryCommit() (bool, error) {
	r.mustBeLeader()
	if r.numVotingMembers() != len(r.matched) {
		r.resetMatchValueArray()
	}
	idx := 0
	for _, v := range r.remotes {
		r.matched[idx] = v.match
		idx++
	}
	for _, v := range r.witnesses {
		r.matched[idx] = v.match
		idx++
	}
	r.sortMatchValues()
	q := r.matched[r.numVotingMembers()-r.quorum()]
	// see p8 raft paper
	// "Raft never commits log entries from previous terms by counting replicas.
	// Only log entries from the leader’s current term are committed by counting
	// replicas"
	return r.log.tryCommit(q, r.term)
}

func (r *raft) appendEntries(entries []pb.Entry) error {
	lastIndex := r.log.lastIndex()
	for i := range entries {
		entries[i].Term = r.term
		entries[i].Index = lastIndex + 1 + uint64(i)
	}
	r.log.append(entries)
	r.remotes[r.replicaID].tryUpdate(r.log.lastIndex())
	if r.isSingleNodeQuorum() {
		if _, err := r.tryCommit(); err != nil {
			return err
		}
	}
	return nil
}

//
// state transition related functions
//

func (r *raft) toFollowerState(term uint64, leaderID uint64,
	resetElectionTimeout bool) {
	if r.isWitness() {
		panic("transitioning to follower from witness state")
	}
	r.state = follower
	r.reset(term, resetElectionTimeout)
	r.setLeaderID(leaderID)
	plog.Infof("%s became follower", r.describe())
}

func (r *raft) becomeNonVoting(term uint64, leaderID uint64) {
	if !r.isNonVoting() {
		panic("transitioning to nonVoting state from other states")
	}
	if r.isWitness() {
		panic("transitioning to nonVoting from witness state")
	}
	r.reset(term, true)
	r.setLeaderID(leaderID)
	plog.Infof("%s became nonVoting", r.describe())
}

func (r *raft) becomeWitness(term uint64, leaderID uint64) {
	if !r.isWitness() {
		panic("transitioning to witness state from non-witness")
	}
	r.reset(term, true)
	r.setLeaderID(leaderID)
	plog.Infof("%s became witness", r.describe())
}

func (r *raft) becomeFollower(term uint64, leaderID uint64) {
	r.toFollowerState(term, leaderID, true)
}

func (r *raft) becomeFollowerKE(term uint64, leaderID uint64) {
	r.toFollowerState(term, leaderID, false)
}

func (r *raft) becomePreVoteCandidate() {
	if !r.preVote {
		panic("becomePreVoteCandidate called when preVote not enabled")
	}
	if r.isLeader() {
		panic("transitioning to candidate state from leader")
	}
	if r.isNonVoting() {
		panic("nonVoting is becoming candidate")
	}
	if r.isWitness() {
		panic("witness is becoming candidate")
	}
	r.state = preVoteCandidate
	r.reset(r.term, true)
	r.setLeaderID(NoLeader)
	plog.Warningf("%s became PreVote candidate", r.describe())
}

func (r *raft) becomeCandidate() {
	if r.isLeader() {
		panic("transitioning to candidate state from leader")
	}
	if r.isNonVoting() {
		panic("nonVoting is becoming candidate")
	}
	if r.isWitness() {
		panic("witness is becoming candidate")
	}
	r.state = candidate
	// 2nd paragraph section 5.2 of the raft paper
	r.reset(r.term+1, true)
	r.setLeaderID(NoLeader)
	r.vote = r.replicaID
	plog.Warningf("%s became candidate", r.describe())
}

func (r *raft) becomeLeader() error {
	// need a state transition machine
	if !r.isLeader() && !r.isCandidate() {
		plog.Panicf("transitioning to leader state from %v", r.state.String())
	}
	r.state = leader
	r.reset(r.term, true)
	r.setLeaderID(r.replicaID)
	r.preLeaderPromotionHandleConfigChange()
	plog.Infof("%s became leader", r.describe())
	// p72 of the raft thesis
	return r.appendEntries([]pb.Entry{{Type: pb.ApplicationEntry, Cmd: nil}})
}

func (r *raft) reset(term uint64, resetElectionTimeout bool) {
	if r.term != term {
		r.term = term
		r.vote = NoLeader
	}
	if r.rl.Enabled() {
		r.rl.Reset()
	}
	if resetElectionTimeout {
		r.electionTick = 0
		r.setRandomizedElectionTimeout()
	}
	r.votes = make(map[uint64]bool)
	r.heartbeatTick = 0
	r.readIndex = newReadIndex()
	r.clearPendingConfigChange()
	r.abortLeaderTransfer()
	r.resetRemotes()
	r.resetNonVotings()
	r.resetWitnesses()
	r.resetMatchValueArray()
}

// preLeaderPromotionHandleConfigChange 处理领导者晋升前的未提交配置变更条目。
// 确保领导者在晋升时最多只有一个未应用的配置变更条目，避免因多配置变更同时提交导致的 quorum 重叠问题。
func (r *raft) preLeaderPromotionHandleConfigChange() {
	n := r.getPendingConfigChangeCount()
	if n > 1 {
		plog.Panicf("%s multiple uncommitted config change entries", r.describe())
	} else if n == 1 {
		plog.Infof("%s becoming leader with pending ConfigChange", r.describe())
		r.setPendingConfigChange() // 标记存在未提交配置变更
	}
}

// resetRemotes 重置所有投票成员的复制进度跟踪状态（match/next 索引）。
// Raft 论文 5.3 节：领导者首次掌权时，将所有 nextIndex 初始化为自身日志最后索引 + 1。
func (r *raft) resetRemotes() {
	for id := range r.remotes {
		// 初始化复制进度：next 为日志最后索引 + 1，match 默认为 0（自身节点设为日志最后索引）
		r.remotes[id] = &remote{
			next: r.log.lastIndex() + 1,
		}
		if id == r.replicaID {
			r.remotes[id].match = r.log.lastIndex() // 自身节点已匹配所有本地日志
		}
	}
}

// resetNonVotings 重置所有非投票成员的复制进度跟踪状态（逻辑同投票成员）。
func (r *raft) resetNonVotings() {
	for id := range r.nonVotings {
		r.nonVotings[id] = &remote{
			next: r.log.lastIndex() + 1,
		}
		if id == r.replicaID {
			r.nonVotings[id].match = r.log.lastIndex()
		}
	}
}

// resetWitnesses 重置所有见证成员的复制进度跟踪状态（逻辑同投票成员）。
func (r *raft) resetWitnesses() {
	for id := range r.witnesses {
		r.witnesses[id] = &remote{
			next: r.log.lastIndex() + 1,
		}
		if id == r.replicaID {
			r.witnesses[id].match = r.log.lastIndex()
		}
	}
}

//
// election related functions
//

// handleVoteResp 处理投票响应（预选举/正式选举），统计已收到的赞成票数。
// 参数：
//   - from: 投票节点 ID
//   - rejected: 是否拒绝投票
//   - preVote: 是否为预选举阶段
// 返回值：
//   - int: 当前已收到的赞成票数
func (r *raft) handleVoteResp(from uint64, rejected bool, preVote bool) int {
	mname := "RequestVoteResp"
	if preVote {
		mname = "RequestPreVoteResp" // 预选举响应类型
	}
	// 日志投票结果（接受/拒绝）
	if rejected {
		plog.Warningf("%s received %s rejection from %s", r.describe(), mname, ReplicaID(from))
	} else {
		plog.Warningf("%s received %s from %s", r.describe(), mname, ReplicaID(from))
	}

	votedFor := 0
	// 记录投票（避免重复统计同一节点的投票）
	if _, ok := r.votes[from]; !ok {
		r.votes[from] = !rejected // true 表示赞成票
	}
	// 统计总赞成票数
	for _, v := range r.votes {
		if v {
			votedFor++
		}
	}
	return votedFor
}

// preVoteCampaign 启动预选举流程（PreVote 优化），避免网络分区导致的任期膨胀。
// 预选举要求候选者先获取多数派节点的预投票，证明其日志足够新，才允许发起正式选举。
func (r *raft) preVoteCampaign() error {
	r.becomePreVoteCandidate() // 转为预选举候选者状态
	r.handleVoteResp(r.replicaID, false, true) // 给自己投赞成票

	// 单节点集群直接进入正式选举
	if r.isSingleNodeQuorum() {
		return r.campaign()
	}

	// 获取本地日志最后索引和任期（用于证明日志新鲜度）
	index := r.log.lastIndex()
	lastTerm, err := r.log.lastTerm()
	if err != nil {
		return err
	}

	// 向所有投票成员发送预选举请求（RequestPreVote）
	for k := range r.votingMembers() {
		if k == r.replicaID {
			continue
		}
		r.send(pb.Message{
			Term:     r.term + 1,      // 预选举使用 term+1（不影响当前任期）
			To:       k,               // 目标投票节点
			Type:     pb.RequestPreVote, // 消息类型：预选举请求
			LogIndex: index,           // 本地日志最后索引
			LogTerm:  lastTerm,        // 本地日志最后任期
		})
		plog.Warningf("%s sent RequestPreVote to %s", r.describe(), ReplicaID(k))
	}
	return nil
}

// campaign 启动正式选举流程，向集群所有投票成员发送 RequestVote 请求。
// 若获得多数派投票，则晋升为领导者。
func (r *raft) campaign() error {
	r.becomeCandidate() // 转为正式选举候选者状态
	term := r.term

	// 触发选举启动事件（用于监控）
	if r.events != nil {
		info := server.CampaignInfo{
			ShardID:   r.shardID,
			ReplicaID: r.replicaID,
			Term:      term,
		}
		r.events.CampaignLaunched(info)
	}

	r.handleVoteResp(r.replicaID, false, false) // 给自己投赞成票

	// 单节点集群直接成为领导者
	if r.isSingleNodeQuorum() {
		return r.becomeLeader()
	}

	// 领导者转移提示（若当前为转移目标，Hint 设为自身 ID）
	var hint uint64
	if r.isLeaderTransferTarget {
		hint = r.replicaID
		r.isLeaderTransferTarget = false
	}

	// 获取本地日志最后索引和任期（用于请求投票）
	index := r.log.lastIndex()
	lastTerm, err := r.log.lastTerm()
	if err != nil {
		return err
	}

	// 向所有投票成员发送 RequestVote 请求
	for k := range r.votingMembers() {
		if k == r.replicaID {
			continue
		}
		r.send(pb.Message{
			Term:     term,            // 当前任期（已自增）
			To:       k,               // 目标投票节点
			Type:     pb.RequestVote,  // 消息类型：投票请求
			LogIndex: index,           // 本地日志最后索引
			LogTerm:  lastTerm,        // 本地日志最后任期
			Hint:     hint,            // 领导者转移提示
		})
		plog.Warningf("%s sent RequestVote to %s", r.describe(), ReplicaID(k))
	}
	return nil
}

//
// membership management
//

// selfRemoved 检查当前节点是否已从集群成员配置中移除。
// 返回值：true 表示节点已被移除（不在投票/非投票/见证成员列表中）
func (r *raft) selfRemoved() bool {
	if r.isNonVoting() {
		_, ok := r.nonVotings[r.replicaID]
		return !ok
	}
	if r.isWitness() {
		_, ok := r.witnesses[r.replicaID]
		return !ok
	}
	_, ok := r.remotes[r.replicaID]
	return !ok
}

// addNode 将节点添加为投票成员，支持从非投票成员晋升或新增节点。
// 若为本地节点晋升，自动转为跟随者状态（非投票节点不参与选举）。
func (r *raft) addNode(replicaID uint64) {
	r.clearPendingConfigChange() // 清除未决配置变更

	// 见证节点不能直接晋升为投票成员（需通过配置变更流程）
	if replicaID == r.replicaID && r.isWitness() {
		plog.Panicf("%s is witness", r.describe())
	}

	// 已为投票成员，无需重复添加
	if _, ok := r.remotes[replicaID]; ok {
		return
	}

	// 从非投票成员晋升（继承复制进度）
	if rp, ok := r.nonVotings[replicaID]; ok {
		r.deleteNonVoting(replicaID)
		r.remotes[replicaID] = rp
		// 本地节点晋升为投票成员，转为跟随者（非投票节点可能之前处于特殊状态）
		if replicaID == r.replicaID {
			r.becomeFollower(r.term, r.leaderID)
		}
	} else if _, ok := r.witnesses[replicaID]; ok {
		panic("could not promote witness to full member") // 见证节点不允许直接晋升
	} else {
		// 新增投票成员，初始化复制进度
		r.setRemote(replicaID, 0, r.log.lastIndex()+1)
	}
}

// addNonVoting 添加非投票成员（仅参与日志复制，不参与选举和投票）。
// 非投票成员用于集群扩容预热或作为只读副本。
func (r *raft) addNonVoting(replicaID uint64) {
	r.clearPendingConfigChange()
	// 本地节点必须已是非投票状态才能添加
	if replicaID == r.replicaID && !r.isNonVoting() {
		plog.Panicf("%s is not a nonVoting", r.describe())
	}
	// 已为非投票成员，无需重复添加
	if _, ok := r.nonVotings[replicaID]; ok {
		return
	}
	// 初始化复制进度
	r.setNonVoting(replicaID, 0, r.log.lastIndex()+1)
}

// addWitness 添加见证成员（仅参与法定人数计算，不存储完整日志）。
// 见证节点用于跨区域部署提升可用性，不占用完整副本资源。
func (r *raft) addWitness(replicaID uint64) {
	r.clearPendingConfigChange()
	// 本地节点必须已是见证状态才能添加
	if replicaID == r.replicaID && !r.isWitness() {
		plog.Panicf("%s is not witness", r.describe())
	}
	// 已为见证成员，无需重复添加
	if _, ok := r.witnesses[replicaID]; ok {
		return
	}
	// 初始化复制进度
	r.setWitness(replicaID, 0, r.log.lastIndex()+1)
}

// removeNode 从集群中移除指定节点（投票/非投票/见证成员）。
// 若移除的是当前领导者，自动退化为跟随者。
func (r *raft) removeNode(replicaID uint64) error {
	r.deleteRemote(replicaID)    // 从投票成员移除
	r.deleteNonVoting(replicaID) // 从非投票成员移除
	r.deleteWitness(replicaID)   // 从见证成员移除
	r.clearPendingConfigChange() // 清除未决配置变更

	// 若移除自身且为领导者，退化为跟随者
	if r.replicaID == replicaID && r.isLeader() {
		r.becomeFollower(r.term, NoLeader)
	}

	// 若正在转移领导者至该节点，中止转移
	if r.leaderTransfering() && r.leaderTransferTarget == replicaID {
		r.abortLeaderTransfer()
	}

	// 领导者移除节点后尝试提交日志（更新集群配置）
	if r.isLeader() && r.numVotingMembers() > 0 {
		ok, err := r.tryCommit()
		if err != nil {
			return err
		}
		if ok {
			r.broadcastReplicateMessage() // 广播配置变更日志
		}
	}
	return nil
}

// deleteRemote 从投票成员映射中删除节点。
func (r *raft) deleteRemote(replicaID uint64) {
	delete(r.remotes, replicaID)
}

// deleteNonVoting 从非投票成员映射中删除节点。
func (r *raft) deleteNonVoting(replicaID uint64) {
	delete(r.nonVotings, replicaID)
}

// deleteWitness 从见证成员映射中删除节点。
func (r *raft) deleteWitness(replicaID uint64) {
	delete(r.witnesses, replicaID)
}

// setRemote 初始化或更新投票成员的复制进度。
func (r *raft) setRemote(replicaID uint64, match uint64, next uint64) {
	plog.Debugf("%s set remote %s, match %d, next %d",
		r.describe(), ReplicaID(replicaID), match, next)
	r.remotes[replicaID] = &remote{
		next:  next,  // 下一条待复制日志索引
		match: match, // 已匹配日志索引
	}
}

// setNonVoting 初始化或更新非投票成员的复制进度。
func (r *raft) setNonVoting(replicaID uint64, match uint64, next uint64) {
	plog.Debugf("%s set nonVoting %s, match %d, next %d",
		r.describe(), ReplicaID(replicaID), match, next)
	r.nonVotings[replicaID] = &remote{
		next:  next,
		match: match,
	}
}

// setWitness 初始化或更新见证成员的复制进度。
func (r *raft) setWitness(replicaID uint64, match uint64, next uint64) {
	plog.Debugf("%s set witness %s, match %d, next %d",
		r.describe(), ReplicaID(replicaID), match, next)
	r.witnesses[replicaID] = &remote{
		next:  next,
		match: match,
	}
}

// setPendingConfigChange 标记存在未提交的配置变更条目。
// Raft 配置变更安全要求：同一时间只能有一个未提交的配置变更，避免新旧 quorum 无重叠。
func (r *raft) setPendingConfigChange() {
	r.pendingConfigChange = true
}

// hasPendingConfigChange 检查是否存在未提交的配置变更条目。
func (r *raft) hasPendingConfigChange() bool {
	return r.pendingConfigChange
}

// clearPendingConfigChange 清除未提交配置变更标记（配置变更提交或中止时调用）。
func (r *raft) clearPendingConfigChange() {
	r.pendingConfigChange = false
}

// getPendingConfigChangeCount 统计已提交但未应用的配置变更条目数量。
// 确保同一时间最多只有一个未应用的配置变更，避免安全风险。
func (r *raft) getPendingConfigChangeCount() int {
	idx := r.log.committed + 1 // 从已提交索引的下一条开始检查
	count := 0
	for {
		ents, err := r.log.entries(idx, maxEntriesToApplySize)
		if err != nil {
			plog.Panicf("%s failed to get entries %v", r.describe(), err)
		}
		if len(ents) == 0 {
			return count // 无更多条目，返回统计结果
		}
		count += countConfigChange(ents) // 统计当前批次中的配置变更条目
		idx = ents[len(ents)-1].Index + 1 // 移动到下一批次
	}
}


func (r *raft) preLeaderPromotionHandleConfigChange() {
	n := r.getPendingConfigChangeCount()
	if n > 1 {
		plog.Panicf("%s multiple uncommitted config change entries", r.describe())
	} else if n == 1 {
		plog.Infof("%s becoming leader with pending ConfigChange", r.describe())
		r.setPendingConfigChange()
	}
}

// see section 5.3 of the raft paper
// "When a leader first comes to power, it initializes all nextIndex values to
// the index just after the last one in its log"
func (r *raft) resetRemotes() {
	for id := range r.remotes {
		r.remotes[id] = &remote{
			next: r.log.lastIndex() + 1,
		}
		if id == r.replicaID {
			r.remotes[id].match = r.log.lastIndex()
		}
	}
}

func (r *raft) resetNonVotings() {
	for id := range r.nonVotings {
		r.nonVotings[id] = &remote{
			next: r.log.lastIndex() + 1,
		}
		if id == r.replicaID {
			r.nonVotings[id].match = r.log.lastIndex()
		}
	}
}

func (r *raft) resetWitnesses() {
	for id := range r.witnesses {
		r.witnesses[id] = &remote{
			next: r.log.lastIndex() + 1,
		}
		if id == r.replicaID {
			r.witnesses[id].match = r.log.lastIndex()
		}
	}
}

//
// election related functions
//

func (r *raft) handleVoteResp(from uint64, rejected bool, preVote bool) int {
	mname := "RequestVoteResp"
	if preVote {
		mname = "RequestPreVoteResp"
	}
	if rejected {
		plog.Warningf("%s received %s rejection from %s",
			r.describe(), mname, ReplicaID(from))
	} else {
		plog.Warningf("%s received %s from %s",
			r.describe(), mname, ReplicaID(from))
	}
	votedFor := 0
	if _, ok := r.votes[from]; !ok {
		r.votes[from] = !rejected
	}
	for _, v := range r.votes {
		if v {
			votedFor++
		}
	}
	return votedFor
}

func (r *raft) preVoteCampaign() error {
	r.becomePreVoteCandidate()
	r.handleVoteResp(r.replicaID, false, true)
	if r.isSingleNodeQuorum() {
		return r.campaign()
	}
	index := r.log.lastIndex()
	lastTerm, err := r.log.lastTerm()
	if err != nil {
		return err
	}
	for k := range r.votingMembers() {
		if k == r.replicaID {
			continue
		}
		r.send(pb.Message{
			Term:     r.term + 1,
			To:       k,
			Type:     pb.RequestPreVote,
			LogIndex: index,
			LogTerm:  lastTerm,
		})
		plog.Warningf("%s sent RequestPreVote to %s", r.describe(), ReplicaID(k))
	}
	return nil
}

func (r *raft) campaign() error {
	r.becomeCandidate()
	term := r.term
	if r.events != nil {
		info := server.CampaignInfo{
			ShardID:   r.shardID,
			ReplicaID: r.replicaID,
			Term:      term,
		}
		r.events.CampaignLaunched(info)
	}
	r.handleVoteResp(r.replicaID, false, false)
	if r.isSingleNodeQuorum() {
		return r.becomeLeader()
	}
	var hint uint64
	if r.isLeaderTransferTarget {
		hint = r.replicaID
		r.isLeaderTransferTarget = false
	}
	index := r.log.lastIndex()
	lastTerm, err := r.log.lastTerm()
	if err != nil {
		return err
	}
	for k := range r.votingMembers() {
		if k == r.replicaID {
			continue
		}
		r.send(pb.Message{
			Term:     term,
			To:       k,
			Type:     pb.RequestVote,
			LogIndex: index,
			LogTerm:  lastTerm,
			Hint:     hint,
		})
		plog.Warningf("%s sent RequestVote to %s", r.describe(), ReplicaID(k))
	}
	return nil
}

//
// membership management
//

func (r *raft) selfRemoved() bool {
	if r.isNonVoting() {
		_, ok := r.nonVotings[r.replicaID]
		return !ok
	}
	if r.isWitness() {
		_, ok := r.witnesses[r.replicaID]
		return !ok
	}
	_, ok := r.remotes[r.replicaID]
	return !ok
}

func (r *raft) addNode(replicaID uint64) {
	r.clearPendingConfigChange()
	if replicaID == r.replicaID && r.isWitness() {
		plog.Panicf("%s is witness", r.describe())
	}
	if _, ok := r.remotes[replicaID]; ok {
		// already a voting member
		return
	}
	if rp, ok := r.nonVotings[replicaID]; ok {
		// promoting to full member with inherited progress info
		r.deleteNonVoting(replicaID)
		r.remotes[replicaID] = rp
		// local peer promoted, become follower
		if replicaID == r.replicaID {
			r.becomeFollower(r.term, r.leaderID)
		}
	} else if _, ok := r.witnesses[replicaID]; ok {
		panic("could not promote witness to full member")
	} else {
		r.setRemote(replicaID, 0, r.log.lastIndex()+1)
	}
}

func (r *raft) addNonVoting(replicaID uint64) {
	r.clearPendingConfigChange()
	if replicaID == r.replicaID && !r.isNonVoting() {
		plog.Panicf("%s is not a nonVoting", r.describe())
	}
	if _, ok := r.nonVotings[replicaID]; ok {
		return
	}
	r.setNonVoting(replicaID, 0, r.log.lastIndex()+1)
}

func (r *raft) addWitness(replicaID uint64) {
	r.clearPendingConfigChange()
	if replicaID == r.replicaID && !r.isWitness() {
		plog.Panicf("%s is not witness", r.describe())
	}
	if _, ok := r.witnesses[replicaID]; ok {
		return
	}
	r.setWitness(replicaID, 0, r.log.lastIndex()+1)
}

func (r *raft) removeNode(replicaID uint64) error {
	r.deleteRemote(replicaID)
	r.deleteNonVoting(replicaID)
	r.deleteWitness(replicaID)
	r.clearPendingConfigChange()
	// step down as leader once it is removed
	if r.replicaID == replicaID && r.isLeader() {
		r.becomeFollower(r.term, NoLeader)
	}
	if r.leaderTransfering() && r.leaderTransferTarget == replicaID {
		r.abortLeaderTransfer()
	}
	if r.isLeader() && r.numVotingMembers() > 0 {
		ok, err := r.tryCommit()
		if err != nil {
			return err
		}
		if ok {
			r.broadcastReplicateMessage()
		}
	}
	return nil
}

func (r *raft) deleteRemote(replicaID uint64) {
	delete(r.remotes, replicaID)
}

func (r *raft) deleteNonVoting(replicaID uint64) {
	delete(r.nonVotings, replicaID)
}

func (r *raft) deleteWitness(replicaID uint64) {
	delete(r.witnesses, replicaID)
}

func (r *raft) setRemote(replicaID uint64, match uint64, next uint64) {
	plog.Debugf("%s set remote %s, match %d, next %d",
		r.describe(), ReplicaID(replicaID), match, next)
	r.remotes[replicaID] = &remote{
		next:  next,
		match: match,
	}
}

func (r *raft) setNonVoting(replicaID uint64, match uint64, next uint64) {
	plog.Debugf("%s set nonVoting %s, match %d, next %d",
		r.describe(), ReplicaID(replicaID), match, next)
	r.nonVotings[replicaID] = &remote{
		next:  next,
		match: match,
	}
}

func (r *raft) setWitness(replicaID uint64, match uint64, next uint64) {
	plog.Debugf("%s set witness %s, match %d, next %d",
		r.describe(), ReplicaID(replicaID), match, next)
	r.witnesses[replicaID] = &remote{
		next:  next,
		match: match,
	}
}

// helper methods required for the membership change implementation
//
// p33-35 of the raft thesis describes a simple membership change protocol which
// requires only one node can be added or removed at a time. its safety is
// guarded by the fact that when there is only one node to be added or removed
// at a time, the old and new quorum are guaranteed to overlap.
// the protocol described in the raft thesis requires the membership change
// entry to be executed as soon as it is appended. this also introduces an extra
// troublesome step to roll back to an old membership configuration when
// necessary.
// similar to etcd raft, we treat such membership change entry as regular
// entries that are only executed after being committed (by the old quorum).
// to do that, however, we need to further restrict the leader to only has at
// most one pending not applied membership change entry in its log. this is to
// avoid the situation that two pending membership change entries are committed
// in one go with the same quorum while they actually require different quorums.
// consider the following situation -
// for a 3 nodes shard with existing members X, Y and Z, let's say we first
// propose a membership change to add a new node A, before A gets committed and
// applied, say we propose another membership change to add a new node B. When
// B gets committed, A will be committed as well, both will be using the 3 node
// membership quorum meaning both entries concerning A and B will become
// committed when any two of the X, Y, Z shard have them replicated. this thus
// violates the safety requirement as B will require 3 out of the 4 nodes (X,
// Y, Z, A) to have it replicated before it can be committed.
// we use the following pendingConfigChange flag to help tracking whether there
// is already a pending membership change entry in the log waiting to be
// executed.
func (r *raft) setPendingConfigChange() {
	r.pendingConfigChange = true
}

func (r *raft) hasPendingConfigChange() bool {
	return r.pendingConfigChange
}

func (r *raft) clearPendingConfigChange() {
	r.pendingConfigChange = false
}

func (r *raft) getPendingConfigChangeCount() int {
	idx := r.log.committed + 1
	count := 0
	for {
		ents, err := r.log.entries(idx, maxEntriesToApplySize)
		if err != nil {
			plog.Panicf("%s failed to get entries %v", r.describe(), err)
		}
		if len(ents) == 0 {
			return count
		}
		count += countConfigChange(ents)
		idx = ents[len(ents)-1].Index + 1
	}
}

//
// handler for various message types
//
// handleFollowerPropose 处理跟随者节点收到的 Propose 消息（客户端提案）。
// 跟随者不直接处理提案，而是转发给当前领导者（若存在）。
// 参数：
//   - m: 包含提案条目的消息
// 返回值：
//   - error: 处理过程中遇到的错误（如无领导者时记录错误）
func (r *raft) handleFollowerPropose(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped proposal, no leader", r.describe())
		r.reportDroppedProposal(m) // 记录被丢弃的提案（用于监控和重试）
		return nil
	}
	m.To = r.leaderID // 转发目标设为当前领导者
	// 复制提案条目（避免原消息被复用导致的并发问题）
	m.Entries = newEntrySlice(m.Entries)
	r.send(m) // 转发提案给领导者
	return nil
}

// leaderIsAvailable 更新领导者可用状态，重置选举计时器以防止触发新选举。
// 当跟随者收到领导者消息（如心跳、复制请求）时调用，证明领导者仍存活。
func (r *raft) leaderIsAvailable() {
	r.electionTick = 0 // 重置选举计时器（避免因超时而发起新选举）
}

// handleFollowerReplicate 处理跟随者节点收到的 Replicate 消息（日志复制请求）。
// 更新领导者状态并调用通用复制逻辑处理日志条目。
// 参数：
//   - m: 包含待复制日志条目的消息
// 返回值：
//   - error: 日志复制过程中遇到的错误（如日志匹配失败）
func (r *raft) handleFollowerReplicate(m pb.Message) error {
	r.leaderIsAvailable() // 标记领导者可用，重置选举计时器
	r.setLeaderID(m.From) // 更新当前领导者 ID（消息发送者）
	return r.handleReplicateMessage(m) // 调用通用复制逻辑处理日志条目
}

// handleFollowerHeartbeat 处理跟随者节点收到的 Heartbeat 消息（领导者心跳）。
// 更新领导者状态并调用通用心跳逻辑处理提交索引。
// 参数：
//   - m: 包含提交索引的心跳消息
// 返回值：
//   - error: 心跳处理过程中遇到的错误
func (r *raft) handleFollowerHeartbeat(m pb.Message) error {
	r.leaderIsAvailable() // 标记领导者可用，重置选举计时器
	r.setLeaderID(m.From) // 更新当前领导者 ID（消息发送者）
	return r.handleHeartbeatMessage(m) // 调用通用心跳逻辑处理提交索引
}

// handleFollowerReadIndex 处理跟随者节点收到的 ReadIndex 请求（线性一致性读）。
// 跟随者不直接处理读请求，而是转发给当前领导者（若存在）。
// 参数：
//   - m: 包含读请求上下文的消息
// 返回值：
//   - error: 处理过程中遇到的错误（如无领导者时记录错误）
func (r *raft) handleFollowerReadIndex(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped ReadIndex, no leader", r.describe())
		r.reportDroppedReadIndex(m) // 记录被丢弃的读请求（用于监控和重试）
		return nil
	}
	m.To = r.leaderID // 转发目标设为当前领导者
	r.send(m) // 转发读请求给领导者
	return nil
}

// handleFollowerLeaderTransfer 处理跟随者节点收到的 LeaderTransfer 请求（领导者转移）。
// 跟随者不参与转移逻辑，仅转发请求给当前领导者（若存在）。
// 参数：
//   - m: 包含转移目标的请求消息
// 返回值：
//   - error: 处理过程中遇到的错误（如无领导者时记录警告）
func (r *raft) handleFollowerLeaderTransfer(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped LeaderTransfer, no leader", r.describe())
		return nil
	}
	m.To = r.leaderID // 转发目标设为当前领导者
	r.send(m) // 转发转移请求给领导者
	return nil
}

// handleFollowerReadIndexResp 处理跟随者节点收到的 ReadIndexResp 消息（读请求响应）。
// 记录读请求结果，用于客户端线性一致性读确认。
// 参数：
//   - m: 包含读索引和上下文的响应消息
// 返回值：
//   - error: 处理过程中遇到的错误
func (r *raft) handleFollowerReadIndexResp(m pb.Message) error {
	ctx := pb.SystemCtx{
		Low:  m.Hint,  // 读请求上下文低 64 位
		High: m.HintHigh, // 读请求上下文高 64 位
	}
	r.leaderIsAvailable() // 标记领导者可用，重置选举计时器
	r.setLeaderID(m.From) // 更新当前领导者 ID（消息发送者）
	r.addReadyToRead(m.LogIndex, ctx) // 记录已就绪的读索引（供客户端读取）
	return nil
}

// handleFollowerInstallSnapshot 处理跟随者节点收到的 InstallSnapshot 消息（快照安装请求）。
// 更新领导者状态并调用通用快照逻辑安装快照（用于快速同步落后节点）。
// 参数：
//   - m: 包含快照数据的消息
// 返回值：
//   - error: 快照安装过程中遇到的错误（如快照验证失败）
func (r *raft) handleFollowerInstallSnapshot(m pb.Message) error {
	r.leaderIsAvailable() // 标记领导者可用，重置选举计时器
	r.setLeaderID(m.From) // 更新当前领导者 ID（消息发送者）
	return r.handleInstallSnapshotMessage(m) // 调用通用快照逻辑安装快照
}

// handleFollowerTimeoutNow 处理跟随者节点收到的 TimeoutNow 消息（立即超时）。
// 触发节点立即发起选举（用于领导者转移协议，参考 Raft 论文 3.10 节）。
// 参数：
//   - m: 触发超时的消息
// 返回值：
//   - error: 选举触发过程中遇到的错误
func (r *raft) handleFollowerTimeoutNow(m pb.Message) error {
	// Raft 论文 3.10 节：TimeoutNow 消息使目标节点立即超时并发起选举，加速领导者转移
	plog.Debugf("%s TimeoutNow received", r.describe())
	r.electionTick = r.randomizedElectionTimeout // 强制选举计时器达到超时阈值
	r.isLeaderTransferTarget = true // 标记为领导者转移目标
	if err := r.tick(); err != nil { // 触发定时任务，进而触发选举
		return err
	}
	if r.isLeaderTransferTarget { // 重置转移目标标记（选举完成后）
		r.isLeaderTransferTarget = false
	}
	return nil
}

//
// handler functions used by candidate
//

// doubleCheckTermMatched 二次检查消息任期与本地任期是否一致（防御性编程）。
// 确保非预选举消息的任期与节点当前任期严格匹配，避免协议状态不一致。
func (r *raft) doubleCheckTermMatched(msgTerm uint64) {
	if msgTerm != 0 && r.term != msgTerm {
		plog.Panicf("%s mismatched term found", r.describe())
	}
}

// handleCandidatePropose 处理候选者节点收到的 Propose 消息（客户端提案）。
// 候选者不处理提案，直接丢弃并记录错误（仅领导者可处理提案）。
// 参数：
//   - m: 包含提案条目的消息
// 返回值：
//   - error: 错误信息（始终为 nil，仅记录警告）
func (r *raft) handleCandidatePropose(m pb.Message) error {
	plog.Warningf("%s dropped proposal, no leader", r.describe())
	r.reportDroppedProposal(m) // 记录被丢弃的提案
	return nil
}

// handleCandidateReadIndex 处理候选者节点收到的 ReadIndex 请求（线性一致性读）。
// 候选者不处理读请求，直接丢弃并记录错误（仅领导者可处理读请求）。
// 参数：
//   - m: 包含读请求上下文的消息
// 返回值：
//   - error: 错误信息（始终为 nil，仅记录警告）
func (r *raft) handleCandidateReadIndex(m pb.Message) error {
	plog.Warningf("%s dropped read index, no leader", r.describe())
	r.reportDroppedReadIndex(m) // 记录被丢弃的读请求
	ctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	r.droppedReadIndexes = append(r.droppedReadIndexes, ctx) // 缓存读请求上下文
	return nil
}

// handleCandidateReplicate 处理候选者节点收到的 Replicate 消息（日志复制请求）。
// 若消息任期与当前任期一致，证明存在合法领导者，候选者退化为跟随者。
// 参数：
//   - m: 包含日志条目的复制消息
// 返回值：
//   - error: 状态转换或日志处理错误
func (r *raft) handleCandidateReplicate(m pb.Message) error {
	// Raft 论文 5.2 节：若候选者收到领导者的复制请求（任期相同），则退化为跟随者
	r.becomeFollower(r.term, m.From)
	return r.handleReplicateMessage(m) // 处理日志复制
}

// handleCandidateInstallSnapshot 处理候选者节点收到的 InstallSnapshot 消息（快照请求）。
// 若消息任期与当前任期一致，证明存在合法领导者，候选者退化为跟随者。
// 参数：
//   - m: 包含快照数据的消息
// 返回值：
//   - error: 状态转换或快照处理错误
func (r *raft) handleCandidateInstallSnapshot(m pb.Message) error {
	// Raft 论文 5.2 节：若候选者收到领导者的快照请求（任期相同），则退化为跟随者
	r.becomeFollower(r.term, m.From)
	return r.handleInstallSnapshotMessage(m) // 处理快照安装
}

// handleCandidateHeartbeat 处理候选者节点收到的 Heartbeat 消息（领导者心跳）。
// 若消息任期与当前任期一致，证明存在合法领导者，候选者退化为跟随者。
// 参数：
//   - m: 包含提交索引的心跳消息
// 返回值：
//   - error: 状态转换或心跳处理错误
func (r *raft) handleCandidateHeartbeat(m pb.Message) error {
	// Raft 论文 5.2 节：若候选者收到领导者的心跳（任期相同），则退化为跟随者
	r.becomeFollower(r.term, m.From)
	return r.handleHeartbeatMessage(m) // 处理心跳（更新提交索引）
}

// handleCandidateRequestVoteResp 处理候选者节点收到的 RequestVoteResp 消息（投票响应）。
// 统计赞成票数，若达到法定人数则晋升为领导者；若反对票达法定人数则退化为跟随者。
// 参数：
//   - m: 包含投票结果的响应消息
// 返回值：
//   - error: 选举结果处理错误（如晋升领导者失败）
func (r *raft) handleCandidateRequestVoteResp(m pb.Message) error {
	// 忽略非投票成员的投票响应
	if _, ok := r.nonVotings[m.From]; ok {
		plog.Warningf("dropped RequestVoteResp from nonVoting")
		return nil
	}

	// 统计当前赞成票数
	count := r.handleVoteResp(m.From, m.Reject, false)
	plog.Warningf("%s received %d votes and %d rejections, quorum is %d",
		r.describe(), count, len(r.votes)-count, r.quorum())

	// Raft 论文 5.2 节：若获得多数派赞成票，晋升为领导者
	if count == r.quorum() {
		if err := r.becomeLeader(); err != nil {
			return err
		}
		// 立即广播复制消息，提交 dummy 条目以确立领导权
		r.broadcastReplicateMessage()
	} else if len(r.votes)-count == r.quorum() { // 反对票达法定人数，退化为跟随者
		// 参考 etcd raft 实现：避免候选者长期占用资源，主动退化为跟随者
		r.becomeFollower(r.term, NoLeader)
	}
	return nil
}

//
// handler functions for preVote candidate
//

// handlePreVoteCandidateRequestPreVoteResp 处理预选举候选者收到的 RequestPreVoteResp 消息（预投票响应）。
// 统计预赞成票数，若达到法定人数则进入正式选举；若反对票达法定人数则退化为跟随者。
// 参数：
//   - m: 包含预投票结果的响应消息
// 返回值：
//   - error: 预选举结果处理错误（如正式选举启动失败）
func (r *raft) handlePreVoteCandidateRequestPreVoteResp(m pb.Message) error {
	// 忽略非投票成员的预投票响应
	if _, ok := r.nonVotings[m.From]; ok {
		plog.Warningf("dropped RequestPreVoteResp from nonVoting")
		return nil
	}

	// 统计当前预赞成票数
	count := r.handleVoteResp(m.From, m.Reject, true)
	plog.Warningf("%s received %d preVotes and %d rejections, quorum is %d",
		r.describe(), count, len(r.votes)-count, r.quorum())

	// 预选举获得多数派支持，进入正式选举
	if count == r.quorum() {
		if err := r.campaign(); err != nil {
			return err
		}
	} else if len(r.votes)-count == r.quorum() { // 预选举反对票达法定人数，退化为跟随者
		// 参考 etcd raft 实现：避免预选举候选者长期占用资源
		r.becomeFollower(r.term, NoLeader)
	}
	return nil
}

// reportDroppedConfigChange 记录被丢弃的配置变更条目（用于监控和重试）。
// 当存在未提交的配置变更时，新的配置变更请求会被丢弃，确保协议安全性。
func (r *raft) reportDroppedConfigChange(e pb.Entry) {
	r.droppedEntries = append(r.droppedEntries, e)
}

// reportDroppedProposal 记录被丢弃的提案条目（用于监控和重试）。
// 当集群无领导者或领导者转移中，提案会被丢弃，避免不一致。
func (r *raft) reportDroppedProposal(m pb.Message) {
	r.droppedEntries = append(r.droppedEntries, newEntrySlice(m.Entries)...)
	// 触发提案丢弃事件（供外部监控）
	if r.events != nil {
		info := server.ProposalInfo{
			ShardID:   r.shardID,
			ReplicaID: r.replicaID,
			Entries:   m.Entries,
		}
		r.events.ProposalDropped(info)
	}
}

// reportDroppedReadIndex 记录被丢弃的读请求（用于监控和重试）。
// 当集群无领导者时，ReadIndex 请求会被丢弃，确保读操作安全性。
func (r *raft) reportDroppedReadIndex(m pb.Message) {
	sysctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	r.droppedReadIndexes = append(r.droppedReadIndexes, sysctx)
	// 触发读请求丢弃事件（供外部监控）
	if r.events != nil {
		info := server.ReadIndexInfo{
			ShardID:   r.shardID,
			ReplicaID: r.replicaID,
		}
		r.events.ReadIndexDropped(info)
	}
}

func (r *raft) handleHeartbeatMessage(m pb.Message) error {
	r.log.commitTo(m.Commit)
	r.send(pb.Message{
		To:       m.From,
		Type:     pb.HeartbeatResp,
		Hint:     m.Hint,
		HintHigh: m.HintHigh,
	})
	return nil
}

func (r *raft) handleInstallSnapshotMessage(m pb.Message) error {
	plog.Debugf("%s called handleInstallSnapshotMessage with snapshot from %s",
		r.describe(), ReplicaID(m.From))
	index, term := m.Snapshot.Index, m.Snapshot.Term
	resp := pb.Message{
		To:   m.From,
		Type: pb.ReplicateResp,
	}
	ok, err := r.restore(m.Snapshot)
	if err != nil {
		return err
	}
	if ok {
		plog.Debugf("%s restored snapshot %d term %d", r.describe(), index, term)
		resp.LogIndex = r.log.lastIndex()
	} else {
		plog.Debugf("%s rejected snapshot %d term %d", r.describe(), index, term)
		resp.LogIndex = r.log.committed
		if r.events != nil {
			info := server.SnapshotInfo{
				ShardID:   r.shardID,
				ReplicaID: r.replicaID,
				Index:     m.Snapshot.Index,
				Term:      m.Snapshot.Term,
				From:      m.From,
			}
			r.events.SnapshotRejected(info)
		}
	}
	r.send(resp)
	return nil
}

func (r *raft) handleReplicateMessage(m pb.Message) error {
	resp := pb.Message{
		To:   m.From,
		Type: pb.ReplicateResp,
	}
	if m.LogIndex < r.log.committed {
		resp.LogIndex = r.log.committed
		r.send(resp)
		return nil
	}
	ok, err := r.log.matchTerm(m.LogIndex, m.LogTerm)
	if err != nil {
		return err
	}
	if ok {
		if _, err := r.log.tryAppend(m.LogIndex, m.Entries); err != nil {
			return err
		}
		lastIdx := m.LogIndex + uint64(len(m.Entries))
		r.log.commitTo(min(lastIdx, m.Commit))
		resp.LogIndex = lastIdx
	} else {
		plog.Debugf("%s rejected Replicate index %d term %d from %s",
			r.describe(), m.LogIndex, m.Term, ReplicaID(m.From))
		resp.Reject = true
		resp.LogIndex = m.LogIndex
		resp.Hint = r.log.lastIndex()
		if r.events != nil {
			info := server.ReplicationInfo{
				ShardID:   r.shardID,
				ReplicaID: r.replicaID,
				Index:     m.LogIndex,
				Term:      m.LogTerm,
				From:      m.From,
			}
			r.events.ReplicationRejected(info)
		}
	}
	r.send(resp)
	return nil
}

//
// Step related functions
//

func isPreVoteMessage(t pb.MessageType) bool {
	return t == pb.RequestPreVote || t == pb.RequestPreVoteResp
}

func isRequestVoteMessage(t pb.MessageType) bool {
	return t == pb.RequestVote || t == pb.RequestPreVote
}

func isRequestMessage(t pb.MessageType) bool {
	return t == pb.Propose || t == pb.ReadIndex || t == pb.LeaderTransfer
}

func isLeaderMessage(t pb.MessageType) bool {
	return t == pb.Replicate || t == pb.InstallSnapshot ||
		t == pb.Heartbeat || t == pb.TimeoutNow || t == pb.ReadIndexResp
}

func (r *raft) dropRequestVoteFromHighTermNode(m pb.Message) bool {
	if !isRequestVoteMessage(m.Type) || !r.checkQuorum || m.Term <= r.term {
		return false
	}
	// see p42 of the raft thesis
	if m.Hint == m.From {
		plog.Debugf("%s, RequestVote with leader transfer hint received from %s",
			r.describe(), ReplicaID(m.From))
		return false
	}
	if r.isLeader() && !r.quiesce && r.electionTick >= r.electionTimeout {
		panic("r.electionTick >= r.electionTimeout on leader")
	}
	// we got a RequestVote with higher term, but we recently had heartbeat msg
	// from leader within the minimum election timeout and that leader is known
	// to have quorum. we thus drop such RequestVote to minimize interruption by
	// network partitioned nodes with higher term.
	// this idea is from the last paragraph of the section 6 of the raft paper
	if r.leaderID != NoLeader && r.electionTick < r.electionTimeout {
		return true
	}
	return false
}

func isPreVoteMessageWithExpectedHigherTerm(m pb.Message) bool {
	return m.Type == pb.RequestPreVote ||
		(m.Type == pb.RequestPreVoteResp && !m.Reject)
}

// onMessageTermNotMatched handles the situation in which the incoming
// message has a term value different from local node's term. it returns a
// boolean flag indicating whether the message should be ignored.
// see the 3rd paragraph, section 5.1 of the raft paper for details.
func (r *raft) onMessageTermNotMatched(m pb.Message) bool {
	if m.Term == 0 || m.Term == r.term {
		return false
	}
	if r.dropRequestVoteFromHighTermNode(m) {
		plog.Warningf("%s dropped RequestVote at term %d from %s, leader available",
			r.describe(), m.Term, ReplicaID(m.From))
		return true
	}
	if m.Term > r.term {
		if !isPreVoteMessageWithExpectedHigherTerm(m) {
			plog.Warningf("%s received %s with higher term (%d) from %s",
				r.describe(), m.Type, m.Term, ReplicaID(m.From))
			leaderID := NoLeader
			if isLeaderMessage(m.Type) {
				leaderID = m.From
			}
			if r.isNonVoting() {
				r.becomeNonVoting(m.Term, leaderID)
			} else if r.isWitness() {
				r.becomeWitness(m.Term, leaderID)
			} else {
				if m.Type == pb.RequestVote {
					plog.Warningf("%s become followerKE after receiving higher term from %s",
						r.describe(), ReplicaID(m.From))
					// not to reset the electionTick value to avoid the risk of having the
					// local node not being to campaign at all. if the local node generates
					// the tick much slower than other nodes (e.g. bad config, hardware
					// clock issue, bad scheduling, overloaded etc.), it may lose the chance
					// to ever start a campaign unless we keep its electionTick value here.
					r.becomeFollowerKE(m.Term, leaderID)
				} else {
					plog.Warningf("%s become follower after receiving higher term from %s",
						r.describe(), ReplicaID(m.From))
					r.becomeFollower(m.Term, leaderID)
				}
			}
		}
	} else if m.Term < r.term {
		if m.Type == pb.RequestPreVote ||
			(isLeaderMessage(m.Type) && (r.checkQuorum || r.preVote)) {
			// see test TestFreeStuckCandidateWithCheckQuorum for details
			r.send(pb.Message{To: m.From, Type: pb.NoOP})
		} else {
			plog.Infof("%s ignored %s with lower term (%d) from %s",
				r.describe(), m.Type, m.Term, ReplicaID(m.From))
		}
		return true
	}
	return false
}

func (r *raft) inconsistentRaftConfig(m pb.Message) bool {
	return !r.preVote && isPreVoteMessage(m.Type)
}

func (r *raft) Handle(m pb.Message) error {
	if r.inconsistentRaftConfig(m) {
		panic("received preVote message when preVote is not enabled")
	}
	if !r.onMessageTermNotMatched(m) {
		if !isPreVoteMessage(m.Type) {
			r.doubleCheckTermMatched(m.Term)
		}
		return r.handle(r, m)
	}
	plog.Infof("%s dropped %s from %s, term %d, term not matched",
		r.describe(), m.Type, ReplicaID(m.From), m.Term)
	return nil
}

func (r *raft) hasConfigChangeToApply() bool {
	// this is a hack to make it easier to port etcd raft tests
	// check those *_etcd_test.go for details
	if r.hasNotAppliedConfigChange != nil {
		return r.hasNotAppliedConfigChange()
	}
	// TODO:
	// with the current entry log implementation, the simplification below is no
	// longer required, we can now actually scan the committed but not applied
	// portion of the log as they are now all in memory.
	return r.log.committed > r.getApplied()
}

func (r *raft) canGrantVote(m pb.Message) bool {
	return r.vote == NoNode || r.vote == m.From || m.Term > r.term
}

//
// handlers for nodes in any state
//

func (r *raft) handleNodeElection(m pb.Message) error {
	if !r.isLeader() {
		// there can be multiple pending membership change entries committed but not
		// applied on this node. say with a shard of X, Y and Z, there are two
		// such entries for adding node A and B are committed but not applied
		// available on X. If X is allowed to start a new election, it can become the
		// leader with a vote from any one of the node Y or Z. Further proposals made
		// by the new leader X in the next term will require a quorum of 2 which can
		// have no overlap with the committed quorum of 3. this violates the safety
		// requirement of raft.
		// ignore the Election message when there is membership configure change
		// committed but not applied
		if r.hasConfigChangeToApply() {
			plog.Warningf("%s campaign skipped, pending config change",
				r.describe())
			if r.events != nil {
				info := server.CampaignInfo{
					ShardID:   r.shardID,
					ReplicaID: r.replicaID,
					Term:      r.term,
				}
				r.events.CampaignSkipped(info)
			}
			return nil
		}
		// prevote is enabled, but the user explicitly requested the leadership to
		// be transferred, so skip the pre-vote stage
		if r.preVote && !r.isLeaderTransferTarget {
			plog.Debugf("%s will start a preVote campaign", r.describe())
			return r.preVoteCampaign()
		}
		plog.Debugf("%s will start a campaign", r.describe())
		return r.campaign()
	}
	plog.Debugf("%s is leader, ignored Election", r.describe())
	return nil
}

func (r *raft) handleNodeRequestPreVote(m pb.Message) error {
	resp := pb.Message{
		To:   m.From,
		Type: pb.RequestPreVoteResp,
	}
	isUpToDate, err := r.log.upToDate(m.LogIndex, m.LogTerm)
	if err != nil {
		return err
	}
	if m.Term < r.term {
		panic("m.term < r.term")
	}
	if m.Term > r.term && isUpToDate {
		resp.Term = m.Term
		plog.Warningf("%s cast preVote from %s index %d term %d, log term: %d",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term, m.LogTerm)
	} else {
		// m.Term == r.term || !isUpToDate
		plog.Warningf("%s rejected preVote %s index %d term %d,logterm %d, utd %t",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term, m.LogTerm, isUpToDate)
		resp.Term = r.term
		resp.Reject = true
	}
	r.send(resp)
	return nil
}

func (r *raft) handleNodeRequestVote(m pb.Message) error {
	resp := pb.Message{
		To:   m.From,
		Type: pb.RequestVoteResp,
	}
	// 3rd paragraph section 5.2 of the raft paper
	canGrant := r.canGrantVote(m)
	// 2nd paragraph section 5.4 of the raft paper
	isUpToDate, err := r.log.upToDate(m.LogIndex, m.LogTerm)
	if err != nil {
		return err
	}
	if canGrant && isUpToDate {
		plog.Warningf("%s cast vote from %s index %d term %d, log term: %d",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term, m.LogTerm)
		r.electionTick = 0
		r.vote = m.From
	} else {
		plog.Warningf("%s rejected vote %s index%d term%d,logterm%d,grant%v,utd%v",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term,
			m.LogTerm, canGrant, isUpToDate)
		resp.Reject = true
	}
	r.send(resp)
	return nil
}

func (r *raft) handleNodeConfigChange(m pb.Message) error {
	if m.Reject {
		r.clearPendingConfigChange()
	} else {
		cctype := (pb.ConfigChangeType)(m.HintHigh)
		nodeid := m.Hint
		switch cctype {
		case pb.AddNode:
			r.addNode(nodeid)
		case pb.RemoveNode:
			if err := r.removeNode(nodeid); err != nil {
				return err
			}
		case pb.AddNonVoting:
			r.addNonVoting(nodeid)
		case pb.AddWitness:
			r.addWitness(nodeid)
		default:
			panic("unexpected config change type")
		}
	}
	return nil
}

func (r *raft) handleLogQuery(m pb.Message) error {
	if r.logQueryResult == nil {
		entries, err := r.log.getCommittedEntries(m.From, m.To, m.Hint)
		r.logQueryResult = &pb.LogQueryResult{
			FirstIndex: r.log.firstIndex(),
			LastIndex:  r.log.committed + 1,
			Error:      err,
			Entries:    entries,
		}
	} else {
		panic("log query result is not nil")
	}
	return nil
}

func (r *raft) handleLocalTick(m pb.Message) error {
	if m.Reject {
		r.quiescedTick()
		return nil
	}
	return r.tick()
}

func (r *raft) handleRestoreRemote(m pb.Message) error {
	r.restoreRemotes(m.Snapshot)
	return nil
}

//
// message handler functions used by leader
//

func (r *raft) handleLeaderHeartbeat(m pb.Message) error {
	r.broadcastHeartbeatMessage()
	return nil
}

// p69 of the raft thesis
func (r *raft) handleLeaderCheckQuorum(m pb.Message) error {
	r.mustBeLeader()
	if !r.leaderHasQuorum() {
		plog.Warningf("%s has lost quorum", r.describe())
		r.becomeFollower(r.term, NoLeader)
	}
	return nil
}

func (r *raft) handleLeaderPropose(m pb.Message) error {
	r.mustBeLeader()
	if r.leaderTransfering() {
		plog.Warningf("%s dropped proposal, leader transferring", r.describe())
		r.reportDroppedProposal(m)
		return nil
	}
	for i, e := range m.Entries {
		if e.Type == pb.ConfigChangeEntry {
			if r.hasPendingConfigChange() {
				plog.Warningf("%s dropped config change, pending change", r.describe())
				r.reportDroppedConfigChange(m.Entries[i])
				m.Entries[i] = pb.Entry{Type: pb.ApplicationEntry}
			}
			r.setPendingConfigChange()
		}
	}
	if err := r.appendEntries(m.Entries); err != nil {
		return err
	}
	r.broadcastReplicateMessage()
	return nil
}

// p72 of the raft thesis
func (r *raft) hasCommittedEntryAtCurrentTerm() bool {
	if r.term == 0 {
		panic("not suppose to reach here")
	}
	lastCommittedTerm, err := r.log.term(r.log.committed)
	if err != nil && !errors.Is(err, ErrCompacted) {
		plog.Panicf("%s failed to get term, %v", r.describe(), err)
	}
	return lastCommittedTerm == r.term
}

func (r *raft) clearReadyToRead() {
	r.readyToRead = r.readyToRead[:0]
}

func (r *raft) addReadyToRead(index uint64, ctx pb.SystemCtx) {
	r.readyToRead = append(r.readyToRead,
		pb.ReadyToRead{
			Index:     index,
			SystemCtx: ctx,
		})
}

// section 6.4 of the raft thesis
func (r *raft) handleLeaderReadIndex(m pb.Message) error {
	r.mustBeLeader()
	ctx := pb.SystemCtx{
		High: m.HintHigh,
		Low:  m.Hint,
	}
	if _, wok := r.witnesses[m.From]; wok {
		plog.Errorf("%s dropped ReadIndex, witness node %d", r.describe(), m.From)
	} else if !r.isSingleNodeQuorum() {
		if !r.hasCommittedEntryAtCurrentTerm() {
			// leader doesn't know the commit value of the shard
			// see raft thesis section 6.4, this is the first step of the ReadIndex
			// protocol.
			plog.Warningf("%s dropped ReadIndex, not ready", r.describe())
			r.reportDroppedReadIndex(m)
			return nil
		}
		r.readIndex.addRequest(r.log.committed, ctx, m.From)
		r.broadcastHeartbeatMessageWithHint(ctx)
	} else {
		r.addReadyToRead(r.log.committed, ctx)
		_, ook := r.nonVotings[m.From]
		if m.From != r.replicaID && ook {
			r.send(pb.Message{
				To:       m.From,
				Type:     pb.ReadIndexResp,
				LogIndex: r.log.committed,
				Hint:     m.Hint,
				HintHigh: m.HintHigh,
				Commit:   m.Commit,
			})
		}
	}
	return nil
}

func (r *raft) handleLeaderReplicateResp(m pb.Message, rp *remote) error {
	r.mustBeLeader()
	rp.setActive()
	if !m.Reject {
		paused := rp.isPaused()
		if rp.tryUpdate(m.LogIndex) {
			rp.respondedTo()
			ok, err := r.tryCommit()
			if err != nil {
				return nil
			}
			if ok {
				r.broadcastReplicateMessage()
			} else if paused {
				r.sendReplicateMessage(m.From)
			}
			// according to the leadership transfer protocol listed on the p29 of the
			// raft thesis
			if r.leaderTransfering() && m.From == r.leaderTransferTarget &&
				r.log.lastIndex() == rp.match {
				r.sendTimeoutNowMessage(r.leaderTransferTarget)
			}
		}
	} else {
		// the replication flow control code is derived from etcd raft, it resets
		// nextIndex to match + 1. it is thus even more conservative than the raft
		// thesis's approach of nextIndex = nextIndex - 1 mentioned on the p21 of
		// the thesis.
		if rp.decreaseTo(m.LogIndex, m.Hint) {
			r.enterRetryState(rp)
			r.sendReplicateMessage(m.From)
		}
	}
	return nil
}

func (r *raft) handleLeaderHeartbeatResp(m pb.Message, rp *remote) error {
	r.mustBeLeader()
	rp.setActive()
	rp.waitToRetry()
	if rp.match < r.log.lastIndex() {
		r.sendReplicateMessage(m.From)
	}
	// heartbeat response contains leadership confirmation requested as part of
	// the ReadIndex protocol.
	if m.Hint != 0 {
		r.handleReadIndexLeaderConfirmation(m)
	}
	return nil
}

func (r *raft) handleLeaderTransfer(m pb.Message) error {
	r.mustBeLeader()
	target := m.Hint
	plog.Debugf("%s called handleLeaderTransfer, target %d", r.describe(), target)
	if target == NoNode {
		plog.Panicf("%s leader transfer target not set", r.describe())
	}
	if r.leaderTransfering() {
		plog.Warningf("LeaderTransfer ignored, leader transfer is ongoing")
		return nil
	}
	if r.replicaID == target {
		plog.Warningf("received LeaderTransfer with target pointing to itself")
		return nil
	}
	rp, ok := r.remotes[target]
	if !ok {
		plog.Warningf("unknown LeaderTransfer target")
		return nil
	}
	r.leaderTransferTarget = target
	r.electionTick = 0
	// fast path below
	// or wait for the target node to catch up, see p29 of the raft thesis
	if rp.match == r.log.lastIndex() {
		r.sendTimeoutNowMessage(target)
	}
	return nil
}

func (r *raft) handleReadIndexLeaderConfirmation(m pb.Message) {
	ctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	ris := r.readIndex.confirm(ctx, m.From, r.quorum())
	for _, s := range ris {
		if s.from == NoNode || s.from == r.replicaID {
			r.addReadyToRead(s.index, s.ctx)
		} else {
			r.send(pb.Message{
				To:       s.from,
				Type:     pb.ReadIndexResp,
				LogIndex: s.index,
				Hint:     m.Hint,
				HintHigh: m.HintHigh,
			})
		}
	}
}

func (r *raft) handleLeaderSnapshotStatus(m pb.Message, rp *remote) error {
	if rp.state != remoteSnapshot {
		return nil
	}
	if m.Hint == 0 {
		if m.Reject {
			rp.clearPendingSnapshot()
			plog.Warningf("%s snapshot failed, %s is now in wait state",
				r.describe(), ReplicaID(m.From))
		} else {
			plog.Debugf("%s snapshot succeeded, %s in wait state now, next %d",
				r.describe(), ReplicaID(m.From), rp.next)
		}
		rp.becomeWait()
	} else {
		rp.setSnapshotAck(m.Hint, m.Reject)
		r.snapshotting = true
	}
	return nil
}

func (r *raft) handleLeaderUnreachable(m pb.Message, rp *remote) error {
	plog.Debugf("%s received Unreachable, %s entered retry state",
		r.describe(), ReplicaID(m.From))
	r.enterRetryState(rp)
	return nil
}

func (r *raft) handleLeaderRateLimit(m pb.Message) error {
	if r.rl.Enabled() {
		r.rl.SetFollowerState(m.From, m.Hint)
	} else {
		plog.Warningf("%s dropped rate limit msg, rl disabled", r.describe())
	}
	return nil
}

func (r *raft) enterRetryState(rp *remote) {
	if rp.state == remoteReplicate {
		rp.becomeRetry()
	}
}

func (r *raft) checkPendingSnapshotAck() error {
	if r.isLeader() && r.snapshotting {
		check := func(m map[uint64]*remote) error {
			for from, rp := range m {
				if rp.state == remoteSnapshot {
					if rp.delayed.tick() {
						if err := r.Handle(pb.Message{
							Type:   pb.SnapshotStatus,
							From:   from,
							Reject: rp.delayed.rejected,
							Hint:   0,
						}); err != nil {
							return err
						}
						rp.clearSnapshotAck()
					} else {
						r.snapshotting = true
					}
				}
			}
			return nil
		}
		r.snapshotting = false
		if err := check(r.remotes); err != nil {
			return err
		}
		if err := check(r.nonVotings); err != nil {
			return err
		}
		if err := check(r.witnesses); err != nil {
			return err
		}
	}
	return nil
}

//
// message handlers used by nonVoting, re-route them to follower handlers
//

func (r *raft) handleNonVotingReplicate(m pb.Message) error {
	return r.handleFollowerReplicate(m)
}

func (r *raft) handleNonVotingHeartbeat(m pb.Message) error {
	return r.handleFollowerHeartbeat(m)
}

func (r *raft) handleNonVotingSnapshot(m pb.Message) error {
	return r.handleFollowerInstallSnapshot(m)
}

func (r *raft) handleNonVotingPropose(m pb.Message) error {
	return r.handleFollowerPropose(m)
}

func (r *raft) handleNonVotingReadIndex(m pb.Message) error {
	return r.handleFollowerReadIndex(m)
}

func (r *raft) handleNonVotingReadIndexResp(m pb.Message) error {
	return r.handleFollowerReadIndexResp(m)
}

//
// message handlers used by witness, re-route them to follower handlers
//

func (r *raft) handleWitnessReplicate(m pb.Message) error {
	return r.handleFollowerReplicate(m)
}

func (r *raft) handleWitnessHeartbeat(m pb.Message) error {
	return r.handleFollowerHeartbeat(m)
}

func (r *raft) handleWitnessSnapshot(m pb.Message) error {
	return r.handleFollowerInstallSnapshot(m)
}

//
// message handlers used by follower
//

func (r *raft) handleFollowerPropose(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped proposal, no leader", r.describe())
		r.reportDroppedProposal(m)
		return nil
	}
	m.To = r.leaderID
	// the message might be queued by the transport layer, this violates the
	// requirement of the entryQueue.get() func. copy the m.Entries to its
	// own space.
	m.Entries = newEntrySlice(m.Entries)
	r.send(m)
	return nil
}

func (r *raft) leaderIsAvailable() {
	r.electionTick = 0
}

func (r *raft) handleFollowerReplicate(m pb.Message) error {
	r.leaderIsAvailable()
	r.setLeaderID(m.From)
	return r.handleReplicateMessage(m)
}

func (r *raft) handleFollowerHeartbeat(m pb.Message) error {
	r.leaderIsAvailable()
	r.setLeaderID(m.From)
	return r.handleHeartbeatMessage(m)
}

func (r *raft) handleFollowerReadIndex(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped ReadIndex, no leader", r.describe())
		r.reportDroppedReadIndex(m)
		return nil
	}
	m.To = r.leaderID
	r.send(m)
	return nil
}

func (r *raft) handleFollowerLeaderTransfer(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped LeaderTransfer, no leader", r.describe())
		return nil
	}
	m.To = r.leaderID
	r.send(m)
	return nil
}

func (r *raft) handleFollowerReadIndexResp(m pb.Message) error {
	ctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	r.leaderIsAvailable()
	r.setLeaderID(m.From)
	r.addReadyToRead(m.LogIndex, ctx)
	return nil
}

func (r *raft) handleFollowerInstallSnapshot(m pb.Message) error {
	r.leaderIsAvailable()
	r.setLeaderID(m.From)
	return r.handleInstallSnapshotMessage(m)
}

func (r *raft) handleFollowerTimeoutNow(m pb.Message) error {
	// the last paragraph, p29 of the raft thesis mentions that this is nothing
	// different from the clock moving forward quickly
	plog.Debugf("%s TimeoutNow received", r.describe())
	r.electionTick = r.randomizedElectionTimeout
	r.isLeaderTransferTarget = true
	if err := r.tick(); err != nil {
		return err
	}
	if r.isLeaderTransferTarget {
		r.isLeaderTransferTarget = false
	}
	return nil
}

//
// handler functions used by candidate
//

func (r *raft) doubleCheckTermMatched(msgTerm uint64) {
	if msgTerm != 0 && r.term != msgTerm {
		plog.Panicf("%s mismatched term found", r.describe())
	}
}

func (r *raft) handleCandidatePropose(m pb.Message) error {
	plog.Warningf("%s dropped proposal, no leader", r.describe())
	r.reportDroppedProposal(m)
	return nil
}

func (r *raft) handleCandidateReadIndex(m pb.Message) error {
	plog.Warningf("%s dropped read index, no leader", r.describe())
	r.reportDroppedReadIndex(m)
	ctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	r.droppedReadIndexes = append(r.droppedReadIndexes, ctx)
	return nil
}

// when any of the following three methods
// handleCandidateReplicate
// handleCandidateInstallSnapshot
// handleCandidateHeartbeat
// is called, it implies that m.Term == r.term and there is a leader
// for that term. see 4th paragraph section 5.2 of the raft paper
func (r *raft) handleCandidateReplicate(m pb.Message) error {
	r.becomeFollower(r.term, m.From)
	return r.handleReplicateMessage(m)
}

func (r *raft) handleCandidateInstallSnapshot(m pb.Message) error {
	r.becomeFollower(r.term, m.From)
	return r.handleInstallSnapshotMessage(m)
}

func (r *raft) handleCandidateHeartbeat(m pb.Message) error {
	r.becomeFollower(r.term, m.From)
	return r.handleHeartbeatMessage(m)
}

func (r *raft) handleCandidateRequestVoteResp(m pb.Message) error {
	if _, ok := r.nonVotings[m.From]; ok {
		plog.Warningf("dropped RequestVoteResp from nonVoting")
		return nil
	}
	count := r.handleVoteResp(m.From, m.Reject, false)
	plog.Warningf("%s received %d votes and %d rejections, quorum is %d",
		r.describe(), count, len(r.votes)-count, r.quorum())
	// 3rd paragraph section 5.2 of the raft paper
	if count == r.quorum() {
		if err := r.becomeLeader(); err != nil {
			return err
		}
		// get the NoOP entry committed ASAP
		r.broadcastReplicateMessage()
	} else if len(r.votes)-count == r.quorum() {
		// etcd raft does this, it is not stated in the raft paper
		r.becomeFollower(r.term, NoLeader)
	}
	return nil
}

//
// handler functions for preVote candidate
//

func (r *raft) handlePreVoteCandidateRequestPreVoteResp(m pb.Message) error {
	if _, ok := r.nonVotings[m.From]; ok {
		plog.Warningf("dropped RequestPreVoteResp from nonVoting")
		return nil
	}
	count := r.handleVoteResp(m.From, m.Reject, true)
	plog.Warningf("%s received %d preVotes and %d rejections, quorum is %d",
		r.describe(), count, len(r.votes)-count, r.quorum())
	if count == r.quorum() {
		if err := r.campaign(); err != nil {
			return err
		}
	} else if len(r.votes)-count == r.quorum() {
		// etcd raft does this, it is not stated in the raft paper
		r.becomeFollower(r.term, NoLeader)
	}
	return nil
}

func (r *raft) reportDroppedConfigChange(e pb.Entry) {
	r.droppedEntries = append(r.droppedEntries, e)
}

func (r *raft) reportDroppedProposal(m pb.Message) {
	r.droppedEntries = append(r.droppedEntries, newEntrySlice(m.Entries)...)
	if r.events != nil {
		info := server.ProposalInfo{
			ShardID:   r.shardID,
			ReplicaID: r.replicaID,
			Entries:   m.Entries,
		}
		r.events.ProposalDropped(info)
	}
}

func (r *raft) reportDroppedReadIndex(m pb.Message) {
	sysctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	r.droppedReadIndexes = append(r.droppedReadIndexes, sysctx)
	if r.events != nil {
		info := server.ReadIndexInfo{
			ShardID:   r.shardID,
			ReplicaID: r.replicaID,
		}
		r.events.ReadIndexDropped(info)
	}
}
/*
核心逻辑说明：
lw 函数：作为消息处理的“前置解析器”，通过查找发送者节点类型（投票/非投票/见证），将消息与对应节点的复制状态（*remote）绑定后传递给实际处理器（如 handleLeaderReplicateResp）。避免了在每个处理器中重复编写节点类型判断逻辑，统一处理未知节点的异常情况。

defaultHandle 函数：实现 Raft 协议的“状态-消息”路由中枢。通过查询 handlers 映射（状态→消息类型→处理器），确保节点在不同状态下对消息的处理符合协议规范（如跟随者转发提案，领导者直接处理提案）。

initializeHandlerMap 函数：通过为每个状态注册专用处理器，构建了 Raft 节点的“状态机行为矩阵”。关键设计亮点：

状态隔离：不同状态（如领导者/跟随者）对同一消息（如 Propose）的处理逻辑完全隔离，避免状态混淆。
复用与扩展：预选举候选者复用候选者的大部分处理器，仅替换预投票响应逻辑；非投票/见证成员复用跟随者处理器，减少代码冗余。
安全性保障：领导者的响应类处理器（如 ReplicateResp）通过 lw 包装，确保仅处理集群内已知节点的消息，防御恶意节点攻击。
通过上述机制，Raft 节点能够根据自身状态和消息类型，安全、高效地路由和处理网络消息，保障集群的一致性与可用性。
*/

// lw (lookup wrapper) 是消息处理包装器，根据消息发送者（From）查找对应的远程节点状态（投票成员/非投票成员/见证成员），
// 并将消息与远程节点状态传递给实际处理函数 f。用于统一处理不同类型节点的响应消息（如 ReplicateResp/HeartbeatResp）。
// 参数：
//   - r: raft 节点实例
//   - f: 实际消息处理函数，接收消息和对应远程节点状态（*remote）
// 返回值：
//   - handlerFunc: 包装后的消息处理函数，实现对发送者节点类型的自动识别
func lw(r *raft, f func(m pb.Message, rp *remote) error) handlerFunc {
	w := func(nm pb.Message) error {
		// 按优先级查找发送者节点类型：投票成员 > 非投票成员 > 见证成员
		if npr, ok := r.remotes[nm.From]; ok {
			return f(nm, npr) // 投票成员：使用 remotes 中的远程状态
		} else if nob, ok := r.nonVotings[nm.From]; ok {
			return f(nm, nob) // 非投票成员：使用 nonVotings 中的远程状态
		} else if wob, ok := r.witnesses[nm.From]; ok {
			return f(nm, wob) // 见证成员：使用 witnesses 中的远程状态
		} else {
			// 未知节点：记录警告并忽略（可能是已移除的节点或网络异常消息）
			plog.Warningf("%s no remote for %s", r.describe(), ReplicaID(nm.From))
			return nil
		}
	}
	return w
}

// defaultHandle 是默认消息分发函数，根据节点当前状态（state）和消息类型（Type），
// 从 handlers 映射中查找并调用对应的处理函数。是 Raft 状态机消息处理的入口。
// 参数：
//   - r: raft 节点实例
//   - m: 待处理的消息
// 返回值：
//   - error: 处理过程中遇到的错误（无对应处理器时返回 nil）
func defaultHandle(r *raft, m pb.Message) error {
	// 从状态-消息类型映射中查找处理器（handlers 由 initializeHandlerMap 初始化）
	if f := r.handlers[r.state][m.Type]; f != nil {
		return f(m) // 调用对应状态下的消息处理器
	}
	return nil // 无匹配处理器，忽略消息
}

// initializeHandlerMap 初始化状态-消息类型到处理器的映射（handlers），
// 为每个 Raft 状态（候选者/预选举候选者/跟随者/领导者/非投票成员/见证成员）注册对应的消息处理函数，
// 确保消息按节点当前状态正确路由到专用处理器。
func (r *raft) initializeHandlerMap() {
	// candidate（候选者状态）：处理选举、投票响应、日志复制等消息
	r.handlers[candidate][pb.Heartbeat] = r.handleCandidateHeartbeat
	r.handlers[candidate][pb.Propose] = r.handleCandidatePropose
	r.handlers[candidate][pb.ReadIndex] = r.handleCandidateReadIndex
	r.handlers[candidate][pb.Replicate] = r.handleCandidateReplicate
	r.handlers[candidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot
	r.handlers[candidate][pb.RequestVoteResp] = r.handleCandidateRequestVoteResp
	r.handlers[candidate][pb.Election] = r.handleNodeElection
	r.handlers[candidate][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[candidate][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[candidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[candidate][pb.LocalTick] = r.handleLocalTick
	r.handlers[candidate][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[candidate][pb.LogQuery] = r.handleLogQuery

	// preVoteCandidate（预选举候选者状态）：处理预选举响应及其他与候选者共享的消息
	r.handlers[preVoteCandidate][pb.Heartbeat] = r.handleCandidateHeartbeat
	r.handlers[preVoteCandidate][pb.Propose] = r.handleCandidatePropose
	r.handlers[preVoteCandidate][pb.ReadIndex] = r.handleCandidateReadIndex
	r.handlers[preVoteCandidate][pb.Replicate] = r.handleCandidateReplicate
	r.handlers[preVoteCandidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot
	r.handlers[preVoteCandidate][pb.RequestPreVoteResp] = r.handlePreVoteCandidateRequestPreVoteResp // 预选举专用响应处理器
	r.handlers[preVoteCandidate][pb.Election] = r.handleNodeElection
	r.handlers[preVoteCandidate][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[preVoteCandidate][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[preVoteCandidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[preVoteCandidate][pb.LocalTick] = r.handleLocalTick
	r.handlers[preVoteCandidate][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[preVoteCandidate][pb.LogQuery] = r.handleLogQuery

	// follower（跟随者状态）：处理提案转发、日志复制、心跳、快照安装等消息
	r.handlers[follower][pb.Propose] = r.handleFollowerPropose
	r.handlers[follower][pb.Replicate] = r.handleFollowerReplicate
	r.handlers[follower][pb.Heartbeat] = r.handleFollowerHeartbeat
	r.handlers[follower][pb.ReadIndex] = r.handleFollowerReadIndex
	r.handlers[follower][pb.LeaderTransfer] = r.handleFollowerLeaderTransfer
	r.handlers[follower][pb.ReadIndexResp] = r.handleFollowerReadIndexResp
	r.handlers[follower][pb.InstallSnapshot] = r.handleFollowerInstallSnapshot
	r.handlers[follower][pb.Election] = r.handleNodeElection
	r.handlers[follower][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[follower][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[follower][pb.TimeoutNow] = r.handleFollowerTimeoutNow // 领导者转移触发超时
	r.handlers[follower][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[follower][pb.LocalTick] = r.handleLocalTick
	r.handlers[follower][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[follower][pb.LogQuery] = r.handleLogQuery

	// leader（领导者状态）：处理提案、读索引、复制响应、心跳响应、领导者转移等核心功能
	r.handlers[leader][pb.LeaderHeartbeat] = r.handleLeaderHeartbeat // 广播心跳
	r.handlers[leader][pb.CheckQuorum] = r.handleLeaderCheckQuorum // 检查 quorum 可用性
	r.handlers[leader][pb.Propose] = r.handleLeaderPropose // 处理客户端提案
	r.handlers[leader][pb.ReadIndex] = r.handleLeaderReadIndex // 线性一致性读
	r.handlers[leader][pb.ReplicateResp] = lw(r, r.handleLeaderReplicateResp) // 复制响应（经 lw 包装）
	r.handlers[leader][pb.HeartbeatResp] = lw(r, r.handleLeaderHeartbeatResp) // 心跳响应（经 lw 包装）
	r.handlers[leader][pb.SnapshotStatus] = lw(r, r.handleLeaderSnapshotStatus) // 快照状态（经 lw 包装）
	r.handlers[leader][pb.Unreachable] = lw(r, r.handleLeaderUnreachable) // 节点不可达（经 lw 包装）
	r.handlers[leader][pb.LeaderTransfer] = r.handleLeaderTransfer // 领导者转移
	r.handlers[leader][pb.Election] = r.handleNodeElection
	r.handlers[leader][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[leader][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[leader][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[leader][pb.LocalTick] = r.handleLocalTick
	r.handlers[leader][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[leader][pb.RateLimit] = r.handleLeaderRateLimit // 流量控制
	r.handlers[leader][pb.LogQuery] = r.handleLogQuery

	// nonVoting（非投票成员状态）：仅参与日志复制，不参与选举，复用跟随者处理器
	r.handlers[nonVoting][pb.Heartbeat] = r.handleNonVotingHeartbeat
	r.handlers[nonVoting][pb.Replicate] = r.handleNonVotingReplicate
	r.handlers[nonVoting][pb.InstallSnapshot] = r.handleNonVotingSnapshot
	r.handlers[nonVoting][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[nonVoting][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[nonVoting][pb.Propose] = r.handleNonVotingPropose
	r.handlers[nonVoting][pb.ReadIndex] = r.handleNonVotingReadIndex
	r.handlers[nonVoting][pb.ReadIndexResp] = r.handleNonVotingReadIndexResp
	r.handlers[nonVoting][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[nonVoting][pb.LocalTick] = r.handleLocalTick
	r.handlers[nonVoting][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[nonVoting][pb.LogQuery] = r.handleLogQuery

	// witness（见证成员状态）：仅参与法定人数计算，不存储完整日志，复用跟随者处理器
	r.handlers[witness][pb.Heartbeat] = r.handleWitnessHeartbeat
	r.handlers[witness][pb.Replicate] = r.handleWitnessReplicate
	r.handlers[witness][pb.InstallSnapshot] = r.handleWitnessSnapshot
	r.handlers[witness][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[witness][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[witness][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[witness][pb.LocalTick] = r.handleLocalTick
	r.handlers[witness][pb.SnapshotReceived] = r.handleRestoreRemote
}


// lw（lookup wrapper）是消息处理包装器，用于统一解析消息发送者的节点类型（投票成员/非投票成员/见证成员），
// 并将消息与对应节点的复制状态（*remote）传递给实际处理函数 f。解决不同类型节点（如非投票节点、见证节点）的响应处理共性问题。
// 参数：
//   - r: raft 节点实例，提供节点类型映射（remotes/nonVotings/witnesses）
//   - f: 实际消息处理函数（如 handleLeaderReplicateResp），需节点复制状态完成处理
// 返回值：
//   - handlerFunc: 包装后的处理器，自动适配发送者节点类型
func lw(r *raft, f func(m pb.Message, rp *remote) error) handlerFunc {
	w := func(nm pb.Message) error {
		// 按优先级查找发送者节点类型：投票成员 > 非投票成员 > 见证成员
		if npr, ok := r.remotes[nm.From]; ok {
			return f(nm, npr) // 投票成员：使用 remotes 中的复制状态
		} else if nob, ok := r.nonVotings[nm.From]; ok {
			return f(nm, nob) // 非投票成员：使用 nonVotings 中的复制状态
		} else if wob, ok := r.witnesses[nm.From]; ok {
			return f(nm, wob) // 见证成员：使用 witnesses 中的复制状态
		} else {
			// 未知节点：记录警告并忽略（可能为已移除节点或网络异常消息）
			plog.Warningf("%s no remote for %s", r.describe(), ReplicaID(nm.From))
			return nil
		}
	}
	return w
}

// defaultHandle 是 Raft 节点的消息分发入口，根据当前节点状态（state）和消息类型（Type），
// 从 handlers 映射中查找并调用对应的状态专用处理器。实现消息与状态的解耦，确保不同状态下消息处理逻辑隔离。
// 参数：
//   - r: raft 节点实例，包含当前状态和 handlers 映射
//   - m: 待处理的消息（如 Propose/Replicate/Heartbeat 等）
// 返回值：
//   - error: 处理器返回的错误（无匹配处理器时返回 nil）
func defaultHandle(r *raft, m pb.Message) error {
	if f := r.handlers[r.state][m.Type]; f != nil {
		return f(m) // 调用状态-类型对应的专用处理器
	}
	return nil // 无匹配处理器，忽略消息（如过期消息或状态切换期间的残留消息）
}

// initializeHandlerMap 初始化状态-消息类型到处理器的映射表（handlers），
// 为每个 Raft 状态（候选者/预选举候选者/跟随者/领导者/非投票成员/见证成员）注册对应的消息处理器。
// 核心作用：确保节点在不同状态下对各类消息的处理逻辑符合 Raft 协议规范，避免状态混淆导致的错误。
func (r *raft) initializeHandlerMap() {
	// candidate（候选者状态）：处理选举投票、日志复制请求等，核心是争取多数派投票以晋升领导者
	r.handlers[candidate][pb.Heartbeat] = r.handleCandidateHeartbeat          // 收到领导者心跳 → 退化为跟随者
	r.handlers[candidate][pb.Propose] = r.handleCandidatePropose              // 收到提案 → 丢弃（仅领导者可处理）
	r.handlers[candidate][pb.ReadIndex] = r.handleCandidateReadIndex          // 收到读请求 → 丢弃（仅领导者可处理）
	r.handlers[candidate][pb.Replicate] = r.handleCandidateReplicate          // 收到日志复制请求 → 退化为跟随者
	r.handlers[candidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot // 收到快照请求 → 退化为跟随者
	r.handlers[candidate][pb.RequestVoteResp] = r.handleCandidateRequestVoteResp // 收到投票响应 → 统计票数决定是否晋升
	r.handlers[candidate][pb.Election] = r.handleNodeElection                 // 收到选举触发消息 → 发起选举（如超时）
	r.handlers[candidate][pb.RequestVote] = r.handleNodeRequestVote           // 收到其他节点投票请求 → 按规则投票
	r.handlers[candidate][pb.RequestPreVote] = r.handleNodeRequestPreVote     // 收到预投票请求 → 按规则预投票
	r.handlers[candidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange    // 配置变更事件 → 应用配置
	r.handlers[candidate][pb.LocalTick] = r.handleLocalTick                   // 本地定时任务 → 检查选举超时
	r.handlers[candidate][pb.SnapshotReceived] = r.handleRestoreRemote        // 收到快照 → 恢复节点状态
	r.handlers[candidate][pb.LogQuery] = r.handleLogQuery                     // 日志查询 → 返回查询结果

	// preVoteCandidate（预选举候选者状态）：预选举阶段专用，避免网络分区导致的任期膨胀
	// 复用 candidate 的大部分处理器，仅替换预投票响应处理器
	r.handlers[preVoteCandidate][pb.Heartbeat] = r.handleCandidateHeartbeat
	r.handlers[preVoteCandidate][pb.Propose] = r.handleCandidatePropose
	r.handlers[preVoteCandidate][pb.ReadIndex] = r.handleCandidateReadIndex
	r.handlers[preVoteCandidate][pb.Replicate] = r.handleCandidateReplicate
	r.handlers[preVoteCandidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot
	r.handlers[preVoteCandidate][pb.RequestPreVoteResp] = r.handlePreVoteCandidateRequestPreVoteResp // 预投票响应 → 统计预投票结果
	r.handlers[preVoteCandidate][pb.Election] = r.handleNodeElection
	r.handlers[preVoteCandidate][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[preVoteCandidate][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[preVoteCandidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[preVoteCandidate][pb.LocalTick] = r.handleLocalTick
	r.handlers[preVoteCandidate][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[preVoteCandidate][pb.LogQuery] = r.handleLogQuery

	// follower（跟随者状态）：被动接收领导者消息，转发客户端请求，核心是维持与领导者的同步
	r.handlers[follower][pb.Propose] = r.handleFollowerPropose                // 收到提案 → 转发给领导者
	r.handlers[follower][pb.Replicate] = r.handleFollowerReplicate            // 收到日志复制 → 同步日志并响应
	r.handlers[follower][pb.Heartbeat] = r.handleFollowerHeartbeat            // 收到心跳 → 更新提交索引并响应
	r.handlers[follower][pb.ReadIndex] = r.handleFollowerReadIndex            // 收到读请求 → 转发给领导者
	r.handlers[follower][pb.LeaderTransfer] = r.handleFollowerLeaderTransfer  // 收到领导者转移请求 → 转发给领导者
	r.handlers[follower][pb.ReadIndexResp] = r.handleFollowerReadIndexResp    // 收到读响应 → 记录读索引（供客户端读取）
	r.handlers[follower][pb.InstallSnapshot] = r.handleFollowerInstallSnapshot // 收到快照 → 安装快照以快速同步
	r.handlers[follower][pb.Election] = r.handleNodeElection                 // 收到选举触发消息 → 发起选举（如超时）
	r.handlers[follower][pb.RequestVote] = r.handleNodeRequestVote           // 收到投票请求 → 按规则投票
	r.handlers[follower][pb.RequestPreVote] = r.handleNodeRequestPreVote     // 收到预投票请求 → 按规则预投票
	r.handlers[follower][pb.TimeoutNow] = r.handleFollowerTimeoutNow          // 收到立即超时消息 → 触发选举（领导者转移用）
	r.handlers[follower][pb.ConfigChangeEvent] = r.handleNodeConfigChange    // 配置变更事件 → 应用配置
	r.handlers[follower][pb.LocalTick] = r.handleLocalTick                   // 本地定时任务 → 检查选举超时
	r.handlers[follower][pb.SnapshotReceived] = r.handleRestoreRemote        // 收到快照 → 恢复节点状态
	r.handlers[follower][pb.LogQuery] = r.handleLogQuery                     // 日志查询 → 返回查询结果

	// leader（领导者状态）：主动处理客户端请求、复制日志、维持领导权，核心是保证集群一致性
	r.handlers[leader][pb.LeaderHeartbeat] = r.handleLeaderHeartbeat          // 收到心跳触发 → 广播心跳（维持领导权）
	r.handlers[leader][pb.CheckQuorum] = r.handleLeaderCheckQuorum            // 收到检查 quorum 消息 → 验证是否仍有多数派支持
	r.handlers[leader][pb.Propose] = r.handleLeaderPropose                    // 收到提案 → 追加日志并复制到集群
	r.handlers[leader][pb.ReadIndex] = r.handleLeaderReadIndex                // 收到读请求 → 执行 ReadIndex 协议（线性一致性读）
	r.handlers[leader][pb.ReplicateResp] = lw(r, r.handleLeaderReplicateResp) // 收到复制响应 → 更新复制进度，尝试提交日志
	r.handlers[leader][pb.HeartbeatResp] = lw(r, r.handleLeaderHeartbeatResp) // 收到心跳响应 → 确认节点存活，更新复制状态
	r.handlers[leader][pb.SnapshotStatus] = lw(r, r.handleLeaderSnapshotStatus) // 收到快照状态 → 处理快照复制结果（成功/失败）
	r.handlers[leader][pb.Unreachable] = lw(r, r.handleLeaderUnreachable)     // 收到节点不可达消息 → 进入重试状态
	r.handlers[leader][pb.LeaderTransfer] = r.handleLeaderTransfer            // 收到领导者转移请求 → 启动转移流程（如同步目标节点日志）
	r.handlers[leader][pb.Election] = r.handleNodeElection                   // 收到选举触发消息 → 忽略（自身已是领导者）
	r.handlers[leader][pb.RequestVote] = r.handleNodeRequestVote             // 收到投票请求 → 拒绝（自身任期更高）
	r.handlers[leader][pb.RequestPreVote] = r.handleNodeRequestPreVote       // 收到预投票请求 → 拒绝（自身任期更高）
	r.handlers[leader][pb.ConfigChangeEvent] = r.handleNodeConfigChange      // 配置变更事件 → 应用配置
	r.handlers[leader][pb.LocalTick] = r.handleLocalTick                     // 本地定时任务 → 检查心跳超时、触发日志复制
	r.handlers[leader][pb.SnapshotReceived] = r.handleRestoreRemote          // 收到快照 → 恢复节点状态
	r.handlers[leader][pb.RateLimit] = r.handleLeaderRateLimit                // 收到流量控制消息 → 调整复制速率
	r.handlers[leader][pb.LogQuery] = r.handleLogQuery                       // 日志查询 → 返回查询结果

	// nonVoting（非投票成员状态）：仅同步日志不参与选举，用于新节点加入时的数据预热
	r.handlers[nonVoting][pb.Heartbeat] = r.handleNonVotingHeartbeat          // 复用跟随者心跳处理器
	r.handlers[nonVoting][pb.Replicate] = r.handleNonVotingReplicate          // 复用跟随者日志复制处理器
	r.handlers[nonVoting][pb.InstallSnapshot] = r.handleNonVotingSnapshot     // 复用跟随者快照安装处理器
	r.handlers[nonVoting][pb.RequestVote] = r.handleNodeRequestVote           // 收到投票请求 → 拒绝（无投票权）
	r.handlers[nonVoting][pb.RequestPreVote] = r.handleNodeRequestPreVote     // 收到预投票请求 → 拒绝（无投票权）
	r.handlers[nonVoting][pb.Propose] = r.handleNonVotingPropose              // 收到提案 → 转发给领导者
	r.handlers[nonVoting][pb.ReadIndex] = r.handleNonVotingReadIndex          // 收到读请求 → 转发给领导者
	r.handlers[nonVoting][pb.ReadIndexResp] = r.handleNonVotingReadIndexResp  // 收到读响应 → 记录读索引
	r.handlers[nonVoting][pb.ConfigChangeEvent] = r.handleNodeConfigChange    // 配置变更事件 → 应用配置
	r.handlers[nonVoting][pb.LocalTick] = r.handleLocalTick                   // 本地定时任务 → 维持与领导者同步
	r.handlers[nonVoting][pb.SnapshotReceived] = r.handleRestoreRemote        // 收到快照 → 恢复节点状态
	r.handlers[nonVoting][pb.LogQuery] = r.handleLogQuery                     // 日志查询 → 返回查询结果

	// witness（见证成员状态）：仅参与法定人数计算，不存储完整日志，用于提升可用性
	r.handlers[witness][pb.Heartbeat] = r.handleWitnessHeartbeat              // 复用跟随者心跳处理器
	r.handlers[witness][pb.Replicate] = r.handleWitnessReplicate              // 复用跟随者日志复制处理器（仅同步关键元数据）
	r.handlers[witness][pb.InstallSnapshot] = r.handleWitnessSnapshot         // 复用跟随者快照安装处理器
	r.handlers[witness][pb.RequestVote] = r.handleNodeRequestVote             // 收到投票请求 → 按规则投票（仅参与法定人数）
	r.handlers[witness][pb.RequestPreVote] = r.handleNodeRequestPreVote       // 收到预投票请求 → 按规则预投票
	r.handlers[witness][pb.ConfigChangeEvent] = r.handleNodeConfigChange      // 配置变更事件 → 应用配置
	r.handlers[witness][pb.LocalTick] = r.handleLocalTick                     // 本地定时任务 → 维持与领导者同步
	r.handlers[witness][pb.SnapshotReceived] = r.handleRestoreRemote          // 收到快照 → 恢复节点状态
}

func lw(r *raft, f func(m pb.Message, rp *remote) error) handlerFunc {
	w := func(nm pb.Message) error {
		if npr, ok := r.remotes[nm.From]; ok {
			return f(nm, npr)
		} else if nob, ok := r.nonVotings[nm.From]; ok {
			return f(nm, nob)
		} else if wob, ok := r.witnesses[nm.From]; ok {
			return f(nm, wob)
		} else {
			plog.Warningf("%s no remote for %s", r.describe(), ReplicaID(nm.From))
			return nil
		}
	}
	return w
}

func defaultHandle(r *raft, m pb.Message) error {
	if f := r.handlers[r.state][m.Type]; f != nil {
		return f(m)
	}
	return nil
}

func (r *raft) initializeHandlerMap() {
	// candidate
	r.handlers[candidate][pb.Heartbeat] = r.handleCandidateHeartbeat
	r.handlers[candidate][pb.Propose] = r.handleCandidatePropose
	r.handlers[candidate][pb.ReadIndex] = r.handleCandidateReadIndex
	r.handlers[candidate][pb.Replicate] = r.handleCandidateReplicate
	r.handlers[candidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot
	r.handlers[candidate][pb.RequestVoteResp] = r.handleCandidateRequestVoteResp
	r.handlers[candidate][pb.Election] = r.handleNodeElection
	r.handlers[candidate][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[candidate][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[candidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[candidate][pb.LocalTick] = r.handleLocalTick
	r.handlers[candidate][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[candidate][pb.LogQuery] = r.handleLogQuery
	// prevote candidate
	r.handlers[preVoteCandidate][pb.Heartbeat] = r.handleCandidateHeartbeat
	r.handlers[preVoteCandidate][pb.Propose] = r.handleCandidatePropose
	r.handlers[preVoteCandidate][pb.ReadIndex] = r.handleCandidateReadIndex
	r.handlers[preVoteCandidate][pb.Replicate] = r.handleCandidateReplicate
	r.handlers[preVoteCandidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot
	r.handlers[preVoteCandidate][pb.RequestPreVoteResp] = r.handlePreVoteCandidateRequestPreVoteResp
	r.handlers[preVoteCandidate][pb.Election] = r.handleNodeElection
	r.handlers[preVoteCandidate][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[preVoteCandidate][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[preVoteCandidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[preVoteCandidate][pb.LocalTick] = r.handleLocalTick
	r.handlers[preVoteCandidate][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[preVoteCandidate][pb.LogQuery] = r.handleLogQuery
	// follower
	r.handlers[follower][pb.Propose] = r.handleFollowerPropose
	r.handlers[follower][pb.Replicate] = r.handleFollowerReplicate
	r.handlers[follower][pb.Heartbeat] = r.handleFollowerHeartbeat
	r.handlers[follower][pb.ReadIndex] = r.handleFollowerReadIndex
	r.handlers[follower][pb.LeaderTransfer] = r.handleFollowerLeaderTransfer
	r.handlers[follower][pb.ReadIndexResp] = r.handleFollowerReadIndexResp
	r.handlers[follower][pb.InstallSnapshot] = r.handleFollowerInstallSnapshot
	r.handlers[follower][pb.Election] = r.handleNodeElection
	r.handlers[follower][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[follower][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[follower][pb.TimeoutNow] = r.handleFollowerTimeoutNow
	r.handlers[follower][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[follower][pb.LocalTick] = r.handleLocalTick
	r.handlers[follower][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[follower][pb.LogQuery] = r.handleLogQuery
	// leader
	r.handlers[leader][pb.LeaderHeartbeat] = r.handleLeaderHeartbeat
	r.handlers[leader][pb.CheckQuorum] = r.handleLeaderCheckQuorum
	r.handlers[leader][pb.Propose] = r.handleLeaderPropose
	r.handlers[leader][pb.ReadIndex] = r.handleLeaderReadIndex
	r.handlers[leader][pb.ReplicateResp] = lw(r, r.handleLeaderReplicateResp)
	r.handlers[leader][pb.HeartbeatResp] = lw(r, r.handleLeaderHeartbeatResp)
	r.handlers[leader][pb.SnapshotStatus] = lw(r, r.handleLeaderSnapshotStatus)
	r.handlers[leader][pb.Unreachable] = lw(r, r.handleLeaderUnreachable)
	r.handlers[leader][pb.LeaderTransfer] = r.handleLeaderTransfer
	r.handlers[leader][pb.Election] = r.handleNodeElection
	r.handlers[leader][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[leader][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[leader][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[leader][pb.LocalTick] = r.handleLocalTick
	r.handlers[leader][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[leader][pb.RateLimit] = r.handleLeaderRateLimit
	r.handlers[leader][pb.LogQuery] = r.handleLogQuery
	// nonVoting
	r.handlers[nonVoting][pb.Heartbeat] = r.handleNonVotingHeartbeat
	r.handlers[nonVoting][pb.Replicate] = r.handleNonVotingReplicate
	r.handlers[nonVoting][pb.InstallSnapshot] = r.handleNonVotingSnapshot
	r.handlers[nonVoting][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[nonVoting][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[nonVoting][pb.Propose] = r.handleNonVotingPropose
	r.handlers[nonVoting][pb.ReadIndex] = r.handleNonVotingReadIndex
	r.handlers[nonVoting][pb.ReadIndexResp] = r.handleNonVotingReadIndexResp
	r.handlers[nonVoting][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[nonVoting][pb.LocalTick] = r.handleLocalTick
	r.handlers[nonVoting][pb.SnapshotReceived] = r.handleRestoreRemote
	r.handlers[nonVoting][pb.LogQuery] = r.handleLogQuery
	// witness
	r.handlers[witness][pb.Heartbeat] = r.handleWitnessHeartbeat
	r.handlers[witness][pb.Replicate] = r.handleWitnessReplicate
	r.handlers[witness][pb.InstallSnapshot] = r.handleWitnessSnapshot
	r.handlers[witness][pb.RequestVote] = r.handleNodeRequestVote
	r.handlers[witness][pb.RequestPreVote] = r.handleNodeRequestPreVote
	r.handlers[witness][pb.ConfigChangeEvent] = r.handleNodeConfigChange
	r.handlers[witness][pb.LocalTick] = r.handleLocalTick
	r.handlers[witness][pb.SnapshotReceived] = r.handleRestoreRemote
}

/*
函数核心价值：checkHandlerMap 是 Raft 节点启动前的「安全检查哨」，通过验证「状态-消息类型」处理器的合法性，确保 initializeHandlerMap 未注册违反协议约束的处理器（如领导者处理跟随者专属的 Heartbeat 消息）。

检查列表设计：checks 切片枚举了所有「无效状态-消息类型组合」，其设计依据 Raft 协议对节点状态的行为约束：

领导者：仅处理提案、日志复制响应等主动行为，不接收其他节点的心跳/复制请求。
跟随者：仅被动接收领导者消息，不处理复制响应/快照状态等领导者专属逻辑。
特殊节点（非投票/见证成员）：因角色限制（无投票权/无完整日志），不参与选举或复杂请求处理。
防御性编程：通过 panic 终止程序而非返回错误，确保无效处理器配置在节点启动阶段被发现，避免运行时因错误消息处理导致的集群分裂或数据不一致。
*/

// checkHandlerMap 验证处理器映射表（handlers）的正确性，确保特定状态-消息类型组合不存在处理器。
// 核心作用：防止无效的状态-消息处理逻辑被注册，避免 Raft 协议状态机因错误处理而出现一致性问题。
// 验证逻辑：遍历预定义的「禁止处理器组合」列表，若发现对应状态-消息类型存在处理器，则触发 panic。
func (r *raft) checkHandlerMap() {
	// checks 定义了不允许存在处理器的状态-消息类型组合，这些组合违反 Raft 协议状态约束：
	//   - 领导者不应处理跟随者专属消息（如 Heartbeat/Replicate）
	//   - 跟随者不应处理领导者专属响应（如 ReplicateResp/HeartbeatResp）
	//   - 非投票成员/见证成员不应处理选举相关消息（如 Election）
	checks := []struct {
		stateType State          // Raft 节点状态（如 leader/follower/candidate）
		msgType   pb.MessageType // 消息类型（如 pb.Heartbeat/pb.Replicate）
	}{
		{leader, pb.Heartbeat},          // 领导者不处理 Heartbeat（跟随者专属）
		{leader, pb.Replicate},          // 领导者不处理 Replicate（跟随者专属）
		{leader, pb.InstallSnapshot},    // 领导者不处理 InstallSnapshot（跟随者专属）
		{leader, pb.ReadIndexResp},      // 领导者不处理 ReadIndexResp（跟随者专属）
		{leader, pb.RequestPreVoteResp}, // 领导者不处理 RequestPreVoteResp（预选举候选者专属）
		{follower, pb.ReplicateResp},    // 跟随者不处理 ReplicateResp（领导者专属）
		{follower, pb.HeartbeatResp},    // 跟随者不处理 HeartbeatResp（领导者专属）
		{follower, pb.SnapshotStatus},   // 跟随者不处理 SnapshotStatus（领导者专属）
		{follower, pb.Unreachable},      // 跟随者不处理 Unreachable（领导者专属）
		{follower, pb.RequestPreVoteResp},// 跟随者不处理 RequestPreVoteResp（预选举候选者专属）
		{candidate, pb.ReplicateResp},   // 候选者不处理 ReplicateResp（领导者专属）
		{candidate, pb.HeartbeatResp},   // 候选者不处理 HeartbeatResp（领导者专属）
		{candidate, pb.SnapshotStatus},  // 候选者不处理 SnapshotStatus（领导者专属）
		{candidate, pb.Unreachable},     // 候选者不处理 Unreachable（领导者专属）
		{candidate, pb.RequestPreVoteResp},// 候选者不处理 RequestPreVoteResp（预选举候选者专属）
		{preVoteCandidate, pb.ReplicateResp}, // 预选举候选者不处理 ReplicateResp（领导者专属）
		{preVoteCandidate, pb.HeartbeatResp}, // 预选举候选者不处理 HeartbeatResp（领导者专属）
		{preVoteCandidate, pb.SnapshotStatus}, // 预选举候选者不处理 SnapshotStatus（领导者专属）
		{preVoteCandidate, pb.Unreachable},    // 预选举候选者不处理 Unreachable（领导者专属）
		{nonVoting, pb.Election},        // 非投票成员不处理 Election（选举相关）
		{nonVoting, pb.RequestVoteResp}, // 非投票成员不处理 RequestVoteResp（无投票权）
		{nonVoting, pb.ReplicateResp},   // 非投票成员不处理 ReplicateResp（领导者专属）
		{nonVoting, pb.HeartbeatResp},   // 非投票成员不处理 HeartbeatResp（领导者专属）
		{nonVoting, pb.RequestPreVoteResp}, // 非投票成员不处理 RequestPreVoteResp（无投票权）
		{witness, pb.Election},          // 见证成员不处理 Election（选举相关）
		{witness, pb.Propose},           // 见证成员不处理 Propose（仅存储元数据）
		{witness, pb.ReadIndex},         // 见证成员不处理 ReadIndex（仅参与法定人数）
		{witness, pb.ReadIndexResp},     // 见证成员不处理 ReadIndexResp（仅参与法定人数）
		{witness, pb.RequestVoteResp},   // 见证成员不处理 RequestVoteResp（仅参与预投票）
		{witness, pb.ReplicateResp},     // 见证成员不处理 ReplicateResp（领导者专属）
		{witness, pb.HeartbeatResp},     // 见证成员不处理 HeartbeatResp（领导者专属）
		{witness, pb.RequestPreVoteResp}, // 见证成员不处理 RequestPreVoteResp（仅参与预投票）
		{witness, pb.LogQuery},          // 见证成员不处理 LogQuery（无完整日志）
	}
	// 遍历检查列表，验证无效组合是否存在处理器
	for _, tt := range checks {
		f := r.handlers[tt.stateType][tt.msgType]
		if f != nil {
			panic("unexpected msg handler") // 发现无效处理器，触发 panic 终止程序（防御性编程）
		}
	}
}

func (r *raft) checkHandlerMap() {
	// following states/types are not supposed to have handler filled in
	checks := []struct {
		stateType State
		msgType   pb.MessageType
	}{
		{leader, pb.Heartbeat},
		{leader, pb.Replicate},
		{leader, pb.InstallSnapshot},
		{leader, pb.ReadIndexResp},
		{leader, pb.RequestPreVoteResp},
		{follower, pb.ReplicateResp},
		{follower, pb.HeartbeatResp},
		{follower, pb.SnapshotStatus},
		{follower, pb.Unreachable},
		{follower, pb.RequestPreVoteResp},
		{candidate, pb.ReplicateResp},
		{candidate, pb.HeartbeatResp},
		{candidate, pb.SnapshotStatus},
		{candidate, pb.Unreachable},
		{candidate, pb.RequestPreVoteResp},
		{preVoteCandidate, pb.ReplicateResp},
		{preVoteCandidate, pb.HeartbeatResp},
		{preVoteCandidate, pb.SnapshotStatus},
		{preVoteCandidate, pb.Unreachable},
		{nonVoting, pb.Election},
		{nonVoting, pb.RequestVoteResp},
		{nonVoting, pb.ReplicateResp},
		{nonVoting, pb.HeartbeatResp},
		{nonVoting, pb.RequestPreVoteResp},
		{witness, pb.Election},
		{witness, pb.Propose},
		{witness, pb.ReadIndex},
		{witness, pb.ReadIndexResp},
		{witness, pb.RequestVoteResp},
		{witness, pb.ReplicateResp},
		{witness, pb.HeartbeatResp},
		{witness, pb.RequestPreVoteResp},
		{witness, pb.LogQuery},
	}
	for _, tt := range checks {
		f := r.handlers[tt.stateType][tt.msgType]
		if f != nil {
			panic("unexpected msg handler")
		}
	}
}
