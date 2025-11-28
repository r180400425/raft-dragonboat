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
	"time"

	"github.com/cockroachdb/errors"  // 增强型错误处理库，提供堆栈跟踪等功能
	"github.com/lni/goutils/logutil" // 日志格式化工具
	"github.com/lni/goutils/random"  // 随机数生成工具

	"github.com/lni/dragonboat/v4/config"            // Raft节点配置定义
	"github.com/lni/dragonboat/v4/internal/server"   // 内部服务器接口
	"github.com/lni/dragonboat/v4/internal/settings" // 内部配置参数
	"github.com/lni/dragonboat/v4/logger"            // 日志工具
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb" // Raft协议相关的protobuf定义
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
	// numMessageTypes uint64 = 29
	numMessageTypes uint64 = 34 // 修改：29 → 33，覆盖所有MessageType枚举值（0-32）（更新为33种，含链式连接相关类型）
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

// 新增在 raft 包内定义 resolver 接口（消费方定义契约）
type ILeaderResolver interface {
	// Resolve 解析目标分片领导者地址（复用原 registry.IResolver 的核心能力）
	Resolve(shardID uint64, leaderID uint64) (addr string, connKey string, err error)
	// SetShardLeader 注册当前分片领导者信息（新增领导者注册能力）
	SetShardLeader(shardID uint64, leaderID uint64)
	// GetShardLeader 查询目标分片的当前领导者ID（新增）
	GetShardLeader(shardID uint64) (leaderID uint64, err error)
}

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

	// tickCount 是累计的逻辑时钟tick数（每个tick对应 NodeHostConfig.RTTMillisecond 毫秒）。
	tickCount uint64

	// electionTick 是距离上一次选举相关事件的tick数（达到 electionTimeout 时触发选举）。
	electionTick uint64

	// heartbeatTick 是距离上一次心跳事件的tick数（领导者用，达到 heartbeatTimeout 时发送心跳）。
	heartbeatTick uint64

	// heartbeatTimeout 是心跳超时阈值（单位：tick数，来自 Config.HeartbeatRTT）。
	heartbeatTimeout uint64

	// electionTimeout 是选举超时阈值（单位：tick数，来自 Config.ElectionRTT）。
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

	// // 新增 resolver 是分片领导者地址解析器，用于跨分片领导者地址查询
	// resolver registry.IResolver 但会导致循环导入，所以采用下面方法：在 raft 包内定义 resolver 接口（消费方定义契约）ILeaderResolver
	// raft 结构体中使用本地接口，而非直接依赖 registry.IResolver
	// resolver 分片领导者地址解析器（使用本地定义的 ILeaderResolver 接口）
	resolver ILeaderResolver

	// 新增：链式连接状态
	chainUpstream struct {
		ShardID  uint64 // 上游 ShardID
		LeaderID uint64 // 上游领导者 ReplicaID
	}
	// 新增：下游链式连接信息
	// 在 raft 结构体的 chainDownstream 中添加 lastPingAck 字段，用于跟踪下游 Pong 消息时间：
	chainDownstream struct {
		ShardID     uint64    // 下游 Raft 组 ID
		LeaderID    uint64    // 下游领导者节点 ID
		lastPingAck time.Time // 新增：记录最后一次收到下游 Pong 的时间
	}
}

// 创建初始化Raft节点实例，是 raft 结构体的构造函数。
// 参数 c：节点配置
//
//	logdb：日志数据库接口，用于持久化存储Raft日志和状态
//
// 返回值： *raft：初始化完成的Raft节点实例
// func newRaft(c config.Config, logdb ILogDB) *raft {
func newRaft(c config.Config, logdb ILogDB, resolver ILeaderResolver) *raft { //修改// newRaft 构造函数新增 resolver 参数，完成依赖注入

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
		electionTimeout:  c.ElectionRTT,            // 选举超时阈值（单位：RTT tick数）
		heartbeatTimeout: c.HeartbeatRTT,           // 心跳超时阈值（单位：RTT tick数）
		checkQuorum:      c.CheckQuorum,            // 是否启用领导者定期检查 quorum（防止孤立领导者）
		preVote:          c.PreVote,                // 是否启用 PreVote 协议（避免网络分区导致的频繁选举）
		readIndex:        newReadIndex(),           // ReadIndex 协议状态管理器（处理线性一致性读）
		rl:               rl,                       // 内存日志速率限制器实例
		resolver:         resolver,                 // 新增：注入 resolver 实现
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
		r.log.firstIndex(),         // 日志第一条条目的索引（日志范围起始）
		r.log.lastIndex(),          // 日志最后一条条目的索引（日志范围结束）
		t,                          // 最后一条日志的任期号
		r.log.committed,            // 已提交的日志索引（多数节点已复制）
		r.log.processed,            // 已应用到状态机的日志索引（已执行）
		dn(r.shardID, r.replicaID), // 节点标识（shardID:replicaID）
		r.term)                     // 当前任期号
}

// 判断节点类型
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

// 判断，用于确保只有领导者节点才能执行某些操作。
func (r *raft) mustBeLeader() {
	if !r.isLeader() {
		plog.Panicf("%s is not leader", r.describe())
	}
}

// setLeaderID : 更新当前节点记录的领导者 ID，并触发领导者变更事件通知（若需要）。
// 该函数是 Raft 协议中领导者身份传播的关键入口，确保节点状态与集群领导者信息同步。
// 参数：
//   - leaderID: 新的领导者节点 ID（NoLeader 表示无领导者）
func (r *raft) setLeaderID(leaderID uint64) {
	// 1. 更新本地领导者 ID 记录
	r.leaderID = leaderID

	// 2. 创建领导者变更信息结构体，包含新领导者 ID 和当前任期
	r.leaderUpdate = &pb.LeaderUpdate{
		LeaderID: leaderID, //指定值
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
	// 领导者转移过程中需主动断开链式连接，避免下游 Shard 继续向旧领导者发送消息：
	if r.leaderTransferTarget != NoNode {
		plog.Infof("%s aborting leader transfer to %d", r.describe(), r.leaderTransferTarget)

		// 1. 发送断开通知给上游分片领导者
		// 新增：发送链式断开消息给下游 Shard
		if r.chainUpstream.LeaderID != 0 {
			disconnectMsg := pb.Message{
				Type:    pb.LeaderChainDisconnect, // 需在 raftpb 中定义
				From:    r.replicaID,
				To:      r.chainUpstream.LeaderID,
				ShardID: r.shardID,
				Term:    r.term,
			}
			r.send(disconnectMsg)
		}

		// 2. 重置上下游链式连接状态（避免残留无效连接信息）
		r.chainUpstream = struct{ ShardID, LeaderID uint64 }{} // 清空上游信息
		r.chainDownstream = struct {
			ShardID, LeaderID uint64
			lastPingAck       time.Time
		}{} // 清空下游信息
	}
	// 3. 重置领导者转移目标（核心状态清理）
	r.leaderTransferTarget = NoNode
}

// 计算具有投票权的成员总数
// Raft 协议中，投票成员包括：
//   - 普通投票节点（Voting Member，存储在 remotes 中）
//   - 见证节点（Witness Member，存储在 witnesses 中，仅参与法定人数计数，不存储完整日志）
//
// 返回值：
//   - int: 投票成员总数（remotes 长度 + witnesses 长度）
func (r *raft) numVotingMembers() int {
	return len(r.remotes) + len(r.witnesses)
}

// 计算达成法定人数（多数派）的数目
func (r *raft) quorum() int {
	return r.numVotingMembers()/2 + 1
}

// 判断是否为单节点器群（法定人数为1）
func (r *raft) isSingleNodeQuorum() bool {
	return r.quorum() == 1
}

// 判断领导者是否拥有法定人数的投票
func (r *raft) leaderHasQuorum() bool {
	c := 0 //计数器

	for nid, member := range r.votingMembers() { //遍历投票成员
		//如果节点是领导者自己，或者节点是活跃的，则计数器加1
		//领导者总认为自己活跃
		if nid == r.replicaID || member.isActive() {
			c++                   //计数
			member.setNotActive() //设置成员非活动，防止重复计数
		}
	}
	return c >= r.quorum()
}

// 所有节点的id列表 未排序
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

// 返回所有具有投票权的成员的复制进度映射。
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

// 获取当前 Raft 节点的核心状态（任期Term，投票对象Vote，已提交索引Commit）
func (r *raft) raftState() pb.State {
	//该状态需持久化存储（通过 logdb），用于节点重启后恢复状态。
	return pb.State{ //结构体
		Term:   r.term,
		Vote:   r.vote,
		Commit: r.log.committed,
	}
}

// 从持久化存储加载 Raft 核心状态并更新（任期Term，投票对象Vote，已提交索引Commit），用于节点重启或状态恢复。
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

restore 函数：恢复本地状态
核心定位：快照恢复的主入口，负责快照合法性验证（索引/任期/节点类型）和日志状态更新。
关键逻辑：分步骤解释“快照跳过条件”（索引过期）、“任期匹配检查”（避免重复恢复）、“日志状态覆盖”（快照内容替换本地日志）。
协议关联：引用 Raft 论文中快照索引与任期匹配的规则，说明为何任期不匹配时必须执行恢复。

*/

// 快照恢复
// 快照恢复是 Raft 协议中日志压缩的关键机制，用于快速同步节点状态（而非逐条复制历史日志）。
//   - ss: 待恢复的快照数据（包含索引、任期、成员配置等元信息）
func (r *raft) restore(ss pb.Snapshot) (bool, error) {

	// 1. 若快照索引 <= 已提交索引，快照已过时，无需恢复（避免覆盖更新的日志）
	if ss.Index <= r.log.committed {
		plog.Warningf("%s, restore aborted, ss.Index <= committed", r.describe())
		return false, nil //这里的false是无需恢复
	}

	// 2. 验证节点类型一致性，节点的类型不能改变（比较当前节点的实际类型和快照中记录的类型，若冲突则终止快照恢复）
	if !r.isNonVoting() {
		for nid := range ss.Membership.NonVotings {
			if nid == r.replicaID {
				plog.Panicf("%s converting to nonVoting, index %d, committed %d, %+v", r.describe(), ss.Index, r.log.committed, ss)
			}
		}
	}

	// 3. 验证节点类型一致性：非见证节点不能通过快照转为见证节点
	if !r.isWitness() {
		for nid := range ss.Membership.Witnesses {
			if nid == r.replicaID {
				plog.Panicf("%s converting to witness, index %d, committed %d, %+v", r.describe(), ss.Index, r.log.committed, ss)
			}
		}
	}

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
	// 调用日志管理器执行快照恢复（覆盖本地日志状态）
	r.log.restore(ss)
	return true, nil // 返回 true 表示快照已成功恢复
}

/**
* restoreRemotes 函数：---集群成员
核心定位：快照恢复的配套函数，确保节点对集群成员的复制进度跟踪与快照状态同步。
成员类型区分：分别说明投票/非投票/见证节点的恢复逻辑，强调不同成员类型的复制进度初始化差异。
状态修正：解释特殊场景处理（如节点自身状态修正、见证节点升级限制），避免恢复后出现状态不一致。
*/

// restoreRemotes 根据快照中的成员配置，恢复【所有节点】的复制进度跟踪状态。
// 快照恢复后，节点需重新初始化对集群成员的复制进度（match/next 索引），确保日志复制从快照后的日志开始。
// 参数：
//   - ss: 已恢复的快照数据（包含最新的成员配置信息）
func (r *raft) restoreRemotes(ss pb.Snapshot) {
	// -------------------------- 恢复投票节点（Voting Members）--------------------------
	r.remotes = make(map[uint64]*remote)      // 重置投票节点复制进度映射
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
	r.nonVotings = make(map[uint64]*remote)    // 重置非投票节点复制进度映射
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
	r.witnesses = make(map[uint64]*remote)    // 重置见证节点复制进度映射
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
func (r *raft) timeForElection() bool {
	// 当节点处于跟随者/候选者状态且未收到领导者心跳时，选举计时器会累积，达到阈值后触发新选举。
	// 返回值：
	//   - bool: 若选举计时器 >= 随机化选举超时阈值，则返回 true（触发选举）；否则返回 false
	return r.electionTick >= r.randomizedElectionTimeout
}

// timeForHeartbeat 判断是否达到心跳发送时间。
func (r *raft) timeForHeartbeat() bool {
	// 领导者通过定期发送心跳维持领导权，心跳超时阈值通常远小于选举超时（如 1/10 选举超时）。
	// 返回值：
	//   - bool: 若心跳计时器 >= 心跳超时阈值，则返回 true（触发心跳发送）；否则返回 false
	return r.heartbeatTick >= r.heartbeatTimeout
}

// p69 of the raft thesis mentions that check quorum is performed when an election timeout elapses

// timeForCheckQuorum 判断是否需要执行领导者定期 quorum 检查。
func (r *raft) timeForCheckQuorum() bool {
	// 参考 Raft 论文 6.3 节，领导者需定期确认多数派节点仍存活，避免"孤立领导者"继续提交日志。
	// 触发时机：选举计时器达到选举超时阈值（与选举触发周期一致）。
	// 返回值：
	//   - bool: 若选举计时器 >= 选举超时阈值，则返回 true（触发 quorum 检查）
	return r.electionTick >= r.electionTimeout
}

// p29 of the raft thesis mentions that leadership transfer should abort when an election timeout elapses

// timeToAbortLeaderTransfer 判断是否需要中止领导者转移。

// 参考 Raft 论文 3.10 节，领导者转移过程中若超过选举超时未完成，需主动中止以避免集群不可用。

func (r *raft) timeToAbortLeaderTransfer() bool {
	//   - bool: 若正在进行领导者转移且选举计时器 >= 选举超时阈值，则返回 true（中止转移）；否则返回 false
	return r.leaderTransfering() && r.electionTick >= r.electionTimeout
}

// timeForRateLimitCheck 判断是否需要执行内存日志速率限制检查。
// 定期检查未应用日志的内存占用，避免节点因日志积压导致内存溢出。
// 触发时机：当前tick数是选举超时阈值的整数倍（周期性检查，频率与选举周期一致）。
// 返回值：
//   - bool: 若tick数 % 选举超时 == 0，则返回 true（触发速率检查）；否则返回 false
func (r *raft) timeForRateLimitCheck() bool {
	return r.tickCount%r.electionTimeout == 0
}

// timeForInMemGC 判断是否需要执行内存日志垃圾回收。
// 清理已持久化到磁盘的日志数据，释放内存空间（由 settings.Soft.InMemGCTimeout 控制周期）。
// 返回值：
//   - bool: 若tick数 % 内存 GC 超时 == 0，则返回 true（触发 GC）；否则返回 false
func (r *raft) timeForInMemGC() bool {
	return r.tickCount%inMemGcTimeout == 0
}

// -------------------------- 核心定时任务调度 --------------------------

// tick 是 Raft 节点的核心定时任务入口，每个逻辑时钟tick（对应配置的 RTT 时间）调用一次。
// 根据节点状态（领导者/非领导者）分发到不同的处理逻辑，并执行周期性维护任务（如内存 GC）。
// 返回值：
//   - error: 任务执行过程中遇到的错误（如日志操作失败）
func (r *raft) tick() error {
	r.quiesce = false // 退出静默模式（若之前处于静默）
	r.tickCount++     // 全局tick计数器自增
	// this is to work around the language limitation described in https://github.com/golang/go/issues/9618
	// 注：此处规避 Go 语言限制（https://github.com/golang/go/issues/9618），通过函数调用隔离闭包逻辑

	// 处理内存日志 GC（周期性触发，避免频繁执行）
	if r.timeForInMemGC() {
		r.log.inmem.tryResize() // 尝试收缩内存日志缓冲区
	}

	// 根据节点状态分发到领导者/非领导者逻辑
	if r.isLeader() {
		return r.leaderTick() // 调用领导者节点处理逻辑
	}
	return r.nonLeaderTick() // 调用跟随者/候选者节点处理逻辑
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

	r.electionTick++ // 选举计时器自增（未收到心跳时累积，触发选举）

	// 周期性执行内存日志速率限制检查（若启用）
	if r.timeForRateLimitCheck() {
		if r.rl.Enabled() {
			r.rl.Tick()              // 更新速率限制器状态
			r.sendRateLimitMessage() // 向领导者发送速率限制状态（如内存占用过高）
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
		r.electionTick = 0 // 重置选举计时器（避免重复触发）
		// 发送本地选举消息，触发选举流程
		if err := r.Handle(pb.Message{
			From: r.replicaID, // 消息发送者为当前节点
			Type: pb.Election, // 消息类型：发起选举
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

*/
// leaderTick 是领导者节点的定时任务处理逻辑，每个 RTT tick调用一次。
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
				From: r.replicaID,    // 消息发送者为当前领导者节点
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
			From: r.replicaID,        // 消息发送者为当前领导者节点
			Type: pb.LeaderHeartbeat, // 消息类型：领导者心跳
		}); err != nil {
			return err // 消息处理失败时返回错误
		}
	}

	// 新增：周期性链式连接健康检查（每 3 个选举超时周期触发一次）
	if r.electionTick%(3*r.electionTimeout) == 0 {
		r.checkChainHealth()
	}

	// 8. 检查待处理的快照确认（确保快照已成功发送给跟随者）
	return r.checkPendingSnapshotAck()
}

// quiescedTick 处理节点在静默模式（quiesce mode）下的定时任务。
// 静默模式是一种优化，当集群无操作时停止发送心跳以节省带宽。
// 触发条件：节点无日志复制、无提案且无领导者转移时自动进入。
func (r *raft) quiescedTick() {
	if !r.quiesce { // 若未启用静默模式
		r.quiesce = true     // 则启动
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

// send and broadcast functions
//
// finalizeMessageTerm 验证并设置消息的任期（Term），确保消息符合 Raft 协议的任期规则。
// 不同类型消息的任期处理逻辑不同（如投票请求需显式设置任期，心跳消息使用当前任期）。
// 参数：
//   - m: 待处理的消息
//
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
	m.From = r.replicaID         // 设置消息发送者为当前节点 ID
	m = r.finalizeMessageTerm(m) // 验证并设置消息任期
	r.msgs = append(r.msgs, m)   // 将消息加入发送队列（由上层模块实际发送）
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
		inmemSz := r.rl.Get()                                             // 当前内存日志总占用
		notCommitedSz := getEntrySliceSize(r.log.getUncommittedEntries()) // 未提交日志占用
		mv = max(inmemSz-notCommitedSz, 0)                                // 已提交未应用日志占用 = 总占用 - 未提交占用
	}

	// 发送速率限制消息给领导者
	r.send(pb.Message{
		Type: pb.RateLimit, // 消息类型：速率限制通知
		To:   r.leaderID,   // 接收者：当前领导者
		Hint: mv,           // 内存使用提示（已提交未应用日志大小）
	})
}

// 创建 InstallSnapshot 消息，用于向目标节点发送快照。
// 快照包含集群状态的完整备份，用于快速同步落后节点（避免逐条复制历史日志）。
// 参数：
//   - to: 目标节点 ID
//   - m: 待填充的消息结构体（输出参数）
//
// 返回值：
//   - uint64: 快照的日志索引
func (r *raft) makeInstallSnapshotMessage(to uint64, m *pb.Message) uint64 {
	m.To = to                    // 设置目标节点
	m.Type = pb.InstallSnapshot  // 消息类型：安装快照
	snapshot := r.log.snapshot() // 获取当前节点的最新快照

	// 防御性检查：快照不能为空
	if pb.IsEmptySnapshot(snapshot) {
		plog.Panicf("%s got an empty snapshot", r.describe())
	}

	// For witness, snapshot message will be marked as dummy snapshot.

	// 见证节点（Witness）仅需元数据快照（不含实际日志数据，节省带宽）
	if _, ok := r.witnesses[to]; ok {
		snapshot = makeWitnessSnapshot(snapshot)
	}

	m.Snapshot = snapshot // 填充快照数据
	return snapshot.Index // 返回快照的日志索引
}

// makeWitnessSnapshot 将完整快照转换为见证节点Witness专用的元数据快照。
// 见证节点不存储完整日志和快照数据，仅需元数据（索引、任期、成员配置）参与法定人数计算。
// 参数：
//   - snapshot: 完整快照
//
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

// 创建 Replicate 消息，包含待复制的日志条目，用于领导者向跟随者同步日志。
// 根据目标节点类型（普通节点/见证节点）调整日志内容（见证节点仅需元数据条目）。
// 参数：
//   - to: 目标节点 ID
//   - next: 目标节点的下一条待复制日志索引（即从该索引开始发送日志）
//   - maxSize: 消息最大允许大小（防止单条消息过大）
//
// 返回值：
//   - pb.Message: 构造的 Replicate 消息
//   - error: 日志获取失败（如日志已压缩）
func (r *raft) makeReplicateMessage(to uint64, next uint64, maxSize uint64) (pb.Message, error) {

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

	// 如果是witness节点，需要特殊处理
	// 见证节点仅需元数据条目（不含命令数据，除非是配置变更条目）

	// Don't send actual log entry to witness as they won't replicate real message,
	// unless there is a config change.
	if _, ok := r.witnesses[to]; ok { //通过查找to节点是否存在于witnessmap中，判断to是否是见证节点
		entries = makeMetadataEntries(entries)
	}
	// 此句语法：
	// 赋值操作：_, ok := r.witnesses[to]
	// 条件判断：ok（布尔值）
	// 只有当 ok 为 true 时，整个条件才为真，执行 if 块中的代码。

	// 构造并返回 Replicate 消息
	return pb.Message{
		To:       to,              // 目标节点
		Type:     pb.Replicate,    // 消息类型：日志复制
		LogIndex: next - 1,        // 基准索引（目标节点已复制到该索引）
		LogTerm:  term,            // 基准索引对应的任期（用于一致性检查）
		Entries:  entries,         // 待复制的日志条目
		Commit:   r.log.committed, // 领导者当前的已提交索引（通知跟随者更新提交进度）
	}, nil
}

// makeMetadataEntries 将普通日志条目转换为元数据条目（仅保留索引、任期和类型，移除命令数据）。
// 用于向见证节点发送日志（见证节点无需执行命令，仅需元数据参与法定人数计算）。
// 参数：
//   - entries: 普通日志条目列表
//
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
	// （Wait 或 Snapshot 状态下暂停主动复制）。
	if rp.isPaused() {
		return
	}

	// 尝试创建 Replicate 消息（包含待复制日志条目）
	m, err := r.makeReplicateMessage(to, rp.next, maxEntrySize)

	if err != nil { // 日志获取失败，此时需要根据快照同步节点状态，恢复节点状态

		if !rp.isActive() { //rp 非活动
			plog.Warningf("%s, %s is not active, sending snapshot is skipped",
				r.describe(), ReplicaID(to))
			return
		}

		// rp active

		// 创建并发送快照消息
		index := r.makeInstallSnapshotMessage(to, &m)
		plog.Infof("%s is sending snapshot (%d) to %s, r.Next %d, r.Match %d, %v", r.describe(), index, ReplicaID(to), rp.next, rp.match, err)
		rp.becomeSnapshot(index) // 标记节点为快照发送中状态

	} else if len(m.Entries) > 0 { //获取到日志

		// 更新节点复制进度（已发送的最后一条日志索引）
		lastIndex := m.Entries[len(m.Entries)-1].Index
		rp.progress(lastIndex)
	}

	// 发送消息（Replicate 或 InstallSnapshot）
	r.send(m)
}

// 向集群所有节点广播 Replicate 消息（领导者定期同步日志）。
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

// 向目标节点发送 Heartbeat 消息（领导者维持领导权的心跳）。
// 心跳消息包含领导者的已提交索引，用于通知跟随者更新提交进度。
func (r *raft) sendHeartbeatMessage(to uint64,
	hint pb.SystemCtx, match uint64) {

	// 参数：
	//   - to: 目标节点 ID
	//   - hint: 系统上下文低 64 位（如 ReadIndex 请求 ID）
	//   - match: 目标节点的已匹配日志索引（用于优化提交索引计算）

	commit := min(match, r.log.committed) // 最小值（确保安全性）

	r.send(pb.Message{
		To:       to,           // 目标节点
		Type:     pb.Heartbeat, // 消息类型：心跳
		Commit:   commit,       // 领导者建议的提交索引
		Hint:     hint.Low,     // 系统上下文低 64 位
		HintHigh: hint.High,    // 系统上下文高 64 位
		// SystemCtx is used to identify a ReadIndex operation.
	})
}

// 向集群所有投票成员广播心跳消息（领导者定期发送，默认每秒几次）。
// 若存在未完成的 ReadIndex 请求，心跳消息会携带 ReadIndex 上下文，用于线性一致性读确认。
// 参考 Raft 论文 6.4 节：ReadIndex 协议通过心跳消息传播已提交索引，确保读操作线性一致性。
// p72 of the raft thesis describe how to use Heartbeat message in the ReadIndex  protocol.
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

// 向集群节点广播携带特定上下文的心跳消息。
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

// 发送 TimeoutNow 消息，触发目标节点立即发起选举（用于领导者转移）。
// Raft 领导者转移协议通过此消息通知目标节点提前超时并竞选领导者。
// 参数：
//   - replicaID: 目标节点 ID（期望成为新领导者的节点）
func (r *raft) sendTimeoutNowMessage(replicaID uint64) {
	r.send(pb.Message{
		Type: pb.TimeoutNow, // 消息类型：立即超时
		To:   replicaID,     // 目标节点
	})
}

//
// log append and commit
//

// sortMatchValues 对投票成员的日志匹配索引（match）进行排序，用于计算多数派提交索引。
func (r *raft) sortMatchValues() {

	// unrolled bubble sort, sort.Slice is not allocation free
	// 采用手动展开的冒泡排序（非标准库 sort，避免sort.Slice 的内存分配），针对小规模数组（投票成员数）高效。

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

	// 收集所有投票成员的匹配索引
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
	// 也就是说，达到q的索引表明已经有多数派确认，此时可以提交，不必等待所有都确认

	// Raft 论文 5.4.2 节：仅当前任期的日志条目可通过计数副本提交，旧任期条目需通过当前任期条目间接提交

	// see p8 raft paper
	// "Raft never commits log entries from previous terms by counting replicas.
	// Only log entries from the leader’s current term are committed by counting replicas"

	return r.log.tryCommit(q, r.term)
}

// appendEntries 将新日志条目追加到本地日志，并触发单节点集群的自动提交。
// 条目追加时会自动填充任期（当前领导者任期）和索引（基于日志最后索引递增）。
func (r *raft) appendEntries(entries []pb.Entry) error {

	lastIndex := r.log.lastIndex() // 获取当前日志最后索引

	// 填充每个条目的任期和索引（确保条目连续性）
	for i := range entries {
		entries[i].Term = r.term                     // 条目任期 = 当前领导者任期
		entries[i].Index = lastIndex + 1 + uint64(i) // 索引 = 最后索引 + 1 + 条目偏移
	}

	r.log.append(entries) // 追加条目到本地日志

	// 更新本地节点的匹配索引（自身日志始终匹配）
	// 当当前节点成功追加新日志条目后（如通过appendEntries方法），通过此代码将自身的已匹配日志索引（match） 更新为最新日志索引。
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

// 将节点转换为跟随者状态，重置任期和选举计时器。
// 内部状态转换函数，被 `becomeFollower` 等公开方法调用。
func (r *raft) toFollowerState(term uint64, leaderID uint64,
	resetElectionTimeout bool) {
	// 参数：
	//   - term: 新任期号
	//   - leaderID: 领导者节点 ID
	//   - resetElectionTimeout: 是否重置选举计时器（避免立即触发新选举）

	if r.isWitness() {
		panic("transitioning to follower from witness state") // 见证节点不能直接转为跟随者
	}

	r.state = follower                  // 更新状态为跟随者
	r.reset(term, resetElectionTimeout) // 重置任期、计时器等状态
	r.setLeaderID(leaderID)             // 设置领导者 ID
	plog.Infof("%s became follower", r.describe())
}

// 将节点转换为非投票成员状态（仅适用于已是非投票节点的场景）。
func (r *raft) becomeNonVoting(term uint64, leaderID uint64) {
	// 非投票节点参与日志复制但不参与选举和投票，通常用于集群扩容时的预热。
	// 参数：
	//   - term: 新任期号
	//   - leaderID: 领导者节点 ID

	if !r.isNonVoting() {
		panic("transitioning to nonVoting state from other states") // 仅非投票节点可调用
	}
	if r.isWitness() {
		panic("transitioning to nonVoting from witness state") // 见证节点不能转为非投票节点
	}
	r.reset(term, true)     // 重置任期和计时器
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
	r.reset(term, true)     // 重置任期和计时器
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
	// 新增：重置链式连接状态（不再是领导者，上游信息失效）
	// 	当节点从领导者退为跟随者时，需重置上游链式信息，避免 stale 数据：
	r.chainUpstream = struct {
		ShardID  uint64
		LeaderID uint64
	}{0, 0}
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
	r.reset(r.term, true)      // 重置状态（任期不变，因 PreVote 使用 term+1）
	r.setLeaderID(NoLeader)    // 清除领导者 ID
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
	r.vote = r.replicaID    // 投票给自己
	plog.Warningf("%s became candidate", r.describe())
}

// becomeLeader 将节点转换为领导者状态，初始化日志复制进度并追加 dummy 条目。
// 领导者需初始化所有节点的 nextIndex（日志最后索引 +1），并通过追加条目确立领导权。
// 返回值：
//   - error: 成为领导者过程中遇到的错误（如日志追加失败）
func (r *raft) becomeLeader() error {

	// 状态转换合法性检查：仅候选者可成为领导者
	// need a state transition machine
	if !r.isLeader() && !r.isCandidate() {
		plog.Panicf("transitioning to leader state from %v", r.state.String())
	}
	r.state = leader                         // 更新状态为领导者
	r.reset(r.term, true)                    // 重置状态（任期不变，计时器重置）
	r.setLeaderID(r.replicaID)               // 设置领导者 ID 为自身
	r.preLeaderPromotionHandleConfigChange() // 处理未提交的配置变更
	plog.Infof("%s became leader", r.describe())

	/*
		关键修改说明
			领导者信息注册：新增代码通过 r.resolver.SetShardLeader(r.shardID, r.replicaID) 将当前分片的领导者信息注册到解析器，使其他分片可通过解析器查询到该领导者地址，实现跨分片链式连接。
			日志记录：添加 plog.Infof 日志输出，便于追踪领导者注册状态。
			兼容性：通过 if r.resolver != nil 条件判断确保在未配置解析器时不触发错误，保持与原有逻辑兼容。
			此修改需配合 registry.Registry 中 SetShardLeader 方法实现（见之前提供的 registry.go 修改），共同完成领导者链式连接功能。
	*/
	// 注册领导者信息到解析器（支持跨分片领导者链式连接）
	if r.resolver != nil {
		r.resolver.SetShardLeader(r.shardID, r.replicaID)
		plog.Infof("%s registered as shard leader in resolver", r.describe())
	}

	// p72 of the raft thesis
	// Raft 论文 6.4 节：领导者需追加一条空日志条目（dummy entry）以提交旧任期日志
	return r.appendEntries([]pb.Entry{{Type: pb.ApplicationEntry, Cmd: nil}})

	/*
		根据 Raft 论文 6.4 节（安全性） 的规定：

		Raft 仅通过计数副本提交当前任期的日志条目，旧任期的日志条目无法直接通过副本计数提交，必须通过当前任期的日志条目间接提交。

		因此，领导者当选后需立即生成一条当前任期的日志条目（即使没有客户端请求），当这条条目被复制到多数派节点并提交后，会间接提交所有之前的旧任期日志条目，确保集群状态一致性。

		该行代码：

		安全性保证：若领导者不追加当前任期的日志条目，直接处理客户端请求，可能导致
		旧任期日志条目长期无法提交（例如无客户端请求时），违反 Raft 安全性要求。

		快速提交旧日志：通过主动生成空条目，确保领导者当选后立即启动日志复制流程，快速提交所有历史日志，避免集群恢复后长时间处于不一致状态。

	*/

}

// reset 重置 Raft 节点的核心状态（任期、计时器、投票、复制进度等）。
// 用于状态转换（如成为候选者/跟随者）或异常恢复时的状态清理。
func (r *raft) reset(term uint64, resetElectionTimeout bool) {

	// 参数：
	//   - term: 新任期号
	//   - resetElectionTimeout: 是否重置选举计时器（触发随机化超时）
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

// 处理节点晋升为leader前的未提交配置变更条目。
// 确保领导者在晋升时最多只有一个未应用的配置变更条目，避免因多配置变更同时提交导致的 quorum 重叠问题。
func (r *raft) preLeaderPromotionHandleConfigChange() {

	// 1. 获取已提交但未应用的配置变更条目数量
	n := r.getPendingConfigChangeCount()

	// 2. 若存在多个未提交配置变更，直接 panic（违反 Raft 安全原则）
	if n > 1 {
		plog.Panicf("%s multiple uncommitted config change entries", r.describe())

		// 3. 若存在一个未提交配置变更，标记为待处理状态
	} else if n == 1 {
		plog.Infof("%s becoming leader with pending ConfigChange", r.describe())
		r.setPendingConfigChange() // 标记存在未提交配置变更
	}
}

// resetRemotes 重置所有投票成员的复制进度跟踪状态（match/next 索引）。
func (r *raft) resetRemotes() {

	// Raft 论文 5.3 节：领导者首次掌权时，将所有 nextIndex 初始化为自身日志最后索引 + 1。
	// see section 5.3 of the raft paper
	// "When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log"

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

// 处理投票响应（预选举/正式选举），统计已收到的赞成票数。
func (r *raft) handleVoteResp(from uint64, rejected bool, preVote bool) int {
	//   - from: 投票节点 ID
	//   - rejected: 是否拒绝投票
	//   - preVote: 是否为预选举阶段

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
	if _, ok := r.votes[from]; !ok { //投票节点from没有在节点r的投票列表中出现
		r.votes[from] = !rejected // true 表示赞成票
	}
	// 统计总赞成票数
	for _, v := range r.votes {
		if v {
			votedFor++
		}
	}
	return votedFor //- int: 当前已收到的赞成票数
}

// 启动预选举流程（PreVote 优化），避免网络分区导致的任期膨胀。
// 预选举要求候选者先获取多数派节点的预投票，证明其日志足够新，才允许发起正式选举。
func (r *raft) preVoteCampaign() error {
	r.becomePreVoteCandidate()                 // 转为预选举候选者状态
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
			Term:     r.term + 1,        // 预选举使用 term+1（不影响当前任期）
			To:       k,                 // 目标投票节点
			Type:     pb.RequestPreVote, // 消息类型：预选举请求
			LogIndex: index,             // 本地日志最后索引
			LogTerm:  lastTerm,          // 本地日志最后任期
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
			Term:     term,           // 当前任期（已自增）
			To:       k,              // 目标投票节点
			Type:     pb.RequestVote, // 消息类型：投票请求
			LogIndex: index,          // 本地日志最后索引
			LogTerm:  lastTerm,       // 本地日志最后任期
			Hint:     hint,           // 领导者转移提示
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
		// already a voting member
		return
	}

	// 从非投票成员晋升（继承复制进度）
	if rp, ok := r.nonVotings[replicaID]; ok {
		// promoting to full member with inherited progress info
		r.deleteNonVoting(replicaID)
		r.remotes[replicaID] = rp
		// local peer promoted, become follower
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
	// 函数来自package builtin
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

/*
helper methods required for the membership change implementation

p33-35 of the raft thesis describes a simple membership change protocol which requires only one node can be added or removed at a time. its safety is guarded by the fact that when there is only one node to be added or removed at a time, the old and new quorum are guaranteed to overlap.
the protocol described in the raft thesis requires the membership change entry to be executed as soon as it is appended. this also introduces an extra troublesome step to roll back to an old membership configuration when necessary.
similar to etcd raft, we treat such membership change entry as regular entries that are only executed after being committed (by the old quorum).
to do that, however, we need to further restrict the leader to only has at most one pending not applied membership change entry in its log. this is to avoid the situation that two pending membership change entries are committed in one go with the same quorum while they actually require different quorums.
consider the following situation - for a 3 nodes shard with existing members X, Y and Z, let's say we first propose a membership change to add a new node A, before A gets committed and applied, say we propose another membership change to add a new node B. When B gets committed, A will be committed as well, both will be using the 3 node membership quorum meaning both entries concerning A and B will become committed when any two of the X, Y, Z shard have them replicated. this thus violates the safety requirement as B will require 3 out of the 4 nodes (X,
Y, Z, A) to have it replicated before it can be committed.
we use the following pendingConfigChange flag to help tracking whether there is already a pending membership change entry in the log waiting to be executed.
*/

// 用于成员变更实现的辅助方法
//
// Raft论文第33-35页描述了一个简单的成员变更协议，该协议要求一次只能添加或删除一个节点。
// 其安全性由以下事实保证：当一次只添加或删除一个节点时，旧的和新的法定人数保证会重叠。
// Raft论文中描述的协议要求成员变更条目一旦追加就必须立即执行。
// 这也引入了一个额外的麻烦步骤，即在必要时回滚到旧的成员配置。
// 与etcd raft类似，我们将这样的成员变更条目视为普通条目，只有在提交后（由旧的法定人数）才会执行。
// 然而，为了做到这一点，我们需要进一步限制领导者在其日志中最多只能有一个待处理的未应用成员变更条目。
// 这是为了避免两个待处理的成员变更条目使用相同的法定人数一次性提交，
// 而实际上它们需要不同的法定人数。
// 考虑以下情况 - 对于一个包含现有成员X、Y和Z的3节点分片，
// 假设我们首先提议一个成员变更来添加新节点A，在A被提交和应用之前，
// 我们又提议另一个成员变更来添加新节点B。
// 当B被提交时，A也会被提交，两者都将使用3节点成员法定人数，
// 这意味着关于A和B的条目将在X、Y、Z分片中的任意两个复制后就变为已提交。
// 这因此违反了安全要求，因为B需要4个节点（X、Y、Z、A）中的3个复制它之后才能被提交。
// 我们使用以下pendingConfigChange标志来帮助跟踪日志中是否已经有一个待处理的成员变更条目等待执行。

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

// 统计已提交但未应用的配置变更条目数量。
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
		count += countConfigChange(ents)  // 统计当前批次中的配置变更条目
		idx = ents[len(ents)-1].Index + 1 // 移动到下一批次
	}
}

//
// message handlers used by follower
//

// handleFollowerPropose 处理跟随者节点收到的 Propose 消息（客户端提案）。
// 跟随者不直接处理提案，而是转发给当前领导者（若存在）。
// 参数：
//   - m: 包含提案条目的消息
//
// 返回值：
//   - error: 处理过程中遇到的错误（如无领导者时记录错误）
func (r *raft) handleFollowerPropose(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped proposal, no leader", r.describe())
		r.reportDroppedProposal(m) // 记录被丢弃的提案（用于监控和重试）
		return nil
	}
	m.To = r.leaderID // 转发目标设为当前领导者

	// the message might be queued by the transport layer, this violates the requirement of the entryQueue.get() func. copy the m.Entries to its own space.
	// 消息可能被传输层排队，这违反了 entryQueue.get() 函数的要求。将 m.Entries 复制到它自己的空间中。

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
//
// 返回值：
//   - error: 日志复制过程中遇到的错误（如日志匹配失败）
func (r *raft) handleFollowerReplicate(m pb.Message) error {
	r.leaderIsAvailable()              // 标记领导者可用，重置选举计时器
	r.setLeaderID(m.From)              // 更新当前领导者 ID（消息发送者）
	return r.handleReplicateMessage(m) // 调用通用复制逻辑处理日志条目
}

// handleFollowerHeartbeat 处理跟随者节点收到的 Heartbeat 消息（领导者心跳）。
// 更新领导者状态并调用通用心跳逻辑处理提交索引。
// 参数：
//   - m: 包含提交索引的心跳消息
//
// 返回值：
//   - error: 心跳处理过程中遇到的错误
func (r *raft) handleFollowerHeartbeat(m pb.Message) error {
	r.leaderIsAvailable()              // 标记领导者可用，重置选举计时器
	r.setLeaderID(m.From)              // 更新当前领导者 ID（消息发送者）
	return r.handleHeartbeatMessage(m) // 调用通用心跳逻辑处理提交索引
}

// handleFollowerReadIndex 处理跟随者节点收到的 ReadIndex 请求（线性一致性读）。
// 跟随者不直接处理读请求，而是转发给当前领导者（若存在）。
// 参数：
//   - m: 包含读请求上下文的消息
//
// 返回值：
//   - error: 处理过程中遇到的错误（如无领导者时记录错误）
func (r *raft) handleFollowerReadIndex(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped ReadIndex, no leader", r.describe())
		r.reportDroppedReadIndex(m) // 记录被丢弃的读请求（用于监控和重试）
		return nil
	}
	m.To = r.leaderID // 转发目标设为当前领导者
	r.send(m)         // 转发读请求给领导者
	return nil
}

// handleFollowerLeaderTransfer 处理跟随者节点收到的 LeaderTransfer 请求（领导者转移）。
// 跟随者不参与转移逻辑，仅转发请求给当前领导者（若存在）。
// 参数：
//   - m: 包含转移目标的请求消息
//
// 返回值：
//   - error: 处理过程中遇到的错误（如无领导者时记录警告）
func (r *raft) handleFollowerLeaderTransfer(m pb.Message) error {
	if r.leaderID == NoLeader {
		plog.Warningf("%s dropped LeaderTransfer, no leader", r.describe())
		return nil
	}
	m.To = r.leaderID // 转发目标设为当前领导者
	r.send(m)         // 转发转移请求给领导者
	return nil
}

// handleFollowerReadIndexResp 处理跟随者节点收到的 ReadIndexResp 消息（读请求响应）。
// 记录读请求结果，用于客户端线性一致性读确认。
// 参数：
//   - m: 包含读索引和上下文的响应消息
//
// 返回值：
//   - error: 处理过程中遇到的错误
func (r *raft) handleFollowerReadIndexResp(m pb.Message) error {
	ctx := pb.SystemCtx{
		Low:  m.Hint,     // 读请求上下文低 64 位
		High: m.HintHigh, // 读请求上下文高 64 位
	}
	r.leaderIsAvailable()             // 标记领导者可用，重置选举计时器
	r.setLeaderID(m.From)             // 更新当前领导者 ID（消息发送者）
	r.addReadyToRead(m.LogIndex, ctx) // 记录已就绪的读索引（供客户端读取）
	return nil
}

// handleFollowerInstallSnapshot 处理跟随者节点收到的 InstallSnapshot 消息（快照安装请求）。
// 更新领导者状态并调用通用快照逻辑安装快照（用于快速同步落后节点）。
// 参数：
//   - m: 包含快照数据的消息
//
// 返回值：
//   - error: 快照安装过程中遇到的错误（如快照验证失败）
func (r *raft) handleFollowerInstallSnapshot(m pb.Message) error {
	r.leaderIsAvailable()                    // 标记领导者可用，重置选举计时器
	r.setLeaderID(m.From)                    // 更新当前领导者 ID（消息发送者）
	return r.handleInstallSnapshotMessage(m) // 调用通用快照逻辑安装快照
}

// handleFollowerTimeoutNow 处理跟随者节点收到的 TimeoutNow 消息（立即超时）。
// 触发节点立即发起选举（用于领导者转移协议，参考 Raft 论文 3.10 节）。
// 参数：
//   - m: 触发超时的消息
//
// 返回值：
//   - error: 选举触发过程中遇到的错误
func (r *raft) handleFollowerTimeoutNow(m pb.Message) error {

	// the last paragraph, p29 of the raft thesis mentions that this is nothing different from the clock moving forward quickly
	// 最后一段，Raft 论文第 29 页提到，这与时钟快速向前移动没有什么不同
	// 这段注释是在解释某种 Raft 协议行为或机制，指出在论文的第 29 页最后一段中提到，所讨论的情况或处理方式本质上等同于"时钟快速向前移动"的概念。这通常指的是在分布式系统中处理时间推进或超时机制的一种比喻性描述。

	// Raft 论文 3.10 节：TimeoutNow 消息使目标节点立即超时并发起选举，加速领导者转移
	plog.Debugf("%s TimeoutNow received", r.describe())
	r.electionTick = r.randomizedElectionTimeout // 强制选举计时器达到超时阈值
	r.isLeaderTransferTarget = true              // 标记为领导者转移目标
	if err := r.tick(); err != nil {             // 触发定时任务，进而触发选举
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
//
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
//
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

// when any of the following three methods
// handleCandidateReplicate
// handleCandidateInstallSnapshot
// handleCandidateHeartbeat
// is called, it implies that m.Term == r.term and there is a leader
// for that term. see 4th paragraph section 5.2 of the raft paper

// handleCandidateReplicate 处理候选者节点收到的 Replicate 消息（日志复制请求）。
// 若消息任期与当前任期一致，证明存在合法领导者，候选者退化为跟随者。
// 参数： - m: 包含日志条目的复制消息
// 返回值：   - error: 状态转换或日志处理错误
func (r *raft) handleCandidateReplicate(m pb.Message) error {
	// Raft 论文 5.2 节：若候选者收到领导者的复制请求（任期相同），则退化为跟随者
	r.becomeFollower(r.term, m.From)
	return r.handleReplicateMessage(m) // 处理日志复制
}

// handleCandidateInstallSnapshot 处理候选者节点收到的 InstallSnapshot 消息（快照请求）。
// 若消息任期与当前任期一致，证明存在合法领导者，候选者退化为跟随者。
// 参数：   - m: 包含快照数据的消息
// 返回值：  - error: 状态转换或快照处理错误
func (r *raft) handleCandidateInstallSnapshot(m pb.Message) error {
	// Raft 论文 5.2 节：若候选者收到领导者的快照请求（任期相同），则退化为跟随者
	r.becomeFollower(r.term, m.From)
	return r.handleInstallSnapshotMessage(m) // 处理快照安装
}

// handleCandidateHeartbeat 处理候选者节点收到的 Heartbeat 消息（领导者心跳）。
// 若消息任期与当前任期一致，证明存在合法领导者，候选者退化为跟随者。
// 参数：   - m: 包含提交索引的心跳消息
// 返回值：   - error: 状态转换或心跳处理错误
func (r *raft) handleCandidateHeartbeat(m pb.Message) error {
	// Raft 论文 5.2 节：若候选者收到领导者的心跳（任期相同），则退化为跟随者
	r.becomeFollower(r.term, m.From)
	return r.handleHeartbeatMessage(m) // 处理心跳（更新提交索引）
}

// handleCandidateRequestVoteResp 处理候选者节点收到的 RequestVoteResp 消息（投票响应）。
// 统计赞成票数，若达到法定人数则晋升为领导者；若反对票达法定人数则退化为跟随者。
// 参数：
//   - m: 包含投票结果的响应消息
//
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

	// 3rd paragraph section 5.2 of the raft paper

	// Raft 论文 5.2 节：若获得多数派赞成票，晋升为领导者
	if count == r.quorum() {
		if err := r.becomeLeader(); err != nil {
			return err
		}
		// get the NoOP entry committed ASAP
		// 立即广播复制消息，提交 dummy 条目以确立领导权
		r.broadcastReplicateMessage()
	} else if len(r.votes)-count == r.quorum() { // 反对票达法定人数，退化为跟随者
		// etcd raft does this, it is not stated in the raft paper
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
//
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
		// etcd raft does this, it is not stated in the raft paper
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

//
// handler for various message types
//

// handleHeartbeatMessage 处理收到的心跳消息（由领导者发送），更新本地提交索引并返回心跳响应。
// 心跳消息是领导者维持领导权的核心机制，通过定期发送包含最新提交索引的心跳，确保跟随者不会因选举超时而发起新选举。
// 参数：
//   - m: 接收到的心跳消息，包含领导者的已提交索引（Commit）和可能的系统上下文（Hint/HintHigh，如ReadIndex请求ID）
//
// 返回值：
//   - error: 处理过程中遇到的错误（始终返回nil，心跳处理无失败场景）
func (r *raft) handleHeartbeatMessage(m pb.Message) error {
	// 更新本地日志的已提交索引为领导者告知的提交索引，确保数据一致性
	r.log.commitTo(m.Commit)
	// 向领导者发送心跳响应，包含原消息的系统上下文（用于ReadIndex等协议确认）
	r.send(pb.Message{
		To:       m.From,           // 响应目标为发送心跳的领导者
		Type:     pb.HeartbeatResp, // 消息类型：心跳响应
		Hint:     m.Hint,           // 携带原消息的低64位系统上下文（如ReadIndex请求ID）
		HintHigh: m.HintHigh,       // 携带原消息的高64位系统上下文
	})
	return nil // 心跳消息处理成功
}

// handleInstallSnapshotMessage 处理收到的快照安装请求消息，用于快速同步落后节点的状态。
// 当跟随者日志落后领导者过多时，领导者会发送快照而非逐条日志，以提高同步效率。
// 参数：
//   - m: 包含快照数据的安装请求消息
//
// 返回值：
//   - error: 快照恢复过程中遇到的错误（如快照验证失败、日志操作错误等）
func (r *raft) handleInstallSnapshotMessage(m pb.Message) error {
	plog.Debugf("%s called handleInstallSnapshotMessage with snapshot from %s",
		r.describe(), ReplicaID(m.From))
	index, term := m.Snapshot.Index, m.Snapshot.Term
	resp := pb.Message{
		To:   m.From,
		Type: pb.ReplicateResp,
	}
	// 调用restore方法执行快照恢复，返回是否成功恢复
	ok, err := r.restore(m.Snapshot)
	if err != nil {
		return err
	}
	if ok {
		plog.Debugf("%s restored snapshot %d term %d", r.describe(), index, term)
		resp.LogIndex = r.log.lastIndex() // 恢复成功，响应中携带当前日志最后索引
	} else {
		plog.Debugf("%s rejected snapshot %d term %d", r.describe(), index, term)
		resp.LogIndex = r.log.committed // 恢复失败，响应中携带已提交索引
		if r.events != nil {
			// 触发快照拒绝事件，用于监控和调试
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
	r.send(resp) // 向领导者发送快照处理响应
	return nil
}

// handleReplicateMessage 处理收到的日志复制请求消息，验证日志一致性并追加日志条目。
// 领导者通过Replicate消息向跟随者复制日志，跟随者需验证日志匹配性后决定是否接受。
func (r *raft) handleReplicateMessage(m pb.Message) error {
	// 参数：
	//   - m: 包含待复制日志条目的请求消息
	// 返回值：
	//   - error: 日志复制过程中遇到的错误（如日志匹配失败、条目追加错误等）
	resp := pb.Message{
		To:   m.From,
		Type: pb.ReplicateResp,
	}
	// 若基准索引已落后于已提交索引，直接响应已提交索引（无需处理）
	if m.LogIndex < r.log.committed {
		resp.LogIndex = r.log.committed
		r.send(resp)
		return nil
	}
	// 验证基准索引处的日志任期是否匹配（Raft一致性检查）
	// 即：验证领导者发送的日志条目中，基准索引 m.LogIndex 处的任期是否与跟随者本地日志在同一索引处的任期一致。
	// m.LogIndex 是领导者开始复制的基准索引，m.LogTerm 是该索引处的任期。
	ok, err := r.log.matchTerm(m.LogIndex, m.LogTerm)
	if err != nil {
		return err
	}
	if ok {
		// 任期匹配，尝试追加日志条目
		if _, err := r.log.tryAppend(m.LogIndex, m.Entries); err != nil {
			return err
		}
		// 计算追加后的最后索引，并更新提交索引（取追加后索引与领导者提交索引的最小值）
		lastIdx := m.LogIndex + uint64(len(m.Entries))
		r.log.commitTo(min(lastIdx, m.Commit))
		resp.LogIndex = lastIdx // 响应中携带成功追加后的最后索引

	} else { //任期不匹配
		// ok=false：表示跟随者在 m.LogIndex 处的日志任期与领导者不一致，即出现任期不匹配。

		plog.Debugf("%s rejected Replicate index %d term %d from %s",
			r.describe(), m.LogIndex, m.Term, ReplicaID(m.From))
		resp.Reject = true            // 标记拒绝复制
		resp.LogIndex = m.LogIndex    // 携带被拒绝的基准索引（告知领导者冲突发生的索引位置（即领导者发送的基准索引 m.LogIndex），领导者需从此索引开始回溯日志。）
		resp.Hint = r.log.lastIndex() // 携带本地最后日志索引，帮助领导者调整复制进度（告知领导者跟随者本地日志的最后索引，领导者可根据此值调整 nextIndex（下一次复制的起始索引），避免无效重试。）
		if r.events != nil {
			// 触发复制拒绝事件，用于监控和调试
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
	r.send(resp) // 向领导者发送复制结果响应
	return nil
}

//
// Step related functions
// 用于消息类型判断的辅助函数，帮助路由和处理不同类型的Raft协议消息

// 判断消息是否为预选举相关消息（请求预投票或预投票响应）
// PreVote是Raft的优化机制，用于在发起正式选举前确认候选者日志是否足够新，避免网络分区导致的任期膨胀。
func isPreVoteMessage(t pb.MessageType) bool {
	return t == pb.RequestPreVote || t == pb.RequestPreVoteResp
}

// isRequestVoteMessage 判断消息是否为投票请求消息（正式选举请求或预选举请求）。
// 投票请求是候选者获取集群多数派支持以成为领导者的核心消息类型。
func isRequestVoteMessage(t pb.MessageType) bool {
	return t == pb.RequestVote || t == pb.RequestPreVote
}

// isRequestMessage 判断消息是否为客户端请求类消息（提案、读索引或领导者转移）。
// 这些消息通常由客户端发起，需要领导者处理或由跟随者转发给领导者。
func isRequestMessage(t pb.MessageType) bool {
	return t == pb.Propose || t == pb.ReadIndex || t == pb.LeaderTransfer
}

// isLeaderMessage 判断消息是否为领导者发送的消息（日志复制、快照安装、心跳等）。
// 领导者通过这些消息维护集群状态一致性、同步日志并维持领导权。
func isLeaderMessage(t pb.MessageType) bool {
	return t == pb.Replicate || t == pb.InstallSnapshot ||
		t == pb.Heartbeat || t == pb.TimeoutNow || t == pb.ReadIndexResp
}

// 判断是否应丢弃来自高任期节点的投票请求（RequestVote）
// 用于减少网络分区节点因高任期发起不必要的选举，保护当前领导的稳定性（参考 Raft 论文 6 节最后一段）
func (r *raft) dropRequestVoteFromHighTermNode(m pb.Message) bool {
	//参数：m:待检查的投票请求信息
	// 返回：bool: true 表示丢弃该投票请求，false 表示允许处理

	// 1.非投票请求消息、未启用quorum检查、 消息任期不高于本地任期 ：不丢弃
	if !isRequestVoteMessage(m.Type) || !r.checkQuorum || m.Term <= r.term {
		return false
	}
	// see p42 of the raft thesis
	// 领导者转移的投票请求（m.Hint == m.From ）： 不丢弃
	// 为什么是m.Hint == m.From ：
	// 领导者转移是一种主动、受控的领导权交接过程（非故障触发的被动选举）。例如，当前领导者因负载过高或维护需求，需将领导权交给集群中其他节点。此时：
	// 转移发起者（原领导者）会向目标节点发送 RequestVote 请求，且必须确保该请求不被目标节点的安全过滤逻辑丢弃。
	// 	通过设置 Hint = From（即 Hint 字段等于原领导者的节点 ID），目标节点可识别这是领导者主动发起的转移请求，而非网络分区节点的干扰请求。
	if m.Hint == m.From {
		plog.Debugf("%s, RequestVote with leader transfer hint received from %s",
			r.describe(), ReplicaID(m.From))
		return false
	}

	// 领导者节点的选举计时器不应该超时，若超时则触发panic
	if r.isLeader() && !r.quiesce && r.electionTick >= r.electionTimeout {
		panic("r.electionTick >= r.electionTimeout on leader")
	}
	// we got a RequestVote with higher term, but we recently had heartbeat msg from leader within the minimum election timeout and that leader is known to have quorum. we thus drop such RequestVote to minimize interruption by network partitioned nodes with higher term.
	// this idea is from the last paragraph of the section 6 of the raft paper
	// 我们收到了一个任期更高的 RequestVote 请求，但我们最近在最小选举超时时间内收到了来自领导者的 heartbeat 消息，并且该领导者已知拥有法定人数。因此我们丢弃这样的 RequestVote，以最小化网络分区节点带来的干扰。
	// 这个想法来自 Raft 论文第 6 节最后一段

	// 若近期（选举超时内）收到过领导者心跳，且领导者已知存在 ： 丢弃
	// 避免网络分区节点携带高任期干扰正常集群   “任期膨胀”  （参考 Raft 论文 6 节安全优化）。
	if r.leaderID != NoLeader && r.electionTick < r.electionTimeout {
		return true
	}
	return false
}

// 判断消息是否为预期的高任期预选举相关消息
// 用于在任期不匹配处理中 识别合法的预选举消息，避免错误地更新本地任期
func isPreVoteMessageWithExpectedHigherTerm(m pb.Message) bool {
	//   - bool: true 表示是预选举请求，或未被拒绝的预选举响应（预期的高任期消息）
	return m.Type == pb.RequestPreVote ||
		(m.Type == pb.RequestPreVoteResp && !m.Reject)
}

// onMessageTermNotMatched handles the situation in which the incoming
// message has a term value different from local node's term. it returns a
// boolean flag indicating whether the message should be ignored.
// see the 3rd paragraph, section 5.1 of the raft paper for details.
// 处理入站消息任期与本地节点任期不匹配的情况。
// 根据 Raft 协议 5.1 节第三段，节点需根据消息任期更新自身状态（如转为跟随者），并决定是否忽略消息。包括：
// - 忽略任期为0或相等的消息；
// - 拒绝来自高任期节点的 RequestVote 消息（在特定条件下）；
// - 当消息任期更高时，将节点状态转换为 Follower 或其他角色；
// - 当消息任期更低时，忽略或发送 NoOP 回复。
// 返回值:
//   - bool: 如果消息被忽略或丢弃，返回 true；否则返回 false。
func (r *raft) onMessageTermNotMatched(m pb.Message) bool {

	// if 消息任期为 0（无效）或与本地任期一致，无需处理
	if m.Term == 0 || m.Term == r.term {
		return false
	}

	// if消息为投票请求且来自高任期节点（如果配置允许）
	if r.dropRequestVoteFromHighTermNode(m) {
		plog.Warningf("%s dropped RequestVote at term %d from %s, leader available",
			r.describe(), m.Term, ReplicaID(m.From))
		return true //丢弃
	}

	// if消息任期高于本地任期：更新本地任期并转为对应角色
	if m.Term > r.term {

		// 忽略预选举相关的高任期消息（预选举不更新任期）

		// 如果不是预期的预投票消息，则进行角色转换
		if !isPreVoteMessageWithExpectedHigherTerm(m) {
			plog.Warningf("%s received %s with higher term (%d) from %s",
				r.describe(), m.Type, m.Term, ReplicaID(m.From))
			leaderID := NoLeader
			// 若为领导者消息，记录发送者为新领导者
			if isLeaderMessage(m.Type) {
				leaderID = m.From
			}

			// 根据当前节点类型更新状态（非投票节点/见证节点/普通节点）
			if r.isNonVoting() {
				r.becomeNonVoting(m.Term, leaderID)
			} else if r.isWitness() {
				r.becomeWitness(m.Term, leaderID)
			} else {
				// 对投票请求特殊处理，避免重置选举计时器导致无法发起选举
				if m.Type == pb.RequestVote {
					plog.Warningf("%s become followerKE after receiving higher term from %s", r.describe(), ReplicaID(m.From))
					// not to reset the electionTick value to avoid the risk of having the local node not being to campaign at all. if the local node generates the tick much slower than other nodes (e.g. bad config, hardware clock issue, bad scheduling, overloaded etc.), it may lose the chance to ever start a campaign unless we keep its electionTick value here.

					// 不重置 electionTick 以避免本地节点无法发起选举的风险
					r.becomeFollowerKE(m.Term, leaderID) // 保留选举计时器的跟随者状态
				} else {
					plog.Warningf("%s become follower after receiving higher term from %s", r.describe(), ReplicaID(m.From))
					r.becomeFollower(m.Term, leaderID) // 普通跟随者状态
				}
			}
		}
		// 消息任期低于本地任期：忽略大部分消息，仅响应预选举或领导者消息（维持集群稳定性）
	} else if m.Term < r.term {

		if m.Type == pb.RequestPreVote ||
			(isLeaderMessage(m.Type) && (r.checkQuorum || r.preVote)) {
			// see test TestFreeStuckCandidateWithCheckQuorum for details

			// 响应预选举或领导者消息，避免候选者因网络分区卡死（参考 TestFreeStuckCandidateWithCheckQuorum）

			// 对于预投票或特定条件下的领导消息，发送 NoOP 回复
			r.send(pb.Message{To: m.From, Type: pb.NoOP})

		} else {
			// 其他低任期消息直接忽略
			plog.Infof("%s ignored %s with lower term (%d) from %s",
				r.describe(), m.Type, m.Term, ReplicaID(m.From))
		}
		return true // 低任期消息无需进一步处理
	}
	return false
}

// 检查当前Raft配置是否与消息类型冲突
func (r *raft) inconsistentRaftConfig(m pb.Message) bool {
	// 预选举false即节点未启用预选举功能 且 消息m是预选举消息 则返回true 表示配置不一致
	// 否则 返回false
	return !r.preVote && isPreVoteMessage(m.Type)
}

// Handle 是 Raft 节点的消息处理入口，负责消息的合法性校验、任期匹配处理及路由至对应状态的处理器。
// 参数：
//   - m: 待处理的网络消息
//
// 返回值：
//   - error: 处理过程中遇到的错误（如配置冲突导致的 panic）
func (r *raft) Handle(m pb.Message) error {

	// 配置冲突检查：禁用预选举时收到预选举消息，触发 panic
	if r.inconsistentRaftConfig(m) {
		panic("received preVote message when preVote is not enabled")
	}

	// 处理任期不匹配：若未忽略消息且非预选举消息，二次检查任期一致性
	if !r.onMessageTermNotMatched(m) {
		if !isPreVoteMessage(m.Type) {
			r.doubleCheckTermMatched(m.Term) // 防御性任期校验，避免状态不一致
		}
		return r.handle(r, m) // 路由至状态专用处理器
	}
	plog.Infof("%s dropped %s from %s, term %d, term not matched",
		r.describe(), m.Type, ReplicaID(m.From), m.Term)
	return nil
}

// hasConfigChangeToApply 检查是否存在已提交但未应用的配置变更条目。
// Raft 安全要求：同一时间仅允许一个未应用的配置变更，避免新旧 quorum 无重叠导致的安全性问题。
// 返回值：
//   - bool: true 表示存在未应用的配置变更，false 表示所有已提交配置变更均已应用
func (r *raft) hasConfigChangeToApply() bool {
	// this is a hack to make it easier to port etcd raft tests
	// check those *_etcd_test.go for details

	// 适配 etcd raft 测试的兼容逻辑（实际应扫描已提交未应用的日志条目）
	if r.hasNotAppliedConfigChange != nil {
		return r.hasNotAppliedConfigChange()
	}
	// TODO:
	// with the current entry log implementation, the simplification below is no longer required, we can now actually scan the committed but not applied portion of the log as they are now all in memory.
	// 在当前的日志条目实现中，以下简化不再需要，我们现在实际上可以扫描已提交但未应用的日志部分，因为它们现在都在内存中。

	// 简化判断：若已提交索引大于已应用索引，则认为存在未应用配置变更
	return r.log.committed > r.getApplied()
}

// canGrantVote 判断是否可以授予投票给请求者。
//   - m: 投票请求消息
func (r *raft) canGrantVote(m pb.Message) bool {
	// 根据 Raft 协议 5.2 节第三段：仅当未投票、已投票给请求者或请求者任期更高时，才可能授予投票。
	//   - bool: true 表示可授予投票，false 表示不可授予
	return r.vote == NoNode || r.vote == m.From || m.Term > r.term
}

//
// handlers for nodes in any state
//

// handleNodeElection 函数，属于 Raft 节点在任意状态下处理选举触发消息的核心逻辑，主要用于决定节点是否发起选举（预选举或正式选举），并确保选举过程符合 Raft 协议的安全性要求。
func (r *raft) handleNodeElection(m pb.Message) error {
	// 若当前节点已是领导者，忽略选举请求（领导者无需参与选举）
	// 非领导者 进入下面步骤
	if !r.isLeader() {
		// there can be multiple pending membership change entries committed but not applied on this node. say with a shard of X, Y and Z, there are two such entries for adding node A and B are committed but not applied available on X. If X is allowed to start a new election, it can become the leader with a vote from any one of the node Y or Z. Further proposals made by the new leader X in the next term will require a quorum of 2 which can have no overlap with the committed quorum of 3. this violates the safety requirement of raft.
		// ignore the Election message when there is membership configure change committed but not applied
		// 当前节点已提交但尚未应用的配置变更条目可能有多个。例如，对于一个由 X、Y 和 Z 组成的分片，有两个提交但尚未在 X 节点上应用的添加节点 A 和 B 的条目。如果允许 X 发起新的选举，它可能通过获得 Y 或 Z 中任一节点的投票而成为领导者。新领导者 X 在下一个任期中所做的进一步提案将需要 2 个节点的法定人数，而这可能与原有的 3 个节点的已提交法定人数没有重叠。这违反了 Raft 的安全性要求。
		// 当存在已提交但尚未应用的成员配置变更时，忽略选举消息

		// 1. 检查是否有未应用的配置变更，若有则跳过选举
		// 安全背景：Raft 成员变更需保证“新旧配置的 quorum 重叠”（一次仅允许一个未应用变更）。若存在未应用的配置变更时发起选举，新领导者可能使用错误的 quorum 规则，导致数据不一致（例如旧配置 3 节点 quorum=2，新配置 4 节点 quorum=3，若两者未重叠可能导致提交不安全）。
		if r.hasConfigChangeToApply() {
			plog.Warningf("%s campaign skipped, pending config change",
				r.describe())
			if r.events != nil {
				info := server.CampaignInfo{
					ShardID:   r.shardID,
					ReplicaID: r.replicaID,
					Term:      r.term,
				}
				r.events.CampaignSkipped(info) // 触发“选举跳过”事件（供监控）
			}
			return nil
		}
		// prevote is enabled, but the user explicitly requested the leadership to be transferred, so skip the pre-vote stage
		// 预选举已启用，但用户明确请求进行领导权转移，因此跳过预选举阶段

		// 2. 若启用预选举且非领导者转移目标，发起预选举
		// r.preVote：节点启用了预选举机制（Raft 优化，避免网络分区节点通过“任期膨胀”夺取领导权）。
		// !r.isLeaderTransferTarget：当前节点不是领导者转移的目标节点（领导者转移是主动交接领导权的过程，无需预选举）。
		if r.preVote && !r.isLeaderTransferTarget {
			plog.Debugf("%s will start a preVote campaign", r.describe())
			// 行为：调用 r.preVoteCampaign() 发起预选举，先向集群确认自身日志是否足够新，再决定是否发起正式选举。
			return r.preVoteCampaign() // 预选举：先确认日志是否足够新，避免任期膨胀
		}
		// 3. 否则发起正式选举
		// 不满足预选举条件时（如禁用预选举，或作为领导者转移目标），直接调用 r.campaign() 发起正式选举，向集群节点发送投票请求（RequestVote）。
		plog.Debugf("%s will start a campaign", r.describe())
		return r.campaign() // 正式选举：直接向集群请求投票
	}
	// 领导者忽略选举请求
	// 原因：Raft 协议中，领导者通过定期发送心跳维持领导权，无需参与选举，避免集群内出现多领导者竞争。
	plog.Debugf("%s is leader, ignored Election", r.describe())
	return nil
}

// 处理预投票请求
// 作用：响应其他节点的预选举请求（RequestPreVote），决定是否授予预投票。
// 预选举（PreVote）是 Raft 的优化机制，用于避免网络分区节点通过“任期膨胀”夺取领导权，仅当候选者日志足够新时才允许其发起正式选举。
func (r *raft) handleNodeRequestPreVote(m pb.Message) error {

	resp := pb.Message{ //构建预投票响应
		To:   m.From,                //响应目标为请求者
		Type: pb.RequestPreVoteResp, //消息类型：预投票响应
	}

	// 检查候选者日志是否“足够新”（Raft 论文 5.4 节：日志较新的节点更适合成为领导者）
	// 需满足候选者日志的最后索引和任期不落后于本地日志
	isUpToDate, err := r.log.upToDate(m.LogIndex, m.LogTerm)
	if err != nil {
		return err
	}

	// 预投票拒绝条件：候选者任期 < 本地任期（理论上不应发生，直接 panic）（预投票请求的任期应不低于接收者任期）。

	if m.Term < r.term {
		panic("m.term < r.term")
	}

	// 授予预投票的条件：候选者任期 > 本地任期，且日志足够新

	if m.Term > r.term && isUpToDate {
		resp.Term = m.Term // 响应携带候选者的高任期
		plog.Warningf("%s cast preVote from %s index %d term %d, log term: %d",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term, m.LogTerm)
	} else {
		// 拒绝预投票：候选者任期 <= 本地任期，或日志不新

		// m.Term == r.term || !isUpToDate
		plog.Warningf("%s rejected preVote %s index %d term %d,logterm %d, utd %t",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term, m.LogTerm, isUpToDate)
		resp.Term = r.term // 响应携带本地任期
		resp.Reject = true // 标记拒绝
	}
	r.send(resp) // 发送预投票响应

	// 预投票不改变本地状态：与正式投票不同，预投票仅确认候选者资格，不记录投票状态（r.vote 无更新），避免干扰后续正式选举。

	return nil
}

// 处理正式投票请求
// 作用：响应其他节点的正式选举请求（RequestVote），决定是否授予投票。这是 Raft 协议中候选者获取领导权的核心步骤，需严格遵循“一人一票”和“日志较新”原则。
// 关键细节：
// 投票条件：需同时满足 canGrant（投票权检查）和 isUpToDate（日志新鲜度检查）：
// canGrant：通过 r.canGrantVote(m) 实现，逻辑为 r.vote == NoNode || r.vote == m.From || m.Term > r.term（未投票、投给同一人，或请求者任期更高）。
// isUpToDate：同预投票，确保请求者日志不落后于本地。
// 状态更新：授予投票后，重置 electionTick（避免自身因选举超时而发起新选举），并记录 r.vote = m.From（确保任期内仅投一次票）。
func (r *raft) handleNodeRequestVote(m pb.Message) error {
	resp := pb.Message{ // 构建投票响应
		To:   m.From,             // 响应目标为请求者
		Type: pb.RequestVoteResp, // 消息类型：投票响应
	}

	// 3rd paragraph section 5.2 of the raft paper
	// 检查是否“可授予投票”（Raft 论文 5.2 节）：未投票、已投给请求者，或请求者任期更高
	canGrant := r.canGrantVote(m)

	// 2nd paragraph section 5.4 of the raft paper
	// 检查请求者日志是否“足够新”（Raft 论文 5.4 节）
	isUpToDate, err := r.log.upToDate(m.LogIndex, m.LogTerm)
	if err != nil {
		return err
	}

	// 授予投票：满足可投票条件且日志足够新
	if canGrant && isUpToDate {
		plog.Warningf("%s cast vote from %s index %d term %d, log term: %d",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term, m.LogTerm)
		r.electionTick = 0 // 重置选举计时器（避免立即发起新选举）
		r.vote = m.From    // 记录投票给请求者(m.From)（“一人一票”原则）
	} else { // 拒绝投票
		plog.Warningf("%s rejected vote %s index%d term%d,logterm%d,grant%v,utd%v",
			r.describe(), ReplicaID(m.From), m.LogIndex, m.Term,
			m.LogTerm, canGrant, isUpToDate)
		resp.Reject = true // 标记拒绝
	}
	r.send(resp) // 发送投票响应
	return nil
}

// 处理配置变更事件

// 作用：应用已提交的集群配置变更（如添加/移除节点、变更节点角色），是 Raft 成员变更协议的核心实现，确保配置更新过程中集群仍能维持可用性和一致性。
// 配置变更类型：通过 m.HintHigh 传递，对应 pb.ConfigChangeType 枚举（如 AddNode/RemoveNode），目标节点 ID 由 m.Hint 传递。
// 安全性保障：Raft 要求配置变更需“逐条提交”（一次仅允许一个未应用变更），避免新旧配置 quorum 无重叠导致的数据不一致（通过 hasConfigChangeToApply 检查）。
// 角色区分：支持多种节点角色（投票成员/非投票成员/见证成员），适配不同场景（如新节点预热、轻量级 quorum 节点）。
func (r *raft) handleNodeConfigChange(m pb.Message) error {
	if m.Reject { // 配置变更被拒绝：清除待处理变更
		r.clearPendingConfigChange()
	} else { // 配置变更通过：解析类型并应用
		cctype := (pb.ConfigChangeType)(m.HintHigh) // 从 HintHigh 获取变更类型（如 AddNode）
		nodeid := m.Hint                            // 从 Hint 获取目标节点 ID

		switch cctype {
		case pb.AddNode: // 添加投票成员
			r.addNode(nodeid)
		case pb.RemoveNode: // 移除投票成员
			if err := r.removeNode(nodeid); err != nil {
				return err
			}
		case pb.AddNonVoting: // 添加非投票成员（仅同步日志，无投票权）
			r.addNonVoting(nodeid)
		case pb.AddWitness: // 添加见证成员（仅参与 quorum 计算，不存储完整日志）
			r.addWitness(nodeid)
		default:
			panic("unexpected config change type")
		}
	}
	return nil
}

// 处理日志查询请求
// 作用：响应其他节点或客户端的日志查询请求，获取指定范围内已提交的日志条目，用于数据同步或审计。
func (r *raft) handleLogQuery(m pb.Message) error {

	// // 确保当前无未处理的日志查询结果（避免并发查询冲突）
	if r.logQueryResult == nil {
		entries, err := r.log.getCommittedEntries(m.From, m.To, m.Hint)
		// 构建查询结果，包含日志范围、错误信息和条目数据
		r.logQueryResult = &pb.LogQueryResult{
			FirstIndex: r.log.firstIndex(),  // 本地日志的起始索引
			LastIndex:  r.log.committed + 1, // 已提交索引的下一个位置（即未提交的起始点）
			Error:      err,                 // 查询过程中的错误（如索引越界）
			Entries:    entries,             // 查询到的已提交日志条目
		}
	} else {
		// 若已有未处理的查询结果，直接 panic（防止重复查询导致状态混乱）
		panic("log query result is not nil")
	}
	return nil
}

// 处理本地定时任务
// 作用：响应节点内部的定时触发消息（LocalTick），根据节点状态执行不同的定时逻辑（如选举超时检查、领导者心跳触发）。
func (r *raft) handleLocalTick(m pb.Message) error {
	// // 若消息标记为“拒绝”（Reject=true），执行“静默状态”定时任务
	if m.Reject {
		r.quiescedTick()
		return nil
	}
	// // 否则执行常规定时任务（如选举超时检查、领导者心跳）
	return r.tick()
}

// 处理远程快照恢复
// 作用：接收并应用远程节点发送的快照数据，快速同步节点状态（当本地日志落后领导者过多时，通过快照而非逐条日志同步，提升效率）。
func (r *raft) handleRestoreRemote(m pb.Message) error {
	// 调用 restoreRemotes 方法，使用消息中的快照数据恢复节点状态
	r.restoreRemotes(m.Snapshot)
	return nil
}

//
// message handler functions used by leader
//

// 广播心跳维持领导权
// 作用：响应心跳触发消息（通常由定时任务调用），向集群所有节点广播心跳消息，维持领导者身份。
func (r *raft) handleLeaderHeartbeat(m pb.Message) error {
	// broadcastHeartbeatMessage：内部实现向集群所有节点广播 Heartbeat 消息，携带领导者的当前提交索引（Commit），供跟随者更新本地提交状态。
	r.broadcastHeartbeatMessage() // 向所有跟随者/非投票成员/见证成员发送心跳
	return nil
}

// 检查领导者是否仍拥有 Quorum
// 作用：验证领导者是否仍获得集群多数派（Quorum）支持，若失去 Quorum 则主动退化为跟随者，避免“孤领导者”导致的数据不一致。
// p69 of the raft thesis （参考 Raft 论文 6.3 节“领导者失效检测”）
func (r *raft) handleLeaderCheckQuorum(m pb.Message) error {
	r.mustBeLeader()          // 确保当前节点是领导者（防御性检查）
	if !r.leaderHasQuorum() { // 检查是否仍与多数派节点保持通信
		plog.Warningf("%s has lost quorum", r.describe())
		r.becomeFollower(r.term, NoLeader) // 失去 Quorum，退化为跟随者
	}
	return nil
}

// 处理客户端提案（核心写逻辑）
// 作用：接收并处理客户端提交的提案（如数据写入请求），确保提案在集群中安全复制并提交，是 Raft 一致性算法的核心写路径。
func (r *raft) handleLeaderPropose(m pb.Message) error {
	r.mustBeLeader() // 仅领导者可处理提案

	// 1. 若正在进行领导者转移，丢弃提案（避免转移期间数据不一致）
	// 领导者转移（Leadership Transfer）是主动交接过程，期间处理提案可能导致数据丢失，因此暂时拒绝新提案。
	if r.leaderTransfering() {
		plog.Warningf("%s dropped proposal, leader transferring", r.describe())
		r.reportDroppedProposal(m) // 记录丢弃事件（供监控/重试）
		return nil
	}

	// 2. 处理配置变更提案（确保同一时间仅一个未完成的配置变更）

	for i, e := range m.Entries {
		if e.Type == pb.ConfigChangeEntry { // 检测配置变更条目
			if r.hasPendingConfigChange() { // 存在未完成的配置变更
				plog.Warningf("%s dropped config change, pending change", r.describe())
				r.reportDroppedConfigChange(m.Entries[i]) // 【记录】丢弃的配置变更
				// Raft协议要求同一时间只能有一个未提交的配置变更条目
				// 如果已经有未提交的配置变更，新的配置变更请求需要被丢弃
				m.Entries[i] = pb.Entry{Type: pb.ApplicationEntry} // 替换为普通条目【这一步是真正的丢弃】
				// 虽然配置变更被丢弃，但仍需在日志中保留一个占位条目
				// 将其替换为普通应用条目(ApplicationEntry)，避免破坏日志结构
			}
			r.setPendingConfigChange() // 标记存在待处理的配置变更
		}
	}

	// 3. 追加提案条目到本地日志

	if err := r.appendEntries(m.Entries); err != nil {
		return err
	}

	// 4. 广播日志复制消息，要求跟随者复制新条目

	r.broadcastReplicateMessage()
	return nil
}

// 验证当前任期是否有已提交条目
// 作用：检查领导者本地日志中是否存在当前任期内已提交的条目，是 ReadIndex 协议（线性一致性读）的前置条件（参考 Raft 论文 6.4 节）。
// 为何需要此检查？
// 新当选的领导者可能尚未提交任何条目（如刚选举成功但未处理提案），此时直接响应读请求可能返回旧数据。ReadIndex 协议要求领导者必须有当前任期的已提交条目，确保其已获得多数派认可，从而安全地提供线性一致的读结果。
// p72 of the raft thesis （Raft 论文 6.4 节“只读操作优化”）
func (r *raft) hasCommittedEntryAtCurrentTerm() bool {

	if r.term == 0 { // 任期 0 无效，防御性 panic
		panic("not suppose to reach here")
	}
	//  // 获取最后提交条目的任期
	lastCommittedTerm, err := r.log.term(r.log.committed)
	if err != nil && !errors.Is(err, ErrCompacted) { // 忽略日志压缩错误
		plog.Panicf("%s failed to get term, %v", r.describe(), err)
	}
	// 验证是否为当前任期
	return lastCommittedTerm == r.term
}

// 清空可读请求队列
// 维护 readyToRead 列表，记录已满足线性一致性条件的读请求索引及上下文，供客户端读取时使用。
func (r *raft) clearReadyToRead() {
	r.readyToRead = r.readyToRead[:0]
}

// 添加可读请求到队列（记录已提交索引和系统上下文）
// 使用场景：当领导者通过 ReadIndex 协议确认某个索引已被多数派提交后，
// 调用 addReadyToRead 将该索引及请求上下文加入队列，
// 客户端随后可安全读取对应数据。
// clearReadyToRead 用于批量处理完成后清空队列，避免重复处理。
func (r *raft) addReadyToRead(index uint64, ctx pb.SystemCtx) {
	r.readyToRead = append(r.readyToRead,
		pb.ReadyToRead{
			Index:     index, // 已提交的日志索引（客户端需读取此索引之后的数据）
			SystemCtx: ctx,   // 请求上下文（如客户端 ID、请求 ID 等元数据）
		})
}

// 处理线性一致性读请求
// 作用：实现 Raft 论文 6.4 节定义的 ReadIndex 协议，确保领导者返回的数据是集群中已提交的最新状态（线性一致性读），避免因网络分区或日志未同步导致的“脏读”。
/*
ReadIndex 协议流程（非单节点集群）：

	检查提交状态：通过 hasCommittedEntryAtCurrentTerm() 确保领导者在当前任期有已提交的日志（避免新当选领导者因未提交日志而返回旧数据）。
	记录读请求：将当前已提交索引（log.committed）和请求上下文加入 readIndex 队列。
	广播心跳确认：发送带上下文的心跳（broadcastHeartbeatMessageWithHint），要求跟随者响应以确认它们已同步到该提交索引。
	多数派确认后响应：当收到多数派（Quorum）跟随者的心跳响应后，领导者将该提交索引标记为“可读”，客户端即可安全读取对应数据。
特殊节点处理：

	见证节点（Witness）：仅参与 Quorum 计算，不存储完整日志，因此直接丢弃读请求。
	单节点集群：无需多数派确认，直接使用本地已提交索引响应。
*/
// section 6.4 of the raft thesis
func (r *raft) handleLeaderReadIndex(m pb.Message) error {
	r.mustBeLeader() // 仅领导者可处理读请求
	ctx := pb.SystemCtx{
		High: m.HintHigh,
		Low:  m.Hint,
	} // 提取请求上下文（如客户端元数据）
	// 1. 见证节点（Witness）不参与数据读取，直接丢弃请求
	if _, wok := r.witnesses[m.From]; wok {
		plog.Errorf("%s dropped ReadIndex, witness node %d", r.describe(), m.From)

		//  2. 非单节点集群（需多数派确认）
	} else if !r.isSingleNodeQuorum() {
		// 检查当前任期是否有已提交的日志条目（ReadIndex 协议前置条件）
		if !r.hasCommittedEntryAtCurrentTerm() {
			// leader doesn't know the commit value of the shard
			// see raft thesis section 6.4, this is the first step of the ReadIndex protocol.
			plog.Warningf("%s dropped ReadIndex, not ready", r.describe())
			r.reportDroppedReadIndex(m)
			return nil
		}
		// 添加读请求到队列，记录当前已提交索引（log.committed）和请求上下文
		r.readIndex.addRequest(r.log.committed, ctx, m.From)
		// 广播带上下文的心跳消息，要求跟随者确认已提交索引
		r.broadcastHeartbeatMessageWithHint(ctx)
		// 3. 单节点集群（无需多数派确认，直接读取）
	} else {
		// 将已提交索引加入可读队列（供客户端读取）
		r.addReadyToRead(r.log.committed, ctx)
		// 若请求来自非投票节点，直接发送读响应
		_, ook := r.nonVotings[m.From]
		if m.From != r.replicaID && ook {
			r.send(pb.Message{
				To:       m.From,
				Type:     pb.ReadIndexResp,
				LogIndex: r.log.committed, // 已提交索引（客户端需读取此索引后的数据）
				Hint:     m.Hint,
				HintHigh: m.HintHigh,
				Commit:   m.Commit,
			})
		}
	}
	return nil
}

// 处理日志复制响应
// 作用：接收跟随者对日志复制消息（Replicate）的响应，更新跟随者的复制状态，尝试提交日志条目，并处理领导者转移（Leadership Transfer）逻辑。

/*
复制状态更新：

rp.tryUpdate(m.LogIndex)：更新跟随者 rp 的 match 字段（已成功复制的最大日志索引），仅当响应中的 LogIndex 大于当前 match 时生效。
r.tryCommit()：检查是否多数派跟随者已复制当前领导者的最新日志条目，若是则更新 log.committed（全局提交索引），确保所有节点最终同步到此状态。
领导者转移触发：
当领导者主动转移领导权给目标节点（leaderTransferTarget），且目标节点已复制所有日志（log.lastIndex() == rp.match）时，发送 TimeoutNowMessage() 触发目标节点立即发起选举，加速领导权交接。

复制冲突处理:
若跟随者因日志不一致拒绝复制（m.Reject == true），通过 rp.decreaseTo()将跟随者的next` 索引回退至其日志匹配点+1，并进入重试状态重新发送日志，确保最终一致性。
*/
func (r *raft) handleLeaderReplicateResp(m pb.Message, rp *remote) error {
	r.mustBeLeader() // 仅领导者可处理复制响应
	rp.setActive()   // 标记该跟随者状态为“活跃”（避免被判定为不可达）
	if !m.Reject {   // 复制成功（未被跟随者拒绝）
		paused := rp.isPaused() // 检查该跟随者是否处于“暂停复制”状态
		// 更新跟随者的已复制索引（match），若成功则继续处理
		if rp.tryUpdate(m.LogIndex) {
			rp.respondedTo() // 记录跟随者的最新响应时间
			// 尝试提交日志：当多数派跟随者已复制该条目时，更新全局提交索引
			ok, err := r.tryCommit()
			if err != nil {
				return nil
			}
			if ok { // 提交成功，广播新的复制消息（通知其他跟随者更新提交状态）
				r.broadcastReplicateMessage()
			} else if paused { // 若跟随者之前暂停复制，则恢复发送复制消息
				r.sendReplicateMessage(m.From)
			}

			// according to the leadership transfer protocol listed on the p29 of the raft thesis
			// 领导者转移逻辑（Raft 论文 p29）：若正在转移领导权，且目标跟随者已同步所有日志
			if r.leaderTransfering() && m.From == r.leaderTransferTarget &&
				r.log.lastIndex() == rp.match {
				r.sendTimeoutNowMessage(r.leaderTransferTarget) // 请求目标节点立即发起选举
			}
		}
	} else { // 复制被拒绝（跟随者日志不一致）

		// the replication flow control code is derived from etcd raft, it resets nextIndex to match + 1. it is thus even more conservative than the raft thesis's approach of nextIndex = nextIndex - 1 mentioned on the p21 of the thesis.

		// 复制流量控制代码来源于 etcd raft，它将 nextIndex 重置为 match + 1。
		// 因此，它比 Raft 论文中第 21 页提到的 nextIndex = nextIndex - 1 的方法更加保守。

		// 复制流量控制（replication flow control）：
		// 这是 Raft 协议中用于管理领导者向跟随者复制日志时的机制，防止发送过多消息导致网络或节点过载。
		// etcd raft 的实现：
		// 在日志复制失败时，etcd raft 将 nextIndex 设置为 match + 1（即已确认匹配的最新日志索引 + 1），以更保守地重试日志发送。
		// Raft 论文中的方法：
		// 论文中建议将 nextIndex 减 1（即 nextIndex = nextIndex - 1），逐步回退以找到匹配点。
		// 保守性对比：
		// nextIndex = match + 1 比 nextIndex = nextIndex - 1 更加保守，因为它直接跳到已知匹配位置的下一项，而不是逐步回退，从而减少不必要的重试次数。

		// 回退跟随者的下一个待复制索引（next），重试复制（类似 Raft 论文 p21 的冲突解决逻辑）
		if rp.decreaseTo(m.LogIndex, m.Hint) {
			r.enterRetryState(rp)          // 标记跟随者为“重试状态”
			r.sendReplicateMessage(m.From) // 重新发送复制消息
		}
	}
	return nil
}

// 处理心跳响应（领导者视角）
// 作用：响应跟随者对心跳消息的确认，更新跟随者状态，并触发 ReadIndex 协议的多数派确认流程。
func (r *raft) handleLeaderHeartbeatResp(m pb.Message, rp *remote) error {
	r.mustBeLeader() // 仅领导者可处理心跳响应
	//跟随者状态维护：通过 rp.setActive() 更新跟随者的活跃时间，确保领导者能准确判断集群节点的健康状态（用于后续 Quorum 检查）。
	rp.setActive()   // 标记该跟随者为“活跃”（避免被判定为不可达）
	rp.waitToRetry() // 重置跟随者的重试计时器（避免频繁重试）

	// 若跟随者的已复制索引（match）落后于领导者最新日志，触发日志复制
	if rp.match < r.log.lastIndex() {
		r.sendReplicateMessage(m.From) // 向该跟随者发送日志复制消息
	}

	// 心跳响应中携带 ReadIndex 协议所需的领导权确认信息（Hint 非空）
	// heartbeat response contains leadership confirmation requested as part of the ReadIndex protocol.
	if m.Hint != 0 { //ReadIndex 协议集成：m.Hint != 0 表示该心跳响应包含读请求的确认信息，调用 handleReadIndexLeaderConfirmation 累计多数派确认，最终完成线性一致性读。
		r.handleReadIndexLeaderConfirmation(m) // 处理读索引的多数派确认
	}
	return nil
}

//处理领导权主动转移
//作用：实现 Raft 协议的“领导权转移”（Leadership Transfer）机制，允许当前领导者主动将领导权交接给指定目标节点，用于维护、升级等场景。

/*
关键细节：

	安全检查：通过多重校验（目标有效性、无并发转移、非自身节点）避免无效转移导致的集群不稳定。
	日志同步前提：仅当目标节点已复制领导者的所有日志（rp.match == r.log.lastIndex()）时，才触发快速转移（发送 TimeoutNow 消息），确保目标节点具备成为领导者的日志基础。
	Raft 协议依据：参考 Raft 论文 §3.10 及 Thesis §9.6，领导权转移通过主动触发目标节点选举，实现无停机交接，避免领导者故障导致的选举延迟。
*/
func (r *raft) handleLeaderTransfer(m pb.Message) error {

	r.mustBeLeader() // 仅领导者可发起转移
	target := m.Hint // 从消息中提取目标节点 ID
	plog.Debugf("%s called handleLeaderTransfer, target %d", r.describe(), target)

	// 合法性检查：目标节点不能是无效节点、自身，或已存在正在进行的转移
	if target == NoNode {
		plog.Panicf("%s leader transfer target not set", r.describe())
	}
	if r.leaderTransfering() { // 正在进行转移，忽略新请求
		plog.Warningf("LeaderTransfer ignored, leader transfer is ongoing")
		return nil
	}
	if r.replicaID == target { // 目标是自身，无效
		plog.Warningf("received LeaderTransfer with target pointing to itself")
		return nil
	}

	// 检查目标节点是否为集群已知的投票成员
	rp, ok := r.remotes[target]
	if !ok {
		plog.Warningf("unknown LeaderTransfer target")
		return nil
	}

	// 初始化转移状态：设置目标节点，重置选举计时器
	r.leaderTransferTarget = target
	r.electionTick = 0
	// fast path below
	// or wait for the target node to catch up, see p29 of the raft thesis
	// 快速转移路径：若目标节点已同步所有日志（match == 最新日志索引）
	if rp.match == r.log.lastIndex() {
		r.sendTimeoutNowMessage(target) // 发送立即超时消息，触发目标节点选举
	}
	return nil
}

// 处理读索引确认（ReadIndex 协议）
// 作用：完成 ReadIndex 协议的最后一步——收集多数派节点对“已提交日志索引”的确认，确保客户端读取到的是集群一致的最新数据（线性一致性读）。
/*
关键细节：

ReadIndex 协议流程：
	领导者收到读请求后记录当前提交索引（log.committed）。
	广播带上下文的心跳消息，要求跟随者确认已同步该索引。
	本函数收集多数派（r.quorum()）确认后，标记该索引为“可读”（addReadyToRead），客户端即可安全读取。
上下文传递：通过 m.Hint 和 m.HintHigh 携带客户端请求元数据，确保响应能准确关联到原始读请求。
*/
func (r *raft) handleReadIndexLeaderConfirmation(m pb.Message) {
	// 从消息中提取读请求上下文（如客户端 ID、请求 ID）
	ctx := pb.SystemCtx{
		Low:  m.Hint,
		High: m.HintHigh,
	}
	// 确认多数派节点已认可该读索引（r.quorum() 为集群多数派数量）
	ris := r.readIndex.confirm(ctx, m.From, r.quorum())
	// 处理每个确认结果：本地记录或向请求节点发送响应
	for _, s := range ris {
		if s.from == NoNode || s.from == r.replicaID {
			// 本地节点或无来源的确认：添加到可读队列（供客户端读取）
			r.addReadyToRead(s.index, s.ctx)
		} else {
			// 其他节点的确认：发送 ReadIndexResp 消息通知结果
			r.send(pb.Message{
				To:       s.from,
				Type:     pb.ReadIndexResp,
				LogIndex: s.index,    // 已确认的提交索引
				Hint:     m.Hint,     // 原始请求上下文（低 64 位）
				HintHigh: m.HintHigh, // 原始请求上下文（高 64 位）
			})
		}
	}
}

// 处理快照同步状态
// 作用：领导者接收跟随者对快照同步的状态响应（成功/失败），更新目标节点的同步状态，确保快照复制流程正常结束。

/*
关键细节：

快照同步场景：当跟随者日志落后过多时，领导者会发送快照（而非逐条日志）进行快速同步。本函数处理跟随者对快照的反馈。
状态流转：同步结束后（成功/失败），目标节点从 remoteSnapshot（快照中）转为 wait（等待）状态，避免重复同步。
进度跟踪：通过 rp.setSnapshotAck 记录快照确认信息，领导者可通过 checkPendingSnapshotAck 监控所有节点的同步进度。
*/
func (r *raft) handleLeaderSnapshotStatus(m pb.Message, rp *remote) error {
	// 仅处理处于“快照同步”状态的节点（避免干扰其他状态的节点）
	if rp.state != remoteSnapshot {
		return nil
	}
	// m.Hint == 0 表示快照同步结果通知（成功/失败）
	if m.Hint == 0 {
		if m.Reject { //快照同步失败
			rp.clearPendingSnapshot() //清除该节点的‘待处理快照’标记
			plog.Warningf("%s snapshot failed, %s is now in wait state",
				r.describe(), ReplicaID(m.From))
		} else { //快照同步成功
			plog.Debugf("%s snapshot succeeded, %s in wait state now, next %d",
				r.describe(), ReplicaID(m.From), rp.next)
		}
		rp.becomeWait() //无论成败，节点都进入等待状态（等待下一次同步指令）
	} else { //hint != 0 表示快照确认信息（用于领导者跟踪快照进度）
		rp.setSnapshotAck(m.Hint, m.Reject) //记录快照确认状态（索引+是否拒绝）
		r.snapshotting = true               //标记 集群正在进行快照同步
	}
	return nil
}

// 处理节点不可达事件
// 作用：当领导者检测到 某个跟随者节点不可达（如网络超时），将其转入‘重试状态’，触发后续的重试机制以恢复通信和日志同步。
/*
触发条件：通常由底层网络模块（如定期健康检查）发送 Unreachable 消息触发，指示目标节点暂时无法通信。
重试状态：通过 enterRetryState 将节点状态从 remoteReplicate（正常复制）转为 retry（重试），领导者会调整复制策略（如降低频率、回退日志索引）以尝试恢复同步。
*/
func (r *raft) handleLeaderUnreachable(m pb.Message, rp *remote) error {
	plog.Debugf("%s received Unreachable, %s entered retry state",
		r.describe(), ReplicaID(m.From))
	r.enterRetryState(rp) //将不可达节点转入‘重试状态’
	return nil
}

// 处理流量控制消息
// 作用：根据跟随者反馈的流量负载状态，动态调整领导者向其发送日志/快照的速率，避免网络拥塞或节点过载。
/*
流量控制机制：r.rl 是速率限制器实例，通过接收跟随者发送的 RateLimit 消息（包含当前负载信息，如 m.Hint），动态调整发送窗口或间隔，避免“快领导者”压垮“慢跟随者”。
灵活性：支持动态启用/禁用，禁用时直接丢弃流量控制消息，不影响核心复制逻辑。
*/
func (r *raft) handleLeaderRateLimit(m pb.Message) error {
	if r.rl.Enabled() { // 若流量控制模块已启用
		// 更新目标跟随者的状态（如当前负载、可接受的复制速率）
		r.rl.SetFollowerState(m.From, m.Hint)
	} else {
		plog.Warningf("%s dropped rate limit msg, rl disabled", r.describe())
	}
	return nil
}

// 节点状态转为重试
// 作用：将指定节点的状态从“正常复制”转为“重试”，是处理复制失败或节点不可达的核心状态转换函数。

// 状态机设计：
//
//	remote 结构体维护节点的复制状态（如 replicate/snapshot/retry/wait），
//	enterRetryState 是状态流转的“开关”，确保只有正常复制中的节点会进入重试流程。
//
// 重试触发场景：通常由 handleLeaderUnreachable（节点不可达）或 handleLeaderReplicateResp（复制被拒绝）调用，后续领导者会通过 sendReplicateMessage 重试日志同步。
func (r *raft) enterRetryState(rp *remote) {
	if rp.state == remoteReplicate { // 仅当节点当前处于“正常复制”状态时，才转为“重试”状态（避免重复转换）
		rp.becomeRetry() // 调用 remote 方法切换状态
	}
}

// 检查快照确认超时
// 作用：领导者定期检查快照同步的确认状态，处理超时未响应的节点，确保快照复制不会无限期阻塞。

// 超时处理：通过 rp.delayed.tick() 检查快照同步是否超时（如配置的 30s 内未收到确认），超时则主动构造 SnapshotStatus 消息，标记为拒绝，避免无限等待。
// 全覆盖检查：依次检查 remotes（投票成员）、nonVotings（非投票成员）、witnesses（见证成员），确保所有类型节点的快照状态均被处理。
// 快照状态重置：若所有节点快照均已确认或超时，r.snapshotting 会被设为 false，结束本次快照同步流程。
func (r *raft) checkPendingSnapshotAck() error {
	if r.isLeader() && r.snapshotting { // 仅领导者且快照同步中触发检查

		// 定义检查函数：遍历节点集合，处理超时快照
		check := func(m map[uint64]*remote) error {
			for from, rp := range m {
				if rp.state == remoteSnapshot { // 仅处理“快照同步中”的节点
					if rp.delayed.tick() { // 检查快照是否超时（delayed 是延迟计时器）
						// 主动构造“快照状态”消息，模拟节点超时响应
						if err := r.Handle(pb.Message{
							Type:   pb.SnapshotStatus,
							From:   from,
							Reject: rp.delayed.rejected, // 标记为超时拒绝
							Hint:   0,
						}); err != nil {
							return err
						}
						rp.clearSnapshotAck() // 清除该节点的待确认快照
					} else {
						r.snapshotting = true // 未超时，保持快照同步状态
					}
				}
			}
			return nil
		}
		r.snapshotting = false // 先假设所有快照已处理
		// 依次检查投票成员、非投票成员、见证成员的快照状态
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

// handleNonVotingReplicate 处理非投票节点的复制消息，通过委托给follower处理器来处理
// 参数:
//   - m: 需要处理的复制消息
//
// 返回值:
//   - error: 处理过程中遇到的任何错误
func (r *raft) handleNonVotingReplicate(m pb.Message) error {
	return r.handleFollowerReplicate(m)
}

// handleNonVotingHeartbeat 处理非投票节点的心跳消息，通过委托给follower处理器来处理
// 参数:
//   - m: 需要处理的心跳消息
//
// 返回值:
//   - error: 处理过程中遇到的任何错误
func (r *raft) handleNonVotingHeartbeat(m pb.Message) error {
	return r.handleFollowerHeartbeat(m)
}

// handleNonVotingSnapshot 处理非投票节点的快照消息，通过委托给follower处理器来处理
// 参数:
//   - m: 需要处理的快照消息
//
// 返回值:
//   - error: 处理过程中遇到的任何错误
func (r *raft) handleNonVotingSnapshot(m pb.Message) error {
	return r.handleFollowerInstallSnapshot(m)
}

// handleNonVotingPropose 处理非投票节点的提案消息，通过委托给follower处理器来处理
// 参数:
//   - m: 需要处理的提案消息
//
// 返回值:
//   - error: 处理过程中遇到的任何错误
func (r *raft) handleNonVotingPropose(m pb.Message) error {
	return r.handleFollowerPropose(m)
}

// handleNonVotingReadIndex 处理非投票节点的读索引消息，通过委托给follower处理器来处理
// 参数:
//   - m: 需要处理的读索引消息
//
// 返回值:
//   - error: 处理过程中遇到的任何错误
func (r *raft) handleNonVotingReadIndex(m pb.Message) error {
	return r.handleFollowerReadIndex(m)
}

// handleNonVotingReadIndexResp 处理非投票节点的读索引响应消息，通过委托给follower处理器来处理
// 参数:
//   - m: 需要处理的读索引响应消息
//
// 返回值:
//   - error: 处理过程中遇到的任何错误
func (r *raft) handleNonVotingReadIndexResp(m pb.Message) error {
	return r.handleFollowerReadIndexResp(m)
}

//
// message handlers used by witness, re-route them to follower handlers
//

// handleWitnessReplicate 处理见证节点收到的日志复制请求
// 见证节点仅参与法定人数计算，不存储完整日志，复用跟随者的日志复制处理逻辑
func (r *raft) handleWitnessReplicate(m pb.Message) error {
	return r.handleFollowerReplicate(m)
}

// handleWitnessHeartbeat 处理见证节点收到的领导者心跳消息
// 维持与领导者的连接状态，复用跟随者的心跳处理逻辑
func (r *raft) handleWitnessHeartbeat(m pb.Message) error {
	return r.handleFollowerHeartbeat(m)
}

// handleWitnessSnapshot 处理见证节点收到的快照安装请求
// 快速同步集群状态，复用跟随者的快照安装处理逻辑
func (r *raft) handleWitnessSnapshot(m pb.Message) error {
	return r.handleFollowerInstallSnapshot(m)
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
//
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

// defaultHandle 是 Raft 节点的消息分发入口，根据当前节点状态（state）和消息类型（Type），
// 从 handlers 映射中查找并调用对应的状态专用处理器。实现消息与状态的解耦，确保不同状态下消息处理逻辑隔离。
// 参数：
//   - r: raft 节点实例，包含当前状态和 handlers 映射
//   - m: 待处理的消息（如 Propose/Replicate/Heartbeat 等）
//
// 返回值：
//   - error: 处理器返回的错误（无匹配处理器时返回 nil）
func defaultHandle(r *raft, m pb.Message) error {
	// 从状态-消息类型映射中查找处理器（handlers 由 initializeHandlerMap 初始化）
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
	r.handlers[candidate][pb.Heartbeat] = r.handleCandidateHeartbeat             // 收到领导者心跳 → 退化为跟随者
	r.handlers[candidate][pb.Propose] = r.handleCandidatePropose                 // 收到提案 → 丢弃（仅领导者可处理）
	r.handlers[candidate][pb.ReadIndex] = r.handleCandidateReadIndex             // 收到读请求 → 丢弃（仅领导者可处理）
	r.handlers[candidate][pb.Replicate] = r.handleCandidateReplicate             // 收到日志复制请求 → 退化为跟随者
	r.handlers[candidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot // 收到快照请求 → 退化为跟随者
	r.handlers[candidate][pb.RequestVoteResp] = r.handleCandidateRequestVoteResp // 收到投票响应 → 统计票数决定是否晋升
	r.handlers[candidate][pb.Election] = r.handleNodeElection                    // 收到选举触发消息 → 发起选举（如超时）
	r.handlers[candidate][pb.RequestVote] = r.handleNodeRequestVote              // 收到其他节点投票请求 → 按规则投票
	r.handlers[candidate][pb.RequestPreVote] = r.handleNodeRequestPreVote        // 收到预投票请求 → 按规则预投票
	r.handlers[candidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange       // 配置变更事件 → 应用配置
	r.handlers[candidate][pb.LocalTick] = r.handleLocalTick                      // 本地定时任务 → 检查选举超时
	r.handlers[candidate][pb.SnapshotReceived] = r.handleRestoreRemote           // 收到快照 → 恢复节点状态
	r.handlers[candidate][pb.LogQuery] = r.handleLogQuery                        // 日志查询 → 返回查询结果

	// preVoteCandidate（预选举候选者状态）：预选举阶段专用，避免网络分区导致的任期膨胀
	// 复用 candidate 的大部分处理器，仅替换预投票响应处理器
	r.handlers[preVoteCandidate][pb.Heartbeat] = r.handleCandidateHeartbeat                          // 收到领导者心跳 → 退化为跟随者
	r.handlers[preVoteCandidate][pb.Propose] = r.handleCandidatePropose                              // 收到客户端提案 → 丢弃（仅领导者可处理）
	r.handlers[preVoteCandidate][pb.ReadIndex] = r.handleCandidateReadIndex                          // 收到读请求 → 丢弃（仅领导者可处理）
	r.handlers[preVoteCandidate][pb.Replicate] = r.handleCandidateReplicate                          // 收到日志复制请求 → 退化为跟随者
	r.handlers[preVoteCandidate][pb.InstallSnapshot] = r.handleCandidateInstallSnapshot              // 收到快照安装请求 → 退化为跟随者
	r.handlers[preVoteCandidate][pb.RequestPreVoteResp] = r.handlePreVoteCandidateRequestPreVoteResp // 预投票响应 → 统计预投票结果
	r.handlers[preVoteCandidate][pb.Election] = r.handleNodeElection                                 // 收到选举触发消息 → 发起预选举
	r.handlers[preVoteCandidate][pb.RequestVote] = r.handleNodeRequestVote                           // 收到投票请求 → 按规则投票
	r.handlers[preVoteCandidate][pb.RequestPreVote] = r.handleNodeRequestPreVote                     // 收到预投票请求 → 按规则预投票
	r.handlers[preVoteCandidate][pb.ConfigChangeEvent] = r.handleNodeConfigChange                    // 配置变更事件 → 应用配置变更
	r.handlers[preVoteCandidate][pb.LocalTick] = r.handleLocalTick                                   // 本地定时任务 → 检查预选举超时
	r.handlers[preVoteCandidate][pb.SnapshotReceived] = r.handleRestoreRemote                        // 收到快照数据 → 恢复节点状态
	r.handlers[preVoteCandidate][pb.LogQuery] = r.handleLogQuery                                     // 日志查询请求 → 返回查询结果

	// follower（跟随者状态）：处理提案转发、日志复制、心跳、快照安装等消息
	// follower（跟随者状态）：被动接收领导者消息，转发客户端请求，核心是维持与领导者的同步
	r.handlers[follower][pb.Propose] = r.handleFollowerPropose                 // 收到提案 → 转发给领导者
	r.handlers[follower][pb.Replicate] = r.handleFollowerReplicate             // 收到日志复制 → 同步日志并响应
	r.handlers[follower][pb.Heartbeat] = r.handleFollowerHeartbeat             // 收到心跳 → 更新提交索引并响应
	r.handlers[follower][pb.ReadIndex] = r.handleFollowerReadIndex             // 收到读请求 → 转发给领导者
	r.handlers[follower][pb.LeaderTransfer] = r.handleFollowerLeaderTransfer   // 收到领导者转移请求 → 转发给领导者
	r.handlers[follower][pb.ReadIndexResp] = r.handleFollowerReadIndexResp     // 收到读响应 → 记录读索引（供客户端读取）
	r.handlers[follower][pb.InstallSnapshot] = r.handleFollowerInstallSnapshot // 收到快照 → 安装快照以快速同步
	r.handlers[follower][pb.Election] = r.handleNodeElection                   // 收到选举触发消息 → 发起选举（如超时）
	r.handlers[follower][pb.RequestVote] = r.handleNodeRequestVote             // 收到投票请求 → 按规则投票
	r.handlers[follower][pb.RequestPreVote] = r.handleNodeRequestPreVote       // 收到预投票请求 → 按规则预投票
	r.handlers[follower][pb.TimeoutNow] = r.handleFollowerTimeoutNow           // 收到立即超时消息 → 触发选举（领导者转移用）
	r.handlers[follower][pb.ConfigChangeEvent] = r.handleNodeConfigChange      // 配置变更事件 → 应用配置
	r.handlers[follower][pb.LocalTick] = r.handleLocalTick                     // 本地定时任务 → 检查选举超时
	r.handlers[follower][pb.SnapshotReceived] = r.handleRestoreRemote          // 收到快照 → 恢复节点状态
	r.handlers[follower][pb.LogQuery] = r.handleLogQuery                       // 日志查询 → 返回查询结果

	// leader（领导者状态）：处理提案、读索引、复制响应、心跳响应、领导者转移等核心功能
	// leader（领导者状态）：主动处理客户端请求、复制日志、维持领导权，核心是保证集群一致性
	r.handlers[leader][pb.LeaderHeartbeat] = r.handleLeaderHeartbeat            // 收到心跳触发 → 广播心跳（维持领导权）
	r.handlers[leader][pb.CheckQuorum] = r.handleLeaderCheckQuorum              // 收到检查 quorum 消息 → 验证是否仍有多数派支持
	r.handlers[leader][pb.Propose] = r.handleLeaderPropose                      // 收到提案 → 追加日志并复制到集群
	r.handlers[leader][pb.ReadIndex] = r.handleLeaderReadIndex                  // 收到读请求 → 执行 ReadIndex 协议（线性一致性读）
	r.handlers[leader][pb.ReplicateResp] = lw(r, r.handleLeaderReplicateResp)   // 收到复制响应 → 更新复制进度，尝试提交日志
	r.handlers[leader][pb.HeartbeatResp] = lw(r, r.handleLeaderHeartbeatResp)   // 收到心跳响应 → 确认节点存活，更新复制状态
	r.handlers[leader][pb.SnapshotStatus] = lw(r, r.handleLeaderSnapshotStatus) // 收到快照状态 → 处理快照复制结果（成功/失败）
	r.handlers[leader][pb.Unreachable] = lw(r, r.handleLeaderUnreachable)       // 收到节点不可达消息 → 进入重试状态
	r.handlers[leader][pb.LeaderTransfer] = r.handleLeaderTransfer              // 收到领导者转移请求 → 启动转移流程（如同步目标节点日志）
	r.handlers[leader][pb.Election] = r.handleNodeElection                      // 收到选举触发消息 → 忽略（自身已是领导者）
	r.handlers[leader][pb.RequestVote] = r.handleNodeRequestVote                // 收到投票请求 → 拒绝（自身任期更高）
	r.handlers[leader][pb.RequestPreVote] = r.handleNodeRequestPreVote          // 收到预投票请求 → 拒绝（自身任期更高）
	r.handlers[leader][pb.ConfigChangeEvent] = r.handleNodeConfigChange         // 配置变更事件 → 应用配置
	r.handlers[leader][pb.LocalTick] = r.handleLocalTick                        // 本地定时任务 → 检查心跳超时、触发日志复制
	r.handlers[leader][pb.SnapshotReceived] = r.handleRestoreRemote             // 收到快照 → 恢复节点状态
	r.handlers[leader][pb.RateLimit] = r.handleLeaderRateLimit                  // 收到流量控制消息 → 调整复制速率
	r.handlers[leader][pb.LogQuery] = r.handleLogQuery                          // 日志查询 → 返回查询结果

	// 新增：注册领导者链式连接消息处理器
	r.handlers[leader][pb.LeaderChainConnect] = r.handleLeaderChainConnect
	// 新增：注册链式连接确认消息处理器
	r.handlers[leader][pb.LeaderChainAck] = r.handleLeaderChainAck
	r.handlers[leader][pb.LeaderChainPing] = r.handleLeaderChainPing // 健康检查 Ping
	// leader（领导者状态）：添加 LeaderChainPong 处理器
	r.handlers[leader][pb.LeaderChainPong] = r.handleLeaderChainPong             // 处理下游 Pong 消息
	r.handlers[leader][pb.LeaderChainDisconnect] = r.handleLeaderChainDisconnect // 链式连接断开（需实现对应处理函数）

	// nonVoting（非投票成员状态）：仅同步日志不参与选举，用于新节点加入时的数据预热
	r.handlers[nonVoting][pb.Heartbeat] = r.handleNonVotingHeartbeat         // 复用跟随者心跳处理器
	r.handlers[nonVoting][pb.Replicate] = r.handleNonVotingReplicate         // 复用跟随者日志复制处理器
	r.handlers[nonVoting][pb.InstallSnapshot] = r.handleNonVotingSnapshot    // 复用跟随者快照安装处理器
	r.handlers[nonVoting][pb.RequestVote] = r.handleNodeRequestVote          // 收到投票请求 → 拒绝（无投票权）
	r.handlers[nonVoting][pb.RequestPreVote] = r.handleNodeRequestPreVote    // 收到预投票请求 → 拒绝（无投票权）
	r.handlers[nonVoting][pb.Propose] = r.handleNonVotingPropose             // 收到提案 → 转发给领导者
	r.handlers[nonVoting][pb.ReadIndex] = r.handleNonVotingReadIndex         // 收到读请求 → 转发给领导者
	r.handlers[nonVoting][pb.ReadIndexResp] = r.handleNonVotingReadIndexResp // 收到读响应 → 记录读索引
	r.handlers[nonVoting][pb.ConfigChangeEvent] = r.handleNodeConfigChange   // 配置变更事件 → 应用配置
	r.handlers[nonVoting][pb.LocalTick] = r.handleLocalTick                  // 本地定时任务 → 维持与领导者同步
	r.handlers[nonVoting][pb.SnapshotReceived] = r.handleRestoreRemote       // 收到快照 → 恢复节点状态
	r.handlers[nonVoting][pb.LogQuery] = r.handleLogQuery                    // 日志查询 → 返回查询结果

	// witness（见证成员状态）：仅参与法定人数计算，不存储完整日志，用于提升可用性
	r.handlers[witness][pb.Heartbeat] = r.handleWitnessHeartbeat         // 复用跟随者心跳处理器
	r.handlers[witness][pb.Replicate] = r.handleWitnessReplicate         // 复用跟随者日志复制处理器（仅同步关键元数据）
	r.handlers[witness][pb.InstallSnapshot] = r.handleWitnessSnapshot    // 复用跟随者快照安装处理器
	r.handlers[witness][pb.RequestVote] = r.handleNodeRequestVote        // 收到投票请求 → 按规则投票（仅参与法定人数）
	r.handlers[witness][pb.RequestPreVote] = r.handleNodeRequestPreVote  // 收到预投票请求 → 按规则预投票
	r.handlers[witness][pb.ConfigChangeEvent] = r.handleNodeConfigChange // 配置变更事件 → 应用配置
	r.handlers[witness][pb.LocalTick] = r.handleLocalTick                // 本地定时任务 → 维持与领导者同步
	r.handlers[witness][pb.SnapshotReceived] = r.handleRestoreRemote     // 收到快照 → 恢复节点状态

}

/*
函数核心价值：
	checkHandlerMap 是 Raft 节点启动前的「安全检查哨」，
	通过验证「状态-消息类型」处理器的合法性，
	确保 initializeHandlerMap 未注册违反协议约束的处理器（如领导者处理跟随者专属的 Heartbeat 消息）。

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
	// following states/types are not supposed to have handler filled in
	// checks 定义了不允许存在处理器的状态-消息类型组合，这些组合违反 Raft 协议状态约束：
	//   - 领导者不应处理跟随者专属消息（如 Heartbeat/Replicate）
	//   - 跟随者不应处理领导者专属响应（如 ReplicateResp/HeartbeatResp）
	//   - 非投票成员/见证成员不应处理选举相关消息（如 Election）
	checks := []struct {
		stateType State          // Raft 节点状态（如 leader/follower/candidate）
		msgType   pb.MessageType // 消息类型（如 pb.Heartbeat/pb.Replicate）
	}{
		{leader, pb.Heartbeat},                // 领导者不处理 Heartbeat（跟随者专属）
		{leader, pb.Replicate},                // 领导者不处理 Replicate（跟随者专属）
		{leader, pb.InstallSnapshot},          // 领导者不处理 InstallSnapshot（跟随者专属）
		{leader, pb.ReadIndexResp},            // 领导者不处理 ReadIndexResp（跟随者专属）
		{leader, pb.RequestPreVoteResp},       // 领导者不处理 RequestPreVoteResp（预选举候选者专属）
		{follower, pb.ReplicateResp},          // 跟随者不处理 ReplicateResp（领导者专属）
		{follower, pb.HeartbeatResp},          // 跟随者不处理 HeartbeatResp（领导者专属）
		{follower, pb.SnapshotStatus},         // 跟随者不处理 SnapshotStatus（领导者专属）
		{follower, pb.Unreachable},            // 跟随者不处理 Unreachable（领导者专属）
		{follower, pb.RequestPreVoteResp},     // 跟随者不处理 RequestPreVoteResp（预选举候选者专属）
		{candidate, pb.ReplicateResp},         // 候选者不处理 ReplicateResp（领导者专属）
		{candidate, pb.HeartbeatResp},         // 候选者不处理 HeartbeatResp（领导者专属）
		{candidate, pb.SnapshotStatus},        // 候选者不处理 SnapshotStatus（领导者专属）
		{candidate, pb.Unreachable},           // 候选者不处理 Unreachable（领导者专属）
		{candidate, pb.RequestPreVoteResp},    // 候选者不处理 RequestPreVoteResp（预选举候选者专属）
		{preVoteCandidate, pb.ReplicateResp},  // 预选举候选者不处理 ReplicateResp（领导者专属）
		{preVoteCandidate, pb.HeartbeatResp},  // 预选举候选者不处理 HeartbeatResp（领导者专属）
		{preVoteCandidate, pb.SnapshotStatus}, // 预选举候选者不处理 SnapshotStatus（领导者专属）
		{preVoteCandidate, pb.Unreachable},    // 预选举候选者不处理 Unreachable（领导者专属）
		{nonVoting, pb.Election},              // 非投票成员不处理 Election（选举相关）
		{nonVoting, pb.RequestVoteResp},       // 非投票成员不处理 RequestVoteResp（无投票权）
		{nonVoting, pb.ReplicateResp},         // 非投票成员不处理 ReplicateResp（领导者专属）
		{nonVoting, pb.HeartbeatResp},         // 非投票成员不处理 HeartbeatResp（领导者专属）
		{nonVoting, pb.RequestPreVoteResp},    // 非投票成员不处理 RequestPreVoteResp（无投票权）
		{witness, pb.Election},                // 见证成员不处理 Election（选举相关）
		{witness, pb.Propose},                 // 见证成员不处理 Propose（仅存储元数据）
		{witness, pb.ReadIndex},               // 见证成员不处理 ReadIndex（仅参与法定人数）
		{witness, pb.ReadIndexResp},           // 见证成员不处理 ReadIndexResp（仅参与法定人数）
		{witness, pb.RequestVoteResp},         // 见证成员不处理 RequestVoteResp（仅参与预投票）
		{witness, pb.ReplicateResp},           // 见证成员不处理 ReplicateResp（领导者专属）
		{witness, pb.HeartbeatResp},           // 见证成员不处理 HeartbeatResp（领导者专属）
		{witness, pb.RequestPreVoteResp},      // 见证成员不处理 RequestPreVoteResp（仅参与预投票）
		{witness, pb.LogQuery},                // 见证成员不处理 LogQuery（无完整日志）
	}
	// 遍历检查列表，验证无效组合是否存在处理器
	for _, tt := range checks {
		f := r.handlers[tt.stateType][tt.msgType]
		if f != nil {
			panic("unexpected msg handler") // 发现无效处理器，触发 panic 终止程序（防御性编程）
		}
	}
}

// 新增函数

/*
优化点说明
功能完整性：

同时支持上游连接建立（记录 chainUpstream）、双向确认（sendChainConnectAck）和下游传播（propagateChainConnect），形成完整的链式连接逻辑。
健壮性增强：

添加参数校验（m.ShardID 和 m.From 非零检查），避免无效请求导致的状态异常。
错误隔离：确认消息发送失败时仅记录警告，不阻断主流程，支持后续重试。
可维护性提升：

拆分独立逻辑为辅助函数（sendChainConnectAck/propagateChainConnect），符合单一职责原则。
详细日志输出：包含上下游 ShardID 和 LeaderID，便于问题定位。
协议兼容性：

复用现有 r.send() 函数处理消息发送，确保与 Raft 协议的消息格式（如 From/Term 字段）统一。
通过 pb.LeaderChainAck 确认消息实现双向验证，避免单边连接失效。
可扩展性：

下游传播逻辑支持动态获取下一跳 ShardID（nextLeader.NextShard），可对接配置中心或元数据服务。
重试机制预留扩展点（通过定时任务重试下游无领导者的场景）。
*/

// 新增：处理领导者链式连接请求
func (r *raft) handleLeaderChainConnect(m pb.Message) error {
	// 1. 基础校验：仅领导者处理，且消息必须包含上游 ShardID 和 LeaderID
	r.mustBeLeader()
	if m.ShardID == 0 || m.From == 0 {
		return fmt.Errorf("invalid chain connect request: shard %d, leader %d", m.ShardID, m.From)
	}

	// 2. 记录上游领导者信息（覆盖旧连接，确保最新性）
	plog.Infof("%s establishing chain connection with upstream shard %d (leader %d)",
		r.describe(), m.ShardID, m.From)
	r.chainUpstream = struct {
		ShardID  uint64 // 上游 Raft 组 ID
		LeaderID uint64 // 上游领导者节点 ID
	}{m.ShardID, m.From}

	// 3. 发送连接确认（双向验证，确保上游收到）
	if err := r.sendChainConnectAck(m); err != nil {
		plog.Warningf("%s failed to send chain ack to upstream %d: %v", r.describe(), m.From, err)
		// 不返回错误，仅记录警告（连接可重试）
	}

	// 4. 向下游 Shard 传播链式连接（递归构建完整链）
	// m.NextShard 为当前 Shard 的下一跳目标 ShardID（由上游传递或配置指定）
	if m.NextShard != 0 {
		r.propagateChainConnect(m.NextShard)
	}

	return nil
}

// 发送连接确认消息给上游领导者
func (r *raft) sendChainConnectAck(m pb.Message) error {
	ackMsg := pb.Message{
		Type:      pb.LeaderChainAck, // 需在 raftpb 中定义该消息类型
		From:      r.replicaID,       // 当前领导者节点 ID
		To:        m.From,            // 上游领导者节点 ID
		ShardID:   r.shardID,         // 当前 Raft 组 ID
		Term:      r.term,            // 当前任期（用于版本校验）
		NextShard: m.NextShard,       // 透传下游目标 ShardID（辅助上游调试）
	}
	// 复用 raft.send() 确保消息格式统一（自动设置 From 和 Term）
	r.send(ackMsg)
	return nil
}

// 新增：获取目标分片的领导者信息及下一跳分片ID
func (r *raft) getShardLeader(shardID uint64) (struct {
	LeaderID  uint64 // 目标分片领导者节点ID
	NextShard uint64 // 链式传播的下一跳分片ID
}, error) {
	if r.resolver == nil {
		return struct {
			LeaderID  uint64
			NextShard uint64
		}{0, 0}, errors.New("leader resolver not initialized")
	}

	// 1. 查询目标分片的当前领导者ID
	leaderID, err := r.resolver.GetShardLeader(shardID)
	if err != nil {
		return struct {
			LeaderID  uint64
			NextShard uint64
		}{0, 0}, errors.Wrapf(err, "failed to get leader for shard %d", shardID)
	}
	if leaderID == NoLeader {
		return struct {
			LeaderID  uint64
			NextShard uint64
		}{0, 0}, errors.Errorf("no leader for shard %d", shardID)
	}

	// 2. 获取下一跳分片ID（示例：从配置或元数据中获取，此处需根据实际业务实现）
	// 实际场景可能需要从配置中心/etcd/ZooKeeper查询当前分片的下游分片ID
	nextShard := r.getNextShardInChain(shardID)

	return struct {
		LeaderID  uint64
		NextShard uint64
	}{leaderID, nextShard}, nil
}

// 新增：获取链式传播的下一跳分片ID（示例实现，需替换为实际逻辑）
// func (r *raft) getNextShardInChain(currentShardID uint64) uint64 {
// 	// 示例逻辑：从配置中读取当前分片的下游分片映射
// 	// 实际应用中可能需要从外部配置/元数据服务获取
// 	chainConfig := map[uint64]uint64{
// 		// 格式：当前分片ID → 下一跳分片ID
// 		100: 200, // 示例：分片100的下一跳是分片200
// 		200: 300, // 示例：分片200的下一跳是分片300
// 	}
// 	return chainConfig[currentShardID]
// }

// 新增：获取链式下一跳分片ID
func (r *raft) getNextShardInChain(currentShardID uint64) uint64 {

}

// 向下游 Shard 传播链式连接请求
func (r *raft) propagateChainConnect(nextShardID uint64) {
	// 获取下游 Shard 的当前领导者信息（需实现 Shard 元信息查询逻辑）
	nextLeader, err := r.getShardLeader(nextShardID)
	if err != nil || nextLeader.LeaderID == raftio.NoLeader {
		plog.Warningf("%s downstream shard %d has no leader, retry later", r.describe(), nextShardID)
		// 触发定时重试（可通过 r.ticker 实现，此处简化）
		return
	}

	// 构造下游连接请求
	propagateMsg := pb.Message{
		Type:      pb.LeaderChainConnect, // 复用同一消息类型
		From:      r.replicaID,           // 当前领导者节点 ID
		To:        nextLeader.LeaderID,   // 下游领导者节点 ID
		ShardID:   r.shardID,             // 当前 Raft 组 ID（作为下游的上游）
		NextShard: nextLeader.NextShard,  // 下游的下一跳目标（从配置或元信息获取）
		Term:      r.term,                // 当前任期
	}
	r.send(propagateMsg)
	plog.Infof("%s propagated chain connect to downstream shard %d (leader %d)",
		r.describe(), nextShardID, nextLeader.LeaderID)
}

// 处理下游 Shard 发送的链式连接确认消息
func (r *raft) handleLeaderChainAck(m pb.Message) error {
	r.mustBeLeader() // 仅领导者处理确认消息
	plog.Infof("%s received chain ack from downstream shard %d (leader %d)",
		r.describe(), m.ShardID, m.From)

	// 1. 记录下游领导者信息（可选：用于监控链式完整性）
	r.chainDownstream = struct {
		ShardID     uint64
		LeaderID    uint64
		lastPingAck time.Time // 新增：健康检查时间戳
	}{
		ShardID:     m.ShardID,
		LeaderID:    m.From,
		lastPingAck: time.Now(), // 初始化：连接建立时记录当前时间，启动健康检查计时
	}

	// 2. 可选：更新链式连接指标（如连接延迟、成功率）
	if r.events != nil {
		// 示例：触发链式连接成功事件（需扩展 server.IRaftEventListener）
		// r.events.ChainConnected(server.ChainInfo{...})
	}

	return nil
}

// 新增：检查链式连接健康状态。双向健康检查（同时验证上游连接是否存活 + 下游连接是否存活）。
// checkChainHealth：全链路双向检查，无上游依赖，低频触发
func (r *raft) checkChainHealth() {
	// 仅领导者需要执行健康检查
	if !r.isLeader() {
		return
	}

	// 1. 上游连接健康检查：发送Ping消息
	if r.chainUpstream.LeaderID != NoNode { // 存在上游领导者
		pingMsg := pb.Message{
			Type:    pb.LeaderChainPing,
			From:    r.replicaID,
			To:      r.chainUpstream.LeaderID,
			ShardID: r.shardID,
			Term:    r.term, // 携带当前任期，用于接收方验证消息有效性
		}
		r.send(pingMsg)
		plog.Debugf("%s sent chain ping to upstream shard %d (leader %d)",
			r.describe(), r.chainUpstream.ShardID, r.chainUpstream.LeaderID)
	}

	// 2. 下游连接超时检查：超过30秒未收到Pong则重连
	if r.chainDownstream.LeaderID != NoNode { // 存在下游领导者
		// lastPingAck为零值表示从未收到过Pong（初始状态）
		if !r.chainDownstream.lastPingAck.IsZero() {
			// 超时阈值：30秒（可根据RTT配置动态调整）
			if time.Since(r.chainDownstream.lastPingAck) > 30*time.Second {
				plog.Warningf("%s downstream chain connection (shard %d, leader %d) timed out, last ack at %v",
					r.describe(), r.chainDownstream.ShardID, r.chainDownstream.LeaderID, r.chainDownstream.lastPingAck)
				// 触发下游重连
				r.propagateChainConnect(r.chainDownstream.ShardID)
			}
		}
	}
}

// 新增：定期检查链式连接健康状态（集成到领导者定时任务）
// 上游领导者主动发起下游健康检查的专用函数（用于上游节点验证下游节点存活状态），
// 该函数仅在上游领导者收到下游节点的 Pong 响应时被调用，用于更新下游连接的 lastPingAck 时间戳（避免误判超时）。它不处理上游连接的健康检查，也不主动发起健康检查请求（需依赖 LeaderChainPing 消息触发）。
// leaderChainHealthTick：上游依赖实时维护，适用于高频、有上游连接场景下的状态保活，无上游时不工作，日志简化，侧重快速响应下游超时。
func (r *raft) leaderChainHealthTick() {
	if !r.isLeader() || r.chainUpstream.LeaderID == 0 { // 无上游则返回
		return
	}
	// 发送健康检查Ping消息给上游领导者
	r.send(pb.Message{
		Type:    pb.LeaderChainPing,
		From:    r.replicaID,
		To:      r.chainUpstream.LeaderID,
		ShardID: r.shardID,
		Term:    r.term, // 携带当前任期，用于接收方验证消息有效性
	})
	// 检查下游连接是否超时
	// 检查下游连接是否超时（初始化时若未收到过 Pong，lastPingAck 为零值，此时不触发超时）
	if r.chainDownstream.LeaderID != 0 &&
		!r.chainDownstream.lastPingAck.IsZero() &&
		r.chainDownstream.lastPingAck.Add(30*time.Second).Before(time.Now()) {
		plog.Warningf("%s downstream chain connection timeout, retrying", r.describe())
		r.propagateChainConnect(r.chainDownstream.ShardID) // 重试下游连接
	}
}

// 新增：处理健康检查Ping消息
// // 处理 Ping 并回复 Pong
func (r *raft) handleLeaderChainPing(m pb.Message) error {
	r.mustBeLeader()
	// 响应Pong消息
	r.send(pb.Message{
		Type:    pb.LeaderChainPong, // 需在raftpb中定义
		From:    r.replicaID,
		To:      m.From,
		ShardID: r.shardID,
		Term:    r.term,
	})
	return nil
}

// 新增 LeaderChainPong 消息处理函数
// 添加 handleLeaderChainPong 函数，更新下游连接的 lastPingAck 时间：
// 新增：处理健康检查 Pong 消息（上游领导者收到下游 Pong 时调用）
func (r *raft) handleLeaderChainPong(m pb.Message) error {
	r.mustBeLeader()
	// 更新下游连接的最后 Pong 时间（用于超时判断）
	if m.ShardID == r.chainDownstream.ShardID && m.From == r.chainDownstream.LeaderID {
		r.chainDownstream.lastPingAck = time.Now() // 更新时间戳，避免误判超时
		plog.Debugf("%s updated downstream chain ping ack time", r.describe())
	}
	return nil
}

// 处理链式连接断开消息（领导者状态）
// 作用：响应上游/下游节点的断开请求，清除对应连接信息，避免无效健康检查或数据同步。
func (r *raft) handleLeaderChainDisconnect(m pb.Message) error {
	r.mustBeLeader() // 仅领导者处理断开请求

	// 1. 日志记录断开事件
	plog.Infof("%s received chain disconnect request from shard %d (leader %d)",
		r.describe(), m.ShardID, m.From)

	// 2. 清除对应连接信息（上游/下游）
	switch {
	case m.ShardID == r.chainUpstream.ShardID && m.From == r.chainUpstream.LeaderID:
		// 匹配上游连接：清除上游信息
		plog.Infof("%s upstream chain connection (shard %d) disconnected",
			r.describe(), m.ShardID)
		r.chainUpstream = struct{ ShardID, LeaderID uint64 }{} // 重置为空
	case m.ShardID == r.chainDownstream.ShardID && m.From == r.chainDownstream.LeaderID:
		// 匹配下游连接：清除下游信息
		plog.Infof("%s downstream chain connection (shard %d) disconnected",
			r.describe(), m.ShardID)
		r.chainDownstream = struct {
			ShardID, LeaderID uint64
			lastPingAck       time.Time
		}{} // 重置为空
	default:
		// 未知连接：记录警告（可能是已过期的断开请求）
		plog.Warningf("%s received disconnect from unknown chain shard %d (leader %d)",
			r.describe(), m.ShardID, m.From)
	}

	// 3. 可选：触发重连逻辑（如需自动恢复连接，可在此处添加定时重试任务）
	// 示例：r.scheduleChainReconnect(m.ShardID)

	return nil
}
