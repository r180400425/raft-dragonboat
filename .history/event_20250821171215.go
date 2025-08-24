// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

package dragonboat

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"

	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/raftio"

	pb "github.com/lni/dragonboat/v4/raftpb"
)

// 新增引入 pb "github.com/lni/dragonboat/v4/raftpb"

// WriteHealthMetrics writes all health metrics in Prometheus format to the
// specified writer. This function is typically called by the metrics http
// handler.
func WriteHealthMetrics(w io.Writer) {
	metrics.WritePrometheus(w, false)
}

/*
关键修改说明
结构体扩展：

新增 nextShardID 字段存储链式连接的下一个 Shard 标识
新增 transport 字段用于跨 Shard 消息发送（依赖 raftio.ITransport 接口）
构造函数调整：

新增 nextShardID 和 transport 参数，支持从外部注入链式配置和通信组件
事件处理增强：

在 LeaderUpdated 中触发 establishLeaderChain 方法，实现领导者变更时自动建立连接
通过自定义 LeaderChainConnect 消息类型，向目标 Shard 领导者发送连接请求
跨 Shard 通信：

依赖 transport.SendMessageToShardLeader 方法（需在 raftio.ITransport 中扩展），自动路由消息到目标 Shard 的当前领导者
*/

/*
依赖补充说明
为使上述代码生效，需配套实现：

在 raftpb 中定义 LeaderChainConnect 消息类型
在 raftio.ITransport 中添加 SendMessageToShardLeader 方法，支持按 ShardID 查询领导者并发送消息
通过配置文件或启动参数注入 nextShardID 链式关系
此结构确保每个 Shard 领导者仅需关注与下一个 Shard 的连接，形成松耦合的线性链式结构，支持动态扩展和故障自动恢复。
*/

type raftEventListener struct {
	readIndexDropped    *metrics.Counter
	proposalDropped     *metrics.Counter
	replicationRejected *metrics.Counter
	snapshotRejected    *metrics.Counter
	queue               *leaderInfoQueue
	hasLeader           *metrics.Gauge
	term                *metrics.Gauge
	campaignLaunched    *metrics.Counter
	campaignSkipped     *metrics.Counter
	leaderID            uint64
	termValue           uint64
	replicaID           uint64
	shardID             uint64
	metrics             bool
	// 新增：链式连接所需字段
	nextShardID uint64            // 下一个Shard的ID（需通过配置注入）
	transport   raftio.ITransport // 用于跨Shard消息发送（需通过构造函数传入）
}

var _ server.IRaftEventListener = (*raftEventListener)(nil)

func newRaftEventListener(shardID uint64, replicaID uint64,
	useMetrics bool, queue *leaderInfoQueue,
	// 新增：链式连接配置参数
	nextShardID uint64, transport raftio.ITransport) *raftEventListener {

	el := &raftEventListener{
		shardID:   shardID,
		replicaID: replicaID,
		metrics:   useMetrics,
		queue:     queue,
		// 新增：初始化链式连接字段
		nextShardID: nextShardID,
		transport:   transport,
	}
	if useMetrics {
		label := fmt.Sprintf(`{shardid="%d",replicaid="%d"}`, shardID, replicaID)
		name := fmt.Sprintf(`dragonboat_raftnode_campaign_launched_total%s`, label)
		el.campaignLaunched = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_campaign_skipped_total%s`, label)
		el.campaignSkipped = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_snapshot_rejected_total%s`, label)
		el.snapshotRejected = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_replication_rejected_total%s`, label)
		el.replicationRejected = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_proposal_dropped_total%s`, label)
		el.proposalDropped = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_read_index_dropped_total%s`, label)
		el.readIndexDropped = metrics.GetOrCreateCounter(name)
		name = fmt.Sprintf(`dragonboat_raftnode_has_leader%s`, label)
		el.hasLeader = metrics.GetOrCreateGauge(name, func() float64 {
			if atomic.LoadUint64(&el.leaderID) == raftio.NoLeader {
				return 0.0
			}
			return 1.0
		})
		name = fmt.Sprintf(`dragonboat_raftnode_term%s`, label)
		el.term = metrics.GetOrCreateGauge(name, func() float64 {
			return float64(atomic.LoadUint64(&el.termValue))
		})
	}
	return el
}

func (e *raftEventListener) close() {
}

func (e *raftEventListener) LeaderUpdated(info server.LeaderInfo) {
	atomic.StoreUint64(&e.leaderID, info.LeaderID)
	atomic.StoreUint64(&e.termValue, info.Term)
	if e.queue != nil {
		ui := raftio.LeaderInfo{
			ShardID:   info.ShardID,
			ReplicaID: info.ReplicaID,
			Term:      info.Term,
			LeaderID:  info.LeaderID,
		}
		e.queue.addLeaderInfo(ui)
	}
	// 新增：触发领导者链式连接
	e.establishLeaderChain(info)
}

// 新增：建立与下一个Shard领导者的连接
func (e *raftEventListener) establishLeaderChain(info server.LeaderInfo) {
	// 1. 若未配置下一个Shard，终止链式连接
	if e.nextShardID == 0 {
		return
	}
	// 2. 构造跨Shard连接请求（使用自定义消息类型）
	msg := pb.Message{
		Type:      pb.LeaderChainConnect, // 需在raftpb中定义新消息类型
		From:      info.LeaderID,
		To:        0, // 目标Shard领导者ID将通过查询获取
		ShardID:   e.shardID,
		NextShard: e.nextShardID,
		Term:      info.Term,
	}
	// 3. 通过transport发送消息（需实现获取目标Shard领导者地址的逻辑）
	if e.transport != nil {
		e.transport.SendMessageToShardLeader(e.nextShardID, msg)
	}
}
func (e *raftEventListener) CampaignLaunched(info server.CampaignInfo) {
	if e.metrics {
		e.campaignLaunched.Add(1)
	}
}

func (e *raftEventListener) CampaignSkipped(info server.CampaignInfo) {
	if e.metrics {
		e.campaignSkipped.Add(1)
	}
}

func (e *raftEventListener) SnapshotRejected(info server.SnapshotInfo) {
	if e.metrics {
		e.snapshotRejected.Add(1)
	}
}

func (e *raftEventListener) ReplicationRejected(info server.ReplicationInfo) {
	if e.metrics {
		e.replicationRejected.Add(1)
	}
}

func (e *raftEventListener) ProposalDropped(info server.ProposalInfo) {
	if e.metrics {
		e.proposalDropped.Add(len(info.Entries))
	}
}

func (e *raftEventListener) ReadIndexDropped(info server.ReadIndexInfo) {
	if e.metrics {
		e.readIndexDropped.Add(1)
	}
}

type sysEventListener struct {
	stopc  chan struct{}
	events chan server.SystemEvent
	ul     raftio.ISystemEventListener
}

func newSysEventListener(l raftio.ISystemEventListener,
	stopc chan struct{}) *sysEventListener {
	return &sysEventListener{
		stopc:  stopc,
		events: make(chan server.SystemEvent),
		ul:     l,
	}
}

func (l *sysEventListener) Publish(e server.SystemEvent) {
	if l.ul == nil {
		return
	}
	select {
	case l.events <- e:
	case <-l.stopc:
		return
	}
}

func (l *sysEventListener) handle(e server.SystemEvent) {
	if l.ul == nil {
		return
	}
	switch e.Type {
	case server.NodeHostShuttingDown:
		l.ul.NodeHostShuttingDown()
	case server.NodeReady:
		l.ul.NodeReady(getNodeInfo(e))
	case server.NodeUnloaded:
		l.ul.NodeUnloaded(getNodeInfo(e))
	case server.NodeDeleted:
		l.ul.NodeDeleted(getNodeInfo(e))
	case server.MembershipChanged:
		l.ul.MembershipChanged(getNodeInfo(e))
	case server.ConnectionEstablished:
		l.ul.ConnectionEstablished(getConnectionInfo(e))
	case server.ConnectionFailed:
		l.ul.ConnectionFailed(getConnectionInfo(e))
	case server.SendSnapshotStarted:
		l.ul.SendSnapshotStarted(getSnapshotInfo(e))
	case server.SendSnapshotCompleted:
		l.ul.SendSnapshotCompleted(getSnapshotInfo(e))
	case server.SendSnapshotAborted:
		l.ul.SendSnapshotAborted(getSnapshotInfo(e))
	case server.SnapshotReceived:
		l.ul.SnapshotReceived(getSnapshotInfo(e))
	case server.SnapshotRecovered:
		l.ul.SnapshotRecovered(getSnapshotInfo(e))
	case server.SnapshotCreated:
		l.ul.SnapshotCreated(getSnapshotInfo(e))
	case server.SnapshotCompacted:
		l.ul.SnapshotCompacted(getSnapshotInfo(e))
	case server.LogCompacted:
		l.ul.LogCompacted(getEntryInfo(e))
	case server.LogDBCompacted:
		l.ul.LogDBCompacted(getEntryInfo(e))
	default:
		panic("unknown event type")
	}
}

func getSnapshotInfo(e server.SystemEvent) raftio.SnapshotInfo {
	return raftio.SnapshotInfo{
		ShardID:   e.ShardID,
		ReplicaID: e.ReplicaID,
		From:      e.From,
		Index:     e.Index,
	}
}

func getNodeInfo(e server.SystemEvent) raftio.NodeInfo {
	return raftio.NodeInfo{
		ShardID:   e.ShardID,
		ReplicaID: e.ReplicaID,
	}
}

func getEntryInfo(e server.SystemEvent) raftio.EntryInfo {
	return raftio.EntryInfo{
		ShardID:   e.ShardID,
		ReplicaID: e.ReplicaID,
		Index:     e.Index,
	}
}

func getConnectionInfo(e server.SystemEvent) raftio.ConnectionInfo {
	return raftio.ConnectionInfo{
		Address:            e.Address,
		SnapshotConnection: e.SnapshotConnection,
	}
}

// ------------------------------------------------------------------------------
