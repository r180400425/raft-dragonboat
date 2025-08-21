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
//
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
// Peer.go is the interface used by the upper layer to access functionalities
// provided by the raft protocol. It translates all incoming requests to raftpb
// messages and pass them to the raft protocol implementation to be handled.
// Such a state machine style design together with the iterative style interface
// here is derived from etcd.
// Compared to etcd raft, we strictly model all inputs to the raft protocol as
// messages including those used to advance the raft state.
//
// Peer.go 是上层模块与底层 Raft 协议交互的接口层，
// 负责将所有输入请求转换为 raftpb 消息并传递给 Raft 协议实现处理。
// 这种状态机风格的设计及迭代式接口源自 etcd，
// 与 etcd raft 相比，本实现严格将所有 Raft 协议输入建模为消息（包括推进 Raft 状态的操作）。

package raft // Raft 协议核心实现包

import (
	"sort" // 用于地址排序

	"github.com/lni/dragonboat/v4/config"          // 配置定义
	"github.com/lni/dragonboat/v4/internal/server" // 服务器内部接口
	pb "github.com/lni/dragonboat/v4/raftpb"       // Raft 协议消息定义
)

// PeerAddress 表示 Raft 分片（shard）中一个节点的基础信息
// PeerAddress is the basic info for a peer in the Raft shard.
type PeerAddress struct {
	Address   string // 节点网络地址（如 IP:端口）
	ReplicaID uint64 // 节点副本 ID（唯一标识）
}

// Peer 是与底层 Raft 协议实现交互的接口结构体
// Peer is the interface struct for interacting with the underlying Raft protocol implementation.
type Peer struct {
	raft      *raft    // 指向底层 Raft 协议实例
	prevState pb.State // 上一次的 Raft 状态（用于检测状态变更）
}

// Launch 启动或重启一个 Raft 节点
// 参数说明：
//   - config: Raft 节点配置（包含分片 ID、副本 ID 等）
//   - logdb: 日志数据库接口（存储 Raft 日志）
//   - events: Raft 事件监听器（用于通知上层模块事件）
//   - addresses: 节点地址列表（初始成员或已知成员）
//   - initial: 是否为初始集群（首次启动）
//   - newNode: 是否为新节点（非重启）
//
// Launch starts or restarts a Raft node.
func Launch(config config.Config,
	logdb ILogDB, events server.IRaftEventListener,
	addresses []PeerAddress, initial bool, newNode bool) Peer {

	// 检查启动请求合法性（如副本 ID 不为 0、初始节点地址非空等）
	checkLaunchRequest(config, addresses, initial, newNode)
	// 打印启动日志，包含分片 ID、副本 ID、初始/新节点标志
	plog.Infof("%s created, initial: %t, new: %t",
		dn(config.ShardID, config.ReplicaID), initial, newNode)
	// 创建 Peer 实例，初始化底层 raft 对象
	p := Peer{raft: newRaft(config, logdb)}
	// 设置 raft 的事件监听器
	p.raft.events = events
	// 记录初始 Raft 状态（用于后续状态变更检测）
	p.prevState = p.raft.raftState()
	// 若为初始集群且是新节点，初始化为跟随者并引导集群
	if initial && newNode {
		p.raft.becomeFollower(1, NoLeader) // 成为任期 1 的跟随者，无领导者
		bootstrap(p.raft, addresses)       // 引导集群（添加初始成员）
	}
	return p
}

// Tick 推进逻辑时钟（正常模式）
// 发送 LocalTick 消息，触发 Raft 节点的定时任务（如选举超时检测）
// Tick moves the logical clock forward by one tick.
func (p *Peer) Tick() error {
	return p.raft.Handle(pb.Message{
		Type:   pb.LocalTick, // 本地时钟消息类型
		Reject: false,        // 非拒绝模式（正常处理）
	})
}

// QuiescedTick 推进逻辑时钟（静默模式）
// 发送 LocalTick 消息，触发静默模式下的定时任务（减少不必要的心跳）
// QuiescedTick moves the logical clock forward by one tick in quiesced mode.
func (p *Peer) QuiescedTick() error {
	return p.raft.Handle(pb.Message{
		Type:   pb.LocalTick, // 本地时钟消息类型
		Reject: true,         // 拒绝模式（静默处理）
	})
}

// QueryRaftLog 查询 Raft 日志
// 参数说明：
//   - firstIndex: 起始日志索引
//   - lastIndex: 结束日志索引
//   - maxSize: 最大返回日志大小（字节）
func (p *Peer) QueryRaftLog(firstIndex uint64,
	lastIndex uint64, maxSize uint64) error {
	return p.raft.Handle(pb.Message{
		Type: pb.LogQuery, //日志查询消息类型
		From: firstIndex,  //起始索引
		To:   lastIndex,   //结束索引
		Hint: maxSize,     //最大大小提示
	})
}

// 该请求将leadership转移给指定的目标节点
// RequestLeaderTransfer makes a request to transfer the leadership to the specified target node.
func (p *Peer) RequestLeaderTransfer(target uint64) error { //参数target：目标节点副本ID

	return p.raft.Handle(pb.Message{
		Type: pb.LeaderTransfer, //leadership转移消息类型
		To:   p.raft.replicaID,  //发送者（当前节点）副本ID
		Hint: target,            //目标节点副本ID

	})
}

// 批量提交条目（通过单个MTPropose消息）
// ProposeEntries proposes specified entries in a batched mode using a single MTPropose message.
func (p *Peer) ProposeEntries(ents []pb.Entry) error { //参数ents：待提交的条目列表
	return p.raft.Handle(pb.Message{
		Type:    pb.Propose,       // 提案消息类型
		From:    p.raft.replicaID, // 发送者副本 ID
		Entries: ents,             // 待提交的日志条目
	})
}

// ProposeConfigChange 提交一个 Raft 集群成员配置变更提案
// 参数说明：
//   - cc: 配置变更信息（如添加/移除节点、类型等）
//   - key: 提案唯一标识（用于去重）
//
// ProposeConfigChange proposes a raft membership change.
func (p *Peer) ProposeConfigChange(cc pb.ConfigChange, key uint64) error {
	// 将配置变更信息序列化为字节流
	data := pb.MustMarshal(&cc)
	return p.raft.Handle(pb.Message{
		Type:    pb.Propose,                                                    // 提案消息类型
		Entries: []pb.Entry{{Type: pb.ConfigChangeEntry, Cmd: data, Key: key}}, // 构造配置变更条目
		// 包括（条目类型，序列化的配置变更数据，提案唯一标识）

	})
}

// ApplyConfigChange 将配置变更应用到本地 Raft 节点
// ApplyConfigChange applies a raft membership change to the local raft node.
func (p *Peer) ApplyConfigChange(cc pb.ConfigChange) error { // 参数 cc: 配置变更信息（由领导者提议并提交）
	// 若 ReplicaID 为 NoLeader，清除待处理的配置变更（异常情况）
	if cc.ReplicaID == NoLeader {
		p.raft.clearPendingConfigChange()
		return nil
	}
	// 发送 ConfigChangeEvent 消息，通知 raft 应用配置变更
	return p.raft.Handle(pb.Message{
		Type:     pb.ConfigChangeEvent, // 配置变更事件类型
		Reject:   false,                // 不拒绝（应用变更）
		Hint:     cc.ReplicaID,         // 目标节点副本 ID
		HintHigh: uint64(cc.Type),      // 配置变更类型（如添加/移除节点）
	})
}

// RejectConfigChange 拒绝当前待处理的 Raft 配置变更
// RejectConfigChange rejects the currently pending raft membership change.
func (p *Peer) RejectConfigChange() error {
	// 发送 ConfigChangeEvent 消息，通知 raft 拒绝配置变更
	return p.raft.Handle(pb.Message{
		Type:   pb.ConfigChangeEvent, // 配置变更事件类型
		Reject: true,                 // 拒绝变更
	})
}

// RestoreRemotes 从快照中恢复远程节点信息（如节点地址、状态等）
// RestoreRemotes applies the remotes info obtained from the specified snapshot.
func (p *Peer) RestoreRemotes(ss pb.Snapshot) error { // 参数 ss: 快照数据（包含远程节点信息）
	// 发送 SnapshotReceived 消息，通知 raft 处理快照恢复
	return p.raft.Handle(pb.Message{
		Type:     pb.SnapshotReceived, // 快照接收消息类型
		Snapshot: ss,                  // 快照数据
	})
}

// ReportUnreachableNode 标记指定节点为不可达
// ReportUnreachableNode marks the specified node as not reachable.
func (p *Peer) ReportUnreachableNode(replicaID uint64) error { // 参数 replicaID: 不可达节点的副本 ID
	// 发送 Unreachable 消息，通知 raft 节点不可达
	return p.raft.Handle(pb.Message{
		Type: pb.Unreachable, // 节点不可达消息类型
		From: replicaID,      // 不可达节点的副本 ID
	})
}

// ReportSnapshotStatus 向本地 Raft 节点报告快照发送/接收状态
// 参数说明：
//   - replicaID: 目标节点副本 ID
//   - reject: 快照是否被拒绝（true 表示拒绝，false 表示成功）
//
// ReportSnapshotStatus reports the status of the snapshot to the local raft node.
func (p *Peer) ReportSnapshotStatus(replicaID uint64, reject bool) error {
	// 发送 SnapshotStatus 消息，通知 raft 快照状态
	return p.raft.Handle(pb.Message{
		Type:   pb.SnapshotStatus, // 快照状态消息类型
		From:   replicaID,         // 目标节点副本 ID
		Reject: reject,            // 快照是否被拒绝
	})
}

// Handle 处理外部（非本地）消息（如来自其他节点的投票请求、日志复制等）
// Handle processes the given message.
func (p *Peer) Handle(m pb.Message) error { // 参数 m: 待处理的 Raft 消息
	// 本地消息（如 LocalTick）不应通过此接口处理，直接 panic
	if IsLocalMessageType(m.Type) {
		panic("local message sent to Step")
	}
	// 检查发送者是否为已知节点：远程节点、非投票节点或见证节点
	_, rok := p.raft.remotes[m.From]    // 远程节点（投票成员）
	_, ook := p.raft.nonVotings[m.From] // 非投票节点（仅同步日志）
	_, wok := p.raft.witnesses[m.From]  // 见证节点（仅参与法定人数）
	// 若为已知节点或非响应类消息，交给 raft 处理
	if rok || ook || wok || !isResponseMessageType(m.Type) {
		return p.raft.Handle(m)
	}
	// 未知节点的响应消息，忽略（返回 nil）
	return nil
}

// GetUpdate 获取 Peer 的当前状态更新（包含待处理日志、消息、状态变更等）
// 参数说明：
//   - moreToApply: 是否有更多待应用的日志条目
//   - lastApplied: 最后一个已应用的日志索引（来自上层状态机）
//
// 返回值：pb.Update 结构体（包含所有待处理的更新信息）
// GetUpdate returns the current state of the Peer.
func (p *Peer) GetUpdate(moreToApply bool,
	lastApplied uint64) (pb.Update, error) {

	// 构建基础更新信息
	ud, err := p.getUpdate(moreToApply, lastApplied)
	if err != nil {
		return pb.Update{}, err // 错误处理
	}
	// 验证更新信息合法性（如已提交条目是否已保存）
	validateUpdate(ud)
	// 设置 FastApply 标志（优化：是否可跳过持久化直接应用）
	ud = setFastApply(ud)
	// 构建 UpdateCommit 字段（记录需要持久化的状态）
	ud.UpdateCommit = getUpdateCommit(ud)
	return ud, nil
}

// setFastApply 设置 Update 的 FastApply 标志（是否支持快速应用）
// 逻辑：若存在快照或已应用条目未完全持久化，则不可快速应用
func setFastApply(ud pb.Update) pb.Update {
	ud.FastApply = true // 默认支持快速应用
	// 若存在快照，需先持久化快照，不可快速应用
	if !pb.IsEmptySnapshot(ud.Snapshot) {
		ud.FastApply = false
	}
	// 若支持快速应用，进一步检查已应用条目是否在待保存范围内
	if ud.FastApply {
		if len(ud.CommittedEntries) > 0 && len(ud.EntriesToSave) > 0 {

			lastApplyIndex := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
			lastSaveIndex := ud.EntriesToSave[len(ud.EntriesToSave)-1].Index
			firstSaveIndex := ud.EntriesToSave[0].Index

			// 若已应用条目在待保存范围内（未完全持久化），不可快速应用
			if lastApplyIndex >= firstSaveIndex && lastApplyIndex <= lastSaveIndex {
				ud.FastApply = false
			}
		}
	}
	return ud
}

// validateUpdate 验证 Update 信息的合法性（防止应用未持久化或未提交的条目）
func validateUpdate(ud pb.Update) {
	// 检查已提交条目是否超过提交索引（防止应用未提交条目）
	if ud.Commit > 0 && len(ud.CommittedEntries) > 0 {
		lastIndex := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index

		if lastIndex > ud.Commit {
			plog.Panicf("trying to apply not committed entry: %d, %d",
				ud.Commit, lastIndex)
		}
	}
	// 检查已应用条目是否超过已保存条目（防止应用未持久化条目）
	if len(ud.CommittedEntries) > 0 && len(ud.EntriesToSave) > 0 {
		lastApply := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
		lastSave := ud.EntriesToSave[len(ud.EntriesToSave)-1].Index

		if lastApply > lastSave {
			plog.Panicf("trying to apply not saved entry: %d, %d",
				lastApply, lastSave)
		}
	}
}

// RateLimited 返回 Raft 节点是否被限流（如网络带宽限制）
// RateLimited returns a boolean flag indicating whether the Raft node is rate limited.
func (p *Peer) RateLimited() bool {
	return p.raft.rl.RateLimited() // 委托给 raft 的限流控制器
}

// HasUpdate 判断是否有更新需要处理（如待保存日志、消息、状态变更等）
// 参数 moreToApply: 是否有更多待应用的日志条目
// HasUpdate returns a boolean value indicating whether there is any Update
// ready to be processed.
func (p *Peer) HasUpdate(moreToApply bool) bool {
	r := p.raft
	// 以下任一条件满足则有更新：
	// 1. 有日志条目待保存到日志数据库
	if len(r.log.entriesToSave()) > 0 {
		return true
	}
	// 2. 有日志查询结果待返回
	if r.logQueryResult != nil {
		return true
	}
	// 3. 有领导者状态更新（如任期、领导者 ID 变更）
	if r.leaderUpdate != nil {
		return true
	}
	// 4. 有消息待发送（如投票请求、日志复制请求）
	if len(r.msgs) > 0 {
		return true
	}
	// 5. 有待应用的日志条目且需要继续应用
	if moreToApply && r.log.hasEntriesToApply() {
		return true
	}
	// 6. Raft 状态发生变更（与 prevState 对比）
	if pst := r.raftState(); !pb.IsEmptyState(pst) &&
		!pb.IsStateEqual(pst, p.prevState) {
		return true
	}
	// 7. 存在非空快照（待持久化或发送）
	if r.log.inmem.snapshot != nil &&
		!pb.IsEmptySnapshot(*r.log.inmem.snapshot) {
		return true
	}
	// 8. 有读操作准备就绪（ReadIndex 结果）
	if len(r.readyToRead) != 0 {
		return true
	}
	// 9. 有丢弃的日志条目（如超过日志保留期限）
	if len(r.droppedEntries) > 0 {
		return true
	}
	// 10. 有丢弃的读索引请求（如超时）
	if len(r.droppedReadIndexes) > 0 {
		return true
	}
	// 无更新
	return false
}

// Commit 提交 Update 状态（标记为已处理，清理资源）
// 参数 ud: 已处理的 Update 结构体
// Commit commits the Update state to mark it as processed.
func (p *Peer) Commit(ud pb.Update) {
	// 清理 raft 中的临时数据：
	p.raft.msgs = nil               // 清空待发送消息队列
	p.raft.logQueryResult = nil     // 清空日志查询结果
	p.raft.leaderUpdate = nil       // 清空领导者更新
	p.raft.droppedEntries = nil     // 清空丢弃的日志条目
	p.raft.droppedReadIndexes = nil // 清空丢弃的读索引
	// 更新 prevState（若状态不为空）
	if !pb.IsEmptyState(ud.State) {
		p.prevState = ud.State
	}
	// 若有读操作就绪，清除 readyToRead
	if ud.UpdateCommit.ReadyToRead > 0 {
		p.raft.clearReadyToRead()
	}
	// 通知日志模块提交更新（持久化状态）
	p.entryLog().commitUpdate(ud.UpdateCommit)
}

// ReadIndex 启动 ReadIndex 操作（实现线性一致性读，Raft 论文 6.4 节）
// 参数 ctx: 系统上下文（包含 Low 和 High 字段，用于标识请求）
// ReadIndex starts a ReadIndex operation. The ReadIndex protocol is defined in the section 6.4 of the Raft thesis.
func (p *Peer) ReadIndex(ctx pb.SystemCtx) error {
	// 发送 ReadIndex 消息，触发读索引协议
	return p.raft.Handle(pb.Message{
		Type:     pb.ReadIndex, // 读索引消息类型
		Hint:     ctx.Low,      // 上下文 Low 值
		HintHigh: ctx.High,     // 上下文 High 值
	})
}

// NotifyRaftLastApplied 通知 Raft 最后应用的日志索引（来自上层状态机）
// 参数 lastApplied: 上层状态机已应用的最后一个日志索引
// NotifyRaftLastApplied passes on the lastApplied index confirmed by the RSM to
// the raft state machine.
func (p *Peer) NotifyRaftLastApplied(lastApplied uint64) {
	p.raft.setApplied(lastApplied) // 设置 raft 的 applied 字段
}

// HasEntryToApply 判断是否有更多待应用的日志条目
// HasEntryToApply returns a boolean flag indicating whether there are more
// entries ready to be applied.
func (p *Peer) HasEntryToApply() bool {
	return p.entryLog().hasEntriesToApply() // 委托给日志模块
}

// entryLog 返回 raft 的日志模块实例（辅助函数）
func (p *Peer) entryLog() *entryLog {
	return p.raft.log
}

// getUpdate 构建基础的 Update 结构体（包含待处理的日志、消息、状态等）
// 参数说明：
//   - moreToApply: 是否有更多待应用的日志条目
//   - lastApplied: 最后一个已应用的日志索引
func (p *Peer) getUpdate(moreToApply bool,
	lastApplied uint64) (pb.Update, error) {
	// 初始化 Update 结构体，填充基础信息
	ud := pb.Update{
		ShardID:       p.raft.shardID,               // 分片 ID
		ReplicaID:     p.raft.replicaID,             // 副本 ID
		EntriesToSave: p.entryLog().entriesToSave(), // 待保存到日志数据库的条目
		Messages:      p.raft.msgs,                  // 待发送的消息列表
		LastApplied:   lastApplied,                  // 最后已应用索引（来自上层）
		FastApply:     true,                         // 默认支持快速应用
	}
	// 填充日志查询结果（若存在）
	if p.raft.logQueryResult != nil {
		ud.LogQueryResult = *p.raft.logQueryResult
	}
	// 填充领导者更新（若存在）
	if p.raft.leaderUpdate != nil {
		ud.LeaderUpdate = *p.raft.leaderUpdate
	}
	// 为所有消息设置分片 ID（确保路由正确）
	for idx := range ud.Messages {
		ud.Messages[idx].ShardID = p.raft.shardID
	}
	// 若需要继续应用，填充待应用的已提交条目
	if moreToApply {
		toApply, err := p.entryLog().entriesToApply()
		if err != nil {
			return pb.Update{}, err // 错误处理
		}
		ud.CommittedEntries = toApply
	}
	// 若有待应用条目，判断是否还有更多条目（用于分页处理）
	if len(ud.CommittedEntries) > 0 {
		lastIndex := ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
		ud.MoreCommittedEntries = p.entryLog().hasMoreEntriesToApply(lastIndex)
	}
	// 若 Raft 状态变更，填充状态信息
	if pst := p.raft.raftState(); !pb.IsStateEqual(pst, p.prevState) {
		ud.State = pst
	}
	// 若存在快照，填充快照信息
	if p.entryLog().inmem.snapshot != nil {
		ud.Snapshot = *p.entryLog().inmem.snapshot
	}
	// 填充读操作就绪列表
	if len(p.raft.readyToRead) > 0 {
		ud.ReadyToReads = p.raft.readyToRead
	}
	// 填充丢弃的日志条目
	if len(p.raft.droppedEntries) > 0 {
		ud.DroppedEntries = p.raft.droppedEntries
	}
	// 填充丢弃的读索引请求
	if len(p.raft.droppedReadIndexes) > 0 {
		ud.DroppedReadIndexes = p.raft.droppedReadIndexes
	}
	return ud, nil
}

// checkLaunchRequest 检查启动请求的合法性（防止无效配置）
func checkLaunchRequest(config config.Config,
	addresses []PeerAddress, initial bool, newNode bool) {
	// 副本 ID 不能为 0（无效标识）
	if config.ReplicaID == 0 {
		panic("config.ReplicaID must not be zero")
	}
	// 初始集群且新节点时，地址列表不能为空（需指定初始成员）
	if initial && newNode && len(addresses) == 0 {
		panic("addresses must be specified")
	}
	// 检查地址列表是否有重复（确保节点地址唯一）
	uniqueAddressList := make(map[string]struct{})
	for _, addr := range addresses {
		uniqueAddressList[addr.Address] = struct{}{}
	}
	if len(uniqueAddressList) != len(addresses) {
		plog.Panicf("duplicated address found %v", addresses)
	}
	// 见证节点（witness）不能作为初始成员（不参与日志复制）
	if initial && config.IsWitness {
		plog.Panicf("witness can not be used as initial member")
	}
	// 非投票节点（non-voting）不能作为初始成员（不参与投票）
	if initial && config.IsNonVoting {
		plog.Panicf("non-voting can not be used as initial member")
	}
}

// bootstrap 引导 Raft 集群（初始化初始成员配置）
func bootstrap(r *raft, addresses []PeerAddress) { // 参数 r: raft 实例，addresses: 初始成员地址列表
	// 按副本 ID 排序地址列表（确保一致性）
	sort.Slice(addresses, func(i, j int) bool {
		return addresses[i].ReplicaID < addresses[j].ReplicaID
	})
	// 为每个初始成员创建配置变更条目（AddNode 类型）
	ents := make([]pb.Entry, len(addresses))
	for i, peer := range addresses {
		plog.Infof("%s added bootstrap ConfigChangeAddNode, %d, %s",
			r.describe(), peer.ReplicaID, peer.Address)
		// 构造配置变更：添加节点
		cc := pb.ConfigChange{
			Type:       pb.AddNode,     // 变更类型：添加节点
			ReplicaID:  peer.ReplicaID, // 节点副本 ID
			Initialize: true,           // 标记为初始化（首次启动）
			Address:    peer.Address,   // 节点地址
		}
		// 序列化配置变更并创建日志条目（任期 1，索引 i+1）
		ents[i] = pb.Entry{
			Type:  pb.ConfigChangeEntry, // 条目类型：配置变更
			Term:  1,                    // 任期 1（初始任期）
			Index: uint64(i + 1),        // 索引从 1 开始
			Cmd:   pb.MustMarshal(&cc),  // 序列化的配置变更数据
		}
	}
	// 将配置变更条目追加到日志
	r.log.append(ents)
	// 设置提交索引为最后一个配置变更条目的索引（初始集群直接提交）
	r.log.committed = uint64(len(ents))
	// 将初始成员添加到 raft 的远程节点列表（标记为投票成员）
	for _, peer := range addresses {
		r.addNode(peer.ReplicaID)
	}
}

// getUpdateCommit 构建 UpdateCommit 结构体（记录需要持久化的状态）
func getUpdateCommit(ud pb.Update) pb.UpdateCommit {
	uc := pb.UpdateCommit{
		ReadyToRead: uint64(len(ud.ReadyToReads)), // 读操作就绪数量
		LastApplied: ud.LastApplied,               // 最后已应用索引
	}
	// 若有待应用条目，记录最后一个条目的索引（已处理到该索引）
	if len(ud.CommittedEntries) > 0 {
		uc.Processed = ud.CommittedEntries[len(ud.CommittedEntries)-1].Index
	}
	// 若有待保存条目，记录最后一个条目的索引和任期（需持久化到日志）
	if len(ud.EntriesToSave) > 0 {
		lastEntry := ud.EntriesToSave[len(ud.EntriesToSave)-1]
		uc.StableLogTo, uc.StableLogTerm = lastEntry.Index, lastEntry.Term
	}
	// 若存在快照，记录快照索引（需持久化快照）
	if !pb.IsEmptySnapshot(ud.Snapshot) {
		uc.StableSnapshotTo = ud.Snapshot.Index
		// Processed 取已应用条目索引和快照索引的最大值
		uc.Processed = max(uc.Processed, uc.StableSnapshotTo)
	}
	return uc
}
