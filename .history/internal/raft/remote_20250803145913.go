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
//
//
// remote.go is used for tracking the state of remote raft node. it is derived
// from etcd raft's flow control code.
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

// remote.go 实现远程节点的复制进度跟踪与状态管理，基于 etcd raft 的流控逻辑衍生。
// 核心功能：维护领导者对每个远程节点（跟随者/候选者）的日志复制状态，包括匹配索引（match）、
// 下一个待发送索引（next）、复制状态机及快照同步状态，确保日志复制高效可靠。

package raft

import (
	"fmt" // 用于格式化远程节点状态字符串
)

/* -----------快照确认跟踪结构体----------- */

// 快照同步的确认状态
type snapshotAck struct {
	ctick    uint64 //快照确认的倒计时tick（0时触发检查）
	rejected bool   //是否被拒绝
}

// 倒计时，归零时返回true
func (a *snapshotAck) tick() bool {
	if a.ctick > 0 {
		a.ctick--
		return a.ctick == 0
	}
	return false
}

/* -----------远程节点状态枚举----------- */

// 定义远程节点的复制状态枚举，表征日志复制/快照同步的不同阶段。
type remoteStateType uint64

const (
	remoteRetry     remoteStateType = iota // 重试状态：需重试发送日志（如前次发送失败）
	remoteWait                             // 等待状态：等待远程节点响应（避免频繁重试）
	remoteReplicate                        // 复制状态：正常发送日志条目（高效稳定复制）
	remoteSnapshot                         // 快照状态：正在发送快照（日志差距过大时）

)

// remoteNames 状态枚举对应的字符串名称（用于日志和调试输出）。
var remoteNames = []string{
	"Retry",
	"Wait",
	"Replicate",
	"Snapshot",
}

// String 将远程节点状态转换为人类可读字符串（实现 fmt.Stringer 接口）。
func (r remoteStateType) String() string {
	return remoteNames[uint64(r)]
}

/* -----------远程节点复制状态结构体----------- */

// 结构体 remote 描述了远程节点的状态和行为。
type remote struct {
	// called matchIndex/nextIndex in the etcd raft paper
	match         uint64          // 已成功复制到远程节点的最高日志索引（Raft 论文中的 matchIndex）
	next          uint64          // 下一个待发送给远程节点的日志索引（Raft 论文中的 nextIndex）
	snapshotIndex uint64          // 当前正在同步的快照对应的日志索引（0 表示无快照同步）
	state         remoteStateType // 当前复制状态（Retry/Wait/Replicate/Snapshot）
	active        bool            // 远程节点是否活跃（领导者定期检查 quorum 时使用）
	delayed       snapshotAck     // 快照同步的延迟确认状态（仅在 Snapshot 状态有效）
}

// String 格式化远程节点的复制状态为字符串（用于日志和调试）。
func (r *remote) String() string {
	return fmt.Sprintf("match:%d,next:%d,state:%s,si:%d",
		r.match, r.next, r.state, r.snapshotIndex)
}

/*  ----------快照确认管理方法-----------  */

// 清除快照确认状态 （重置为默认值）
func (r *remote) clearSnapshotAck() {
	r.delayed = snapshotAck{}
}

// 设置快照确认状态的1.确认倒计时tick和2.是否被拒绝（仅在Snapshot 状态下有效）
func (r *remote) setSnapshotAck(tick uint64, rejected bool) {
	if r.state == remoteSnapshot { //Snapshot 状态下有效
		r.delayed.ctick, r.delayed.rejected = tick, rejected //赋值
	} else {
		panic("setting snapshot ack when not in snapshot state")
	}
}

/* -----------快照重置与转换方法----------- */

// 重置快照索引为0，0表示无快照同步
func (r *remote) reset() {
	r.snapshotIndex = 0

	// 这个 reset 方法的主要作用是：

	// 清除快照状态: 将 snapshotIndex 重置为 0，表示不再进行快照同步
	// 状态清理: 当快照同步完成或需要中止时，清理相关的快照索引信息

	// 在 becomeRetry() 方法中：当节点状态变为重试状态时，清除快照索引
	// 在 becomeReplicate() 方法中：当节点进入正常复制状态时，清除快照索引
	// 在 becomeSnapshot() 方法中：在设置新的快照索引之前先重置
}

// 转换为Retry重试状态，重置next索引，清除快照状态
func (r *remote) becomeRetry() {
	// next是下一个要发送的日志索引
	if r.state == remoteSnapshot { //如果处于快照恢复状态，设置为最高的索引+1
		r.next = max(r.match+1, r.snapshotIndex+1)
	} else { //其他状态，设置为已匹配的索引+1
		r.next = r.match + 1
	}
	r.reset()             // 重置快照索引为0，0表示无快照同步
	r.state = remoteRetry //设置状态为重试状态
}

// 从Retry转换为Wait等待状态，避免频繁重试
func (r *remote) retryToWait() {
	if r.state == remoteRetry { //如果当前状态是重试状态
		r.state = remoteWait ///设置状态为等待状态
	}
}

// 从 Wait 状态转换为 Retry 状态（退出等待，准备重试发送日志）。
func (r *remote) waitToRetry() {
	if r.state == remoteWait {
		r.state = remoteRetry
	}
}

// becomeWait 转换为 Wait 状态（先重置为 Retry 状态，再转为 Wait）。
func (r *remote) becomeWait() {
	r.clearSnapshotAck()
	r.becomeRetry()
	r.retryToWait()
}

// 转换为 remoteReplicate 状态（正常日志复制模式），next 索引设为 match+1。
func (r *remote) becomeReplicate() {
	r.next = r.match + 1      // 设置下一个待发送索引为 match+1
	r.reset()                 //设置r.snapshotIn=0表示不再进行快照同步
	r.state = remoteReplicate //设置状态为复制状态
}

// 转换为 remoteSnapshot 状态，开始同步快照
func (r *remote) becomeSnapshot(index uint64) {
	r.reset()                //设置r.snapshotIn=0表示不再进行快照同步
	r.snapshotIndex = index  // 参数 index: 快照对应的日志索引（快照包含该索引及之前的所有日志）
	r.state = remoteSnapshot //设置状态为快照同步状态
}

// clearPendingSnapshot 清除挂起的快照同步（重置快照索引为 0）。
func (r *remote) clearPendingSnapshot() {
	r.snapshotIndex = 0
}

/* -----------复制进度更新方法----------- */

/*
tryUpdate 函数由 领导者节点（Leader） 在以下场景中调用：
	当领导者收到 跟随者节点（Follower） 或 候选者节点（Candidate） 的 AppendEntries 响应（日志复制成功确认）时，用于更新对该远程节点的日志复制进度跟踪。

调用场景细节
	触发条件
		领导者向远程节点发送 AppendEntries RPC（包含日志条目）后，若远程节点成功复制日志，会在响应中返回其已复制的 最高日志索引（即 index 参数）。领导者通过 tryUpdate(index) 处理该响应，更新对该远程节点的 match（已确认复制的最高索引）和 next（下一条待发送索引）。

	核心目的
		动态调整 next 索引，避免重复发送已复制的日志（next = index + 1）。
		更新 match 索引，作为领导者计算 集群已提交日志索引（commitIndex）的依据（需多数节点的 match 达到同一索引）。

调用方与被调用方角色
	调用方：只能是 领导者节点（只有领导者负责日志复制和进度跟踪）。
	被调用方：领导者维护的 remote 结构体实例（对应每个远程节点，包括跟随者和候选者）。

典型调用链路
	领导者向远程节点发送 AppendEntries RPC（包含日志条目）。
	远程节点成功复制日志后，返回响应，包含其已复制的最高日志索引 index。
	领导者接收响应，调用 remote.tryUpdate(index) 更新该节点的复制进度。
	tryUpdate 调整 next 和 match，并根据需要转换远程节点的复制状态（如从 Wait 转为 Retry，触发后续日志发送）。

*/

// “tryUpdate 是领导者节点用于跟踪远程节点（跟随者/候选者）日志复制进度的核心更新方法，用于根据远程节点反馈的‘已成功复制的最高日志索引’（index），动态调整领导者对该节点的复制状态（match 和 next 索引），确保日志复制高效推进。”

func (r *remote) tryUpdate(index uint64) bool {
	// 参数 index：远程节点已成功复制的最高日志索引（由远程节点通过 AppendEntries 响应返回）。

	// 更新r. next（如果有需要）
	// r. next是领导者计划发送给远程节点r的下一条日志索引
	// 若远程节点已复制到index，下一条应该从index+1开始
	// 该if是为了避免重复发送已复制的日志
	if r.next < index+1 {
		r.next = index + 1
	}

	// 2.更新已匹配索引：仅当远程节点的复制进度index超过match时更新
	// r.match是远程节点已确认复制的最高日志索引（记录）
	// index是远程节点已复制的最高日志索引（实际）
	// 如果记录的match < 实际的index，则更新match
	if r.match < index {
		r.waitToRetry() // 若远程节点此前处于 Wait 状态（因前次发送未响应而等待），此时因远程节点已反馈新进度，需转为 Retry 状态以主动发送后续日志（避免持续等待）。
		r.match = index // 更新 match 为远程节点已确认的最高索引
		return true     //表示更新match
	}
	// 若 index 小于等于当前 match（远程节点未提供新进度），或 next 已处于正确位置，则无需更新状态，返回 false。
	return false //无需更新
}

// 更新复制进度（领导者在发送日志后调用，根据远程节点的最新日志索引lastIndex调整r. next）
func (r *remote) progress(lastIndex uint64) {
	// lastIndex 是 远程节点的最新日志索引
	// 注意状态：仅在复制和重试状态
	if r.state == remoteReplicate {
		r.next = lastIndex + 1 // 将下一个待发送索引设置为 lastIndex + 1
	} else if r.state == remoteRetry {
		r.retryToWait() // 若当前是重试状态，将状态切换为等待状态

		/*
			1. 避免过度频繁的重试
				当节点处于 remoteRetry 状态时，领导者会积极地向该节点发送日志。如果刚刚发送了一次日志（调用 progress 方法），应该暂时进入等待状态，避免过于频繁的重试：
			2. 实现发送节奏控制
				这种状态转换实现了简单的流量控制机制：
					remoteRetry 状态：允许发送日志
					remoteWait 状态：暂停发送，等待响应
					通过在发送后立即切换到 Wait 状态，可以避免网络拥塞和资源浪费。
			3. 符合 Raft 算法的流控设计
				这是从 etcd raft 继承的流控机制：
					当领导者向跟随者发送日志后，暂时进入等待状态
					等待跟随者的响应后再决定下一步动作
					如果收到响应，可能切换回 Retry 或 Replicate 状态
					如果超时，可能通过其他机制重新激活
		*/

	} else {
		panic("unexpected remote state")
	}
}

/*

示例

1. 领导者发现跟随者日志落后太多
   → 调用 becomeSnapshot() 进入 Snapshot 状态

2. 发送快照数据给跟随者
   → 跟随者接收并应用快照

3. 跟随者确认快照应用完成
   → match >= snapshotIndex 成立
   → 调用 respondedTo() 转换为 Retry 状态

4. 发送快照后的第一条日志
   → 跟随者成功响应
   → 调用 respondedTo() 转换为 Replicate 状态

5. 开始高效流水线复制


选择 remoteRetry 状态的原因：

	稳妥的过渡：

	快照同步是一个重大操作，完成后需要谨慎处理
	remoteRetry 状态是比 remoteReplicate 更保守的状态
	允许系统逐步验证连接的稳定性和数据的一致性
	准备后续日志复制：

	快照同步完成后，通常需要发送一些后续日志来保持一致性
	remoteRetry 状态适合这种场景，可以逐步发送日志并等待确认
	符合状态机设计原则：

	remoteSnapshot → remoteRetry → remoteReplicate
	这是一个渐进的转换过程，每一步都有明确的触发条件

*/

// 处理远程节点的响应事件（领导者收到远程节点的消息后调用）
func (r *remote) respondedTo() {

	if r.state == remoteRetry { //如果节点r在重试状态
		r.becomeReplicate() //进入正常的复制状态

		// 远程节点成功响应表明网络连接正常，可以从保守的重试模式切换到高效的流水线复制模式

	} else if r.state == remoteSnapshot { //如果节点r在快照同步状态
		// r.match：远程节点已确认复制的最高日志索引
		// r.snapshotIndex：正在同步的快照对应的日志索引
		if r.match >= r.snapshotIndex { //说明快照同步已经完成
			r.becomeRetry() //进入重试状态，准备发送后续日志
		}
	}
}

// 处理远程节点拒绝日志请求的情况，调整next，重试
//   - rejected: 被拒绝的日志索引（远程节点未接受该索引的日志）
//   - last: 远程节点的最新已知日志索引
//
// 返回值：true 若 next 索引被成功调整；false 若请求过时（rejected 已小于等于 match）。
func (r *remote) decreaseTo(rejected uint64, last uint64) bool {

	if r.state == remoteReplicate {
		if rejected <= r.match {
			// stale msg 过时消息
			return false
		}
		//rejected > r.match 消息未过时
		r.next = r.match + 1 //更新下一个日志索引
		return true
	}
	if r.next-1 != rejected { //检查rejected是否是最新请求
		// stale 过时
		return false
	}

	r.waitToRetry() //若此时等待，则进入重试

	// 下一个待接收的日志索引 next
	// 可能是被拒绝的日志索引rejected
	// 可能是当前最新日志索引last+1
	//为了避免中间的日志被跳过，取min
	// 可能是1（默认状态）

	r.next = max(1, min(rejected, last+1))
	return true
}

//-------------------状态查询与活性管理方法-------------------

// 判断远程节点的复制是否处于暂停状态（Wait 或 Snapshot 状态下暂停主动复制）。
func (r *remote) isPaused() bool {
	switch r.state {
	case remoteRetry:
		return false
	case remoteWait:
		return true
	case remoteReplicate:
		return false
	case remoteSnapshot:
		return true
	default:
		panic("unexpected remote state")
	}
}

func (r *remote) isActive() bool {
	return r.active
}

func (r *remote) setActive() {
	r.active = true
}

func (r *remote) setNotActive() {
	r.active = false
}
