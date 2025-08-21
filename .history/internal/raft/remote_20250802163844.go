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

// 尝试根据远程节点的已复制状态索引 更新 本地跟踪状态
func (r *remote) tryUpdate(index uint64) bool {
	// index是远程节点的已成功复制的日志索引
	if r.next < index+1 { //优化下次发起起点
		r.next = index + 1
	}
	if r.match < index { //说明已复制的日志索引 小于 远程节点的已复制状态索引
		r.waitToRetry()
		r.match = index
		return true
	}
	// r.match >= index  说明已复制的日志索引 大于等于 远程节点的已复制状态索引
	return false
}

func (r *remote) progress(lastIndex uint64) {
	if r.state == remoteReplicate {
		r.next = lastIndex + 1
	} else if r.state == remoteRetry {
		r.retryToWait()
	} else {
		panic("unexpected remote state")
	}
}

func (r *remote) respondedTo() {
	if r.state == remoteRetry {
		r.becomeReplicate()
	} else if r.state == remoteSnapshot {
		if r.match >= r.snapshotIndex {
			r.becomeRetry()
		}
	}
}

func (r *remote) decreaseTo(rejected uint64, last uint64) bool {
	if r.state == remoteReplicate {
		if rejected <= r.match {
			// stale msg
			return false
		}
		r.next = r.match + 1
		return true
	}
	if r.next-1 != rejected {
		// stale
		return false
	}
	r.waitToRetry()
	r.next = max(1, min(rejected, last+1))
	return true
}

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
