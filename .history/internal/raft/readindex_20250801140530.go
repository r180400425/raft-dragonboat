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

package raft

// 引入 Raft 协议相关的 protobuf 定义（如 SystemCtx、Message 等）
import (
	"github.com/lni/dragonboat/v4/raftpb"
)

// 跟踪单个ReadIndex请求的确认状态
type readStatus struct {
	confirmed map[uint64]struct{} //已确认请求的节点ID集合（空结构体占位，仅用于存在性检查）
	ctx       raftpb.SystemCtx    // 请求的上下文（唯一标识）
	index     uint64              //对应的已提交日志索引（线性一致性读的基准索引）
	from      uint64              //发起该ReadIndex请求的节点ID
}

// readIndex is the struct that implements the ReadIndex protocol described in
// section 6.4 (with the idea in section 6.4.1 excluded) of Diego Ongaro's PhD
// thesis.

// readIndex 是ReadIndex协议的状态管理器，负责跟踪所有待处理的线性一致性读请求
// 实现Diego Ongaro 的PhD论文中的6.4节所描述的协议（不包含6.4.1节中的优化）
// 确保线性一致性读：leader通过向多数派节点确认自己仍是leader，并以最新已提交日志索引作为读操作的基准，避免读取过期数据。
type readIndex struct {
	pending map[raftpb.SystemCtx]*readStatus //跟踪所有待处理的ReadIndex请求（键为请求上下文，值为readStatus指针）
	queue   []raftpb.SystemCtx               // 待处理请求的上下文队列（先进先出），最新请求追加到队尾
}

// 构造函数：创建并初始化ReadIndex实例
func newReadIndex() *readIndex {
	return &readIndex{
		pending: make(map[raftpb.SystemCtx]*readStatus), //请求状态跟踪映射
		queue:   make([]raftpb.SystemCtx, 0),            //请求队列（预分配空切片）
	}
}

// addRequest 添加一个新的 ReadIndex 请求到待处理队列。
// 参数：
//   - index: 当前 Raft 分片的已提交日志索引（作为读操作的基准索引）
//   - ctx: 请求的系统上下文（唯一标识）
//   - from: 发起请求的节点 ID
func (r *readIndex) addRequest(index uint64,
	ctx raftpb.SystemCtx, from uint64) {
	// 1. 若请求已存在（上下文ctx已在 pending 中），直接返回（避免重复处理）
	if _, ok := r.pending[ctx]; ok {
		return
	}

	// 2. 检查已提交索引是否单调递增（已提交索引是单调递增的，若出现回退则触发 panic）
	if len(r.queue) > 0 {
		// 获取队列中最新请求的状态（peepCtx 返回队尾元素，即最新请求）
		p, ok := r.pending[r.peepCtx()]
		if !ok {
			panic("inconsistent pending and queue: queue has elements but pending does not")
		}
		// 已提交索引不能小于已有请求的索引（确保线性一致性读的基准索引不回退）
		if index < p.index {
			plog.Panicf("index moved backward in readIndex, current index %d, previous index %d",
				index, p.index)
		}
	}

	// 3. 将新请求添加到队列和 pending 映射中
	r.queue = append(r.queue, ctx) // 追加到队尾（FIFO 顺序）
	r.pending[ctx] = &readStatus{  // 初始化请求状态
		index:     index,                     // 记录当前已提交索引
		from:      from,                      // 记录请求发起节点
		ctx:       ctx,                       // 关联请求上下文
		confirmed: make(map[uint64]struct{}), // 初始化确认节点集合
	}
}

func (r *readIndex) hasPendingRequest() bool {
	return len(r.queue) > 0
}

func (r *readIndex) peepCtx() raftpb.SystemCtx {
	return r.queue[len(r.queue)-1]
}

func (r *readIndex) confirm(ctx raftpb.SystemCtx,
	from uint64, quorum int) []*readStatus {
	p, ok := r.pending[ctx]
	if !ok {
		return nil
	}
	p.confirmed[from] = struct{}{}
	if len(p.confirmed)+1 < quorum {
		return nil
	}
	done := 0
	cs := []*readStatus{}
	for _, pctx := range r.queue {
		done++
		s, ok := r.pending[pctx]
		if !ok {
			panic("inconsistent pending and queue content")
		}
		cs = append(cs, s)
		if pctx == ctx {
			for _, v := range cs {
				if v.index > s.index {
					panic("v.index > s.index is unexpected")
				}
				// re-write the index for extra safety.
				// we don't know what we don't know.
				v.index = s.index
			}
			r.queue = r.queue[done:]
			for _, v := range cs {
				delete(r.pending, v.ctx)
			}
			if len(r.queue) != len(r.pending) {
				panic("inconsistent length")
			}
			return cs
		}
	}
	return nil
}
