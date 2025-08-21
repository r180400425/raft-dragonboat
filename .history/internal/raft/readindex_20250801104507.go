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
		pending: make(map[raftpb.SystemCtx]*readStatus),
		queue:   make([]raftpb.SystemCtx, 0),
	}
}

// 添加一个待处理的ReadIndex请求
func (r *readIndex) addRequest(index uint64,
	ctx raftpb.SystemCtx, from uint64) {
	if _, ok := r.pending[ctx]; ok {
		return
	}
	// index is the committed value of the shard, it should never move
	// backward, check it here
	if len(r.queue) > 0 {
		p, ok := r.pending[r.peepCtx()]
		if !ok {
			panic("inconsistent pending and queue")
		}
		if index < p.index {
			plog.Panicf("index moved backward in readIndex, %d:%d",
				index, p.index)
		}
	}
	r.queue = append(r.queue, ctx)
	r.pending[ctx] = &readStatus{
		index:     index,
		from:      from,
		ctx:       ctx,
		confirmed: make(map[uint64]struct{}),
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
