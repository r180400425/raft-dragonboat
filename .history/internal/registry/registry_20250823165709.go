// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

package registry

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/raft" // 新增导入 raft 包以实现其接口，为了使raft.ILeaderResolver 接口不循环导入
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/raftio"
)

var (
	// ErrUnknownTarget is the error returned when the target address of the node
	// is unknown.
	ErrUnknownTarget = errors.New("target address unknown")
)

// IResolver converts the (shard id, replica id) tuple to network address.
type IResolver interface {
	// Resolve 解析目标分片领导者的地址
	// shardID: 目标分片ID；replicaID: 目标节点ID（0表示查询领导者）
	Resolve(uint64, uint64) (string, string, error)
	Add(uint64, uint64, string)
	// SetShardLeader 注册分片领导者信息
	// shardID: 分片ID；leaderID: 领导者节点ID
	SetShardLeader(shardID uint64, leaderID uint64)
}

var _ raftio.INodeRegistry = (*Registry)(nil)
var _ IResolver = (*Registry)(nil)
var _ raft.ILeaderResolver = (*Registry)(nil) //新增： 确保 Registry 实现 raft.ILeaderResolver 接口（编译时检查）

// Registry is used to manage all known node addresses in the multi raft system.
// The transport layer uses this address registry to locate nodes.
// 新增Registry 扩展支持分片领导者跟踪
// Registry 管理多 Raft 系统中的节点地址和分片领导者信息
type Registry struct {
	partitioner  server.IPartitioner
	validate     config.TargetValidator
	addr         sync.Map // map of raftio.NodeInfo => string// map[raftio.NodeInfo]string (节点地址映射)
	shardLeaders sync.Map //新增 map[uint64]uint64 (分片ID -> 领导者节点ID)
}

// NewNodeRegistry returns a new Registry object.
// NewNodeRegistry 创建新的 Registry 实例
func NewNodeRegistry(streamConnections uint64, v config.TargetValidator) *Registry {
	n := &Registry{validate: v}
	if streamConnections > 1 {
		n.partitioner = server.NewFixedPartitioner(streamConnections)
	}
	return n
}

// Close closes the registry.
func (n *Registry) Close() error { return nil }

// Add adds the specified replica and its target info to the registry.
func (n *Registry) Add(shardID uint64, replicaID uint64, target string) {
	if n.validate != nil && !n.validate(target) {
		plog.Panicf("invalid target %s", target)
	}
	key := raftio.GetNodeInfo(shardID, replicaID)
	v, ok := n.addr.LoadOrStore(key, target)
	if ok {
		if v.(string) != target {
			plog.Panicf("inconsistent target for %s, %s:%s",
				logutil.DescribeNode(shardID, replicaID), v, target)
		}
	}
}

func (n *Registry) getConnectionKey(addr string, shardID uint64) string {
	if n.partitioner == nil {
		return addr
	}
	return fmt.Sprintf("%s-%d", addr, n.partitioner.GetPartitionID(shardID))
}

// Remove removes a remote from the node registry.
func (n *Registry) Remove(shardID uint64, replicaID uint64) {
	n.addr.Delete(raftio.GetNodeInfo(shardID, replicaID))
}

// RemoveShard removes info associated with the specified shard.
func (n *Registry) RemoveShard(shardID uint64) {
	var toRemove []raftio.NodeInfo
	n.addr.Range(func(k, v interface{}) bool {
		ni := k.(raftio.NodeInfo)
		if ni.ShardID == shardID {
			toRemove = append(toRemove, ni)
		}
		return true
	})
	for _, v := range toRemove {
		n.addr.Delete(v)
	}
}

// Resolve 解析目标地址（支持跨分片领导者查询）
// Resolve looks up the address of the specified node.
// 新增：Resolve 实现 raft.ILeaderResolver.Resolve 方法：解析目标分片领导者地址
func (n *Registry) Resolve(shardID uint64, replicaID uint64) (string, string, error) {

	// 新增
	// 如果目标是分片领导者（replicaID=0），动态查询当前领导者
	if replicaID == 0 {
		leaderID, ok := n.shardLeaders.Load(shardID)
		if !ok {
			return "", "", ErrUnknownTarget
		}
		replicaID = leaderID.(uint64)
	}
	// 原有代码
	key := raftio.GetNodeInfo(shardID, replicaID)
	addr, ok := n.addr.Load(key)
	if !ok {
		return "", "", ErrUnknownTarget
	}
	return addr.(string), n.getConnectionKey(addr.(string), shardID), nil
}

// 新增
// 创建支持分片领导者解析的解析器
func NewDefaultShardLeaderResolver(nhConfig config.NodeHostConfig) *Registry {
	return &Registry{
		validate: nhConfig.GetTargetValidator(),
	}
}

// SetShardLeader 注册分片领导者信息
func (n *Registry) SetShardLeader(shardID uint64, leaderID uint64) {
	n.shardLeaders.Store(shardID, leaderID)
}

// 新增
// 移除以下代码块（原实现 raftio.ShardLeaderResolver 接口的 GetLeader 方法）
// GetLeader 返回指定分片的当前领导者 ID（实现 raftio.ShardLeaderResolver 接口）

// func (r *Registry) GetLeader(shardID uint64) (uint64, error) {
// 	// 使用 sync.Map 的 Load 方法安全读取领导者信息
// 	leaderID, exists := r.shardLeaders.Load(shardID)
// 	if !exists {
// 		return 0, errors.Errorf("leader for shard %d not found", shardID)
// 	}
// 	// 将 interface{} 类型转换为 uint64
// 	return leaderID.(uint64), nil
// }

// 新增
// GetShardLeader 返回指定分片的当前领导者 ID（实现 raft.ILeaderResolver.GetShardLeader 接口）
func (r *Registry) GetShardLeader(shardID uint64) (uint64, error) {
	// 使用 sync.Map 的 Load 方法安全读取领导者信息
	leaderID, exists := r.shardLeaders.Load(shardID)
	if !exists {
		return 0, errors.Errorf("leader for shard %d not found", shardID)
	}
	// 将 interface{} 类型转换为 uint64
	return leaderID.(uint64), nil
}
