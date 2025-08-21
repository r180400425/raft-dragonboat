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

// 版权所有 2017-2021 Lei Ni (nilei81@gmail.com) 及其他贡献者。
//
// 根据Apache许可证2.0版（以下简称"许可证"）授权；
// 除非遵守许可证，否则您不得使用此文件。
// 您可以在以下地址获取许可证副本：
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// 除非适用法律要求或书面同意，否则根据许可证分发的软件
// 按"原样"分发，不附带任何明示或暗示的担保或条件。
// 请参阅许可证以了解管理权限和限制的特定语言。

/*
Package logdb implements the persistent log storage used by Dragonboat.

This package is internally used by Dragonboat, applications are not expected
to import this package.

包logdb实现了Dragonboat使用的持久化日志存储。

此包供Dragonboat内部使用，应用程序不应导入此包。
*/
package logdb

import (
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb/kv"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

var (
	plog = logger.GetLogger("logdb") // 获取logdb包的日志记录器
)

// IReusableKey is the interface for keys that can be reused. A reusable key is
// usually obtained by calling the GetKey() function of the IContext
// instance.
// IReusableKey是可重用键的接口。可重用键通常通过调用IContext实例的GetKey()函数获得。
type IReusableKey interface {
	SetEntryBatchKey(shardID uint64, replicaID uint64, index uint64) // SetEntryBatchKey将键设置为指定Raft节点的条目批处理键，带有指定的条目索引。
	// SetEntryKey sets the key to be an entry key for the specified Raft node
	// with the specified entry index.
	SetEntryKey(shardID uint64, replicaID uint64, index uint64) // SetEntryKey将键设置为指定Raft节点的条目键，带有指定的条目索引。
	// SetStateKey sets the key to be an persistent state key suitable
	// for the specified Raft shard node.
	SetStateKey(shardID uint64, replicaID uint64) // SetStateKey将键设置为适合指定Raft分片节点的持久化状态键。
	// SetMaxIndexKey sets the key to be the max possible index key for the
	// specified Raft shard node.
	SetMaxIndexKey(shardID uint64, replicaID uint64) // SetMaxIndexKey将键设置为指定Raft分片节点的最大可能索引键。
	// Key returns the underlying byte slice of the key.
	Key() []byte // Key返回键的底层字节切片。
	// Release releases the key instance so it can be reused in the future.
	Release() // Release释放键实例，以便将来可以重用。
}

// IContext is the per thread context used in the logdb module.
// IContext is expected to contain a list of reusable keys and byte
// slices that are owned per thread so they can be safely reused by the same
// thread when accessing ILogDB.
// IContext是logdb模块中使用的每个线程的上下文。
// IContext应包含每个线程拥有的可重用键和字节切片列表，以便同一线程在访问ILogDB时可以安全地重用它们。
type IContext interface {
	// Destroy destroys the IContext instance.
	Destroy()
	// Reset resets the IContext instance, all previous returned keys and
	// buffers will be put back to the IContext instance and be ready to
	// be used for the next iteration.
	Reset() // Reset重置IContext实例，所有先前返回的键和缓冲区将放回IContext实例，准备用于下一次迭代。
	// GetKey returns a reusable key.
	// GetKey返回一个可重用键。
	GetKey() IReusableKey
	// GetValueBuffer returns a byte buffer with at least sz bytes in length.
	// GetValueBuffer返回一个长度至少为sz字节的字节缓冲区。
	GetValueBuffer(sz uint64) []byte
	// GetWriteBatch returns a write batch or transaction instance.
	// GetWriteBatch返回一个写入批处理或事务实例。
	GetWriteBatch() interface{}
	// SetWriteBatch adds the write batch to the IContext instance.
	// SetWriteBatch将写入批处理添加到IContext实例。
	SetWriteBatch(wb interface{})
	// GetEntryBatch returns an entry batch instance.
	// GetEntryBatch返回一个条目批处理实例。
	GetEntryBatch() pb.EntryBatch
	// GetLastEntryBatch returns an entry batch instance.
	// GetLastEntryBatch返回一个条目批处理实例。
	GetLastEntryBatch() pb.EntryBatch
}

// DefaultFactory is the default factory for creating LogDB instance.
// DefaultFactory是创建LogDB实例的默认工厂。
type DefaultFactory struct {
}

// NewDefaultFactory creates a new DefaultFactory instance.
// NewDefaultFactory创建一个新的DefaultFactory实例。
func NewDefaultFactory() *DefaultFactory {
	return &DefaultFactory{}
}

// Create creates the LogDB instance.
// Create创建LogDB实例。
func (f *DefaultFactory) Create(cfg config.NodeHostConfig,
	cb config.LogDBCallback,
	dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return NewDefaultLogDB(cfg, cb, dirs, lldirs)
}

// Name returns the name of the default LogDB instance.
// Name返回默认LogDB实例的名称。
func (f *DefaultFactory) Name() string {
	return "sharded-pebble"
}

// NewDefaultLogDB creates a Log DB instance using the default KV store
// implementation. The created Log DB tries to store entry records in
// plain format but it switches to the batched mode if there is already
// batched entries saved in the existing DB.
// NewDefaultLogDB使用默认的KV存储实现创建Log DB实例。
// 创建的Log DB尝试以普通格式存储条目记录，但如果现有DB中已保存有批处理条目，则会切换到批处理模式。
func NewDefaultLogDB(config config.NodeHostConfig,
	callback config.LogDBCallback,
	dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return NewLogDB(config,
		callback, dirs, lldirs, false, true, newDefaultKVStore)
}

// NewDefaultBatchedLogDB creates a Log DB instance using the default KV store
// implementation with batched entry support.
// NewDefaultBatchedLogDB使用支持批处理条目的默认KV存储实现创建Log DB实例。
func NewDefaultBatchedLogDB(config config.NodeHostConfig,
	callback config.LogDBCallback,
	dirs []string, lldirs []string) (raftio.ILogDB, error) {
	return NewLogDB(config,
		callback, dirs, lldirs, true, false, newDefaultKVStore)
}

// NewLogDB creates a Log DB instance based on provided configuration
// parameters. The underlying KV store used by the Log DB instance is created
// by the provided factory function.
// NewLogDB基于提供的配置参数创建Log DB实例。
// Log DB实例使用的底层KV存储由提供的工厂函数创建。
func NewLogDB(config config.NodeHostConfig,
	callback config.LogDBCallback, dirs []string, lldirs []string,
	batched bool, check bool, f kv.Factory) (raftio.ILogDB, error) {
	checkDirs(config.Expert.LogDB.Shards, dirs, lldirs)
	llDirRequired := len(lldirs) == 1
	if len(dirs) == 1 {
		for i := uint64(1); i < config.Expert.LogDB.Shards; i++ {
			dirs = append(dirs, dirs[0])
			if llDirRequired {
				lldirs = append(lldirs, lldirs[0])
			}
		}
	}
	return OpenShardedDB(config, callback, dirs, lldirs, batched, check, f)
}

// checkDirs检查日志数据库目录配置的有效性
func checkDirs(numOfShards uint64, dirs []string, lldirs []string) {
	if len(dirs) == 1 {
		if len(lldirs) != 0 && len(lldirs) != 1 {
			plog.Panicf("only 1 regular dir but %d low latency dirs", len(lldirs))
		}
	} else if len(dirs) > 1 {
		if uint64(len(dirs)) != numOfShards {
			plog.Panicf("%d regular dirs, but expect to have %d rdb instances",
				len(dirs), numOfShards)
		}
		if len(lldirs) > 0 {
			if len(dirs) != len(lldirs) {
				plog.Panicf("%v regular dirs, but %v low latency dirs", dirs, lldirs)
			}
		}
	} else {
		panic("no regular dir")
	}
}
