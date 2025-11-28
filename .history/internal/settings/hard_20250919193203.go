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

/*
Package settings is used for managing internal parameters that can be set at
compile time by expert level users. Most of those parameters can also be
overwritten by using the json mechanism described below.

Package settings 用于管理可以在编译时设置的内部参数。
大多数这些参数也可以通过下面描述的 json 机制进行覆盖。
*/
package settings

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/lni/dragonboat/v4/logger"
)

var (
	plog = logger.GetLogger("settings")
)

//
// Parameters in both hard.go and soft.go are _NOT_ a part of the public API.
// There is no guarantee that any of these parameters are going to be available
// in future releases. Change them only when you know what you are doing.
//
// This file contain hard configuration values that should _NEVER_ be changed
// after your system has been deployed. Changing any value here will CORRUPT
// the data in your existing deployment.
//
// We do have an mechanism to overwrite the default values for the hard struct.
// To tune these parameters, place a json file named
// dragonboat-hard-settings.json in the current working directory of your
// dragonboat application, all fields in the json file will be applied to
// overwrite the default setting values. e.g. for a json file with the
// following content -
//
// {
//   "LRUMaxSessionCount": 32,
// }
//
// hard.LRUMaxSessionCount will be set to 32
//
// The application need to be restarted to apply such configuration changes.
// Again - tuning these hard parameters using the above described json file
// will cause your existing data to be corrupted. Decide them in your dev/test
// phase, once your system is deployed in production, _NEVER_ change them.
//

// Hard is the hard settings that can not be changed after the system has been
// deployed.

//
// hard.go 和 soft.go 中的参数不是公共 API 的一部分。
// 不能保证这些参数在将来的版本中仍然可用。只有在您知道自己在做什么的情况下才更改它们。
//
// 此文件包含在系统部署后永远不应更改的硬配置值。更改此处的任何值都将损坏您现有部署中的数据。
//
// 我们确实有一种机制来覆盖 hard 结构的默认值。
// 要调整这些参数，请在 dragonboat 应用程序的当前工作目录中放置一个名为
// dragonboat-hard-settings.json 的 json 文件，json 文件中的所有字段都将应用于
// 覆盖默认设置值。例如，对于具有以下内容的 json 文件 -
//
// {
//   "LRUMaxSessionCount": 32,
// }
//
// hard.LRUMaxSessionCount 将被设置为 32
//
// 应用程序需要重新启动以应用这些配置更改。
// 再次强调 - 使用上述 json 文件调整这些硬参数将导致您现有数据损坏。
// 在开发/测试阶段决定这些参数，一旦系统在生产环境中部署，永远不要更改它们。
//

// Hard 是系统部署后不能更改的硬设置。

var Hard = getHardSettings()

// hard 结构体定义了系统部署后不能更改的硬设置参数
type hard struct {
	// LRUMaxSessionCount is the max number of client sessions that can be
	// concurrently held and managed by each raft shard.
	// LRUMaxSessionCount 是每个 raft 分片可以并发持有和管理的最大客户端会话数。
	LRUMaxSessionCount uint64
	// LogDBEntryBatchSize is the max size of each entry batch.
	// LogDBEntryBatchSize 是每个条目批次的最大大小。
	LogDBEntryBatchSize uint64
}

// BlockFileMagicNumber 是基于块的快照文件中使用的魔数。
// BlockFileMagicNumber is the magic number used in block based snapshot files.
var BlockFileMagicNumber = []byte{0x3F, 0x5B, 0xCB, 0xF1, 0xFA, 0xBA, 0x81, 0x9F}

const (
	//
	// RSM
	// RSM (Replicated State Machine 复制状态机)
	//

	// SnapshotHeaderSize defines the snapshot header size in number of bytes.
	// SnapshotHeaderSize 定义了快照头的大小（以字节为单位）。
	SnapshotHeaderSize uint64 = 1024

	//
	// transport
	// transport (传输)
	//

	// UnmanagedDeploymentID is the special deployment ID value used when no user
	// deployment ID is specified.
	// UnmanagedDeploymentID 是未指定用户部署 ID 时使用的特殊部署 ID 值。
	UnmanagedDeploymentID uint64 = 1
	// MaxMessageBatchSize is the max size for a single message batch sent between
	// nodehosts.
	// MaxMessageBatchSize 是节点主机之间发送的单个消息批次的最大大小。
	MaxMessageBatchSize uint64 = LargeEntitySize
	// SnapshotChunkSize is the snapshot chunk size.
	// SnapshotChunkSize 是快照块的大小。
	SnapshotChunkSize uint64 = 2 * 1024 * 1024
)

// HardHash returns the hash value of the Hard setting.
// HardHash 返回 Hard 设置的哈希值。
func HardHash(execShards uint64,
	logDBShards uint64, sessionCount uint64, batchSize uint64) uint64 {
	hashstr := fmt.Sprintf("%d-%d-%t-%d-%d",
		execShards,
		logDBShards,
		false, // was the UseRangeDelete option  // 曾经是 UseRangeDelete 选项
		sessionCount,
		batchSize)
	mh := md5.New()
	if _, err := io.WriteString(mh, hashstr); err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(mh.Sum(nil))
}

// getHardSettings 获取硬设置
func getHardSettings() hard {
	org := getDefaultHardSettings()
	overwriteHardSettings(&org)
	return org
}

// getDefaultHardSettings 获取默认的硬设置
func getDefaultHardSettings() hard {
	return hard{
		LRUMaxSessionCount:  4096,
		LogDBEntryBatchSize: 48,
	}
}
