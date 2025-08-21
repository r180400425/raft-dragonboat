# 测试 #

## 测试方法 ##
* 测试驱动开发：已移植 [etcd raft](https://github.com/coreos/etcd/tree/master/raft) 的相关测试用例，确保 etcd 项目识别的所有边界场景均已覆盖。
* 高测试覆盖率：通过单元测试和 [混沌测试](https://en.wikipedia.org/wiki/Monkey_testing) 进行全面测试。
* 线性一致性检查：使用 [Jepsen](https://github.com/jepsen-io/jepsen) 的 [Knossos](https://github.com/jepsen-io/knossos) 和 [porcupine](https://github.com/anishathalye/porcupine) 工具检查 I/O 操作的线性一致性。
* 模糊测试：基于 [go-fuzz](https://github.com/dvyukov/go-fuzz) 进行模糊测试。
* I/O 错误注入测试：使用 [ScyllaDB](http://www.scylladb.com/) 的 [charybdefs](https://github.com/scylladb/charybdefs) 工具向底层文件系统注入 I/O 错误，验证 Dragonboat 的错误处理能力。
* 掉电测试：测试系统在断电后的实际表现。

## 混沌测试 ##
### 测试环境 ###
* 每个进程包含 5 个 NodeHost 和 3 个 Drummer 服务器
* 每个进程包含数百个 Raft 分片
* 随机终止并重启 NodeHost 和 Drummer 服务器（每个 NodeHost 通常在线几分钟）
* 随机删除某个 NodeHost 的所有数据，模拟永久性磁盘故障
* 随机丢弃和重排 NodeHost 之间的通信消息
* 随机将 NodeHost 与网络其他部分隔离
* 部分实例在后台持续进行快照生成和日志压缩
* 已提交的日志条目延迟随机时间后应用
* 快照的捕获和应用延迟随机时间
* 后台工作线程持续对随机 Raft 分片进行读写操作，并检查 stale read（过期读）
* 客户端活动历史文件通过线性一致性检查工具（如 Jepsen 的 Knossos）验证
* 每台测试服务器并发运行数百个上述进程，每次迭代 30 分钟，每晚执行多次迭代
* 每晚在多台服务器上并发运行

### 检查项 ###
* 无线性一致性违规
* 无分片永久阻塞
* 状态机必须保持同步
* 分片成员关系必须一致
* LogDB 中存储的 Raft 日志必须一致
* 无僵尸分片节点

### 测试结果 ###
部分 Jepsen [Knossos](https://github.com/jepsen-io/knossos) 格式（edn）的历史文件已公开[获取](https://github.com/lni/knossos-data)。

# 性能基准测试 #

## 测试环境 ##
* 三台服务器，每台配备一颗 22 核 Intel XEON E5-2696v4 处理器（所有核心可睿频至 2.8GHz）
* 40GE Mellanox 网卡
* Intel 900P SSD 用于存储 RocksDB 的 WAL，Intel P3700 1.6T SSD 用于存储其他所有数据
* Ubuntu 16.04 系统（已应用 Spectre 和 Meltdown 漏洞补丁），ext4 文件系统

## 基准测试方法 ##
* 三台服务器的三个 NodeHost 实例共部署 48 个 Raft 分片
* 每个 Raft 节点使用基于内存的键值存储作为 RSM（状态机）
* 键值存储以更新操作为主
* 所有 I/O 请求由本地进程发起
* 每个请求在独立 goroutine 中处理（线程模型简单，便于应用调试）
* 严格保证 fsync 调用
* 禁用双向 TLS（MutualTLS）

## Intel Optane SSD 对比 ##
与 Intel P3700 等企业级 NVME SSD 相比：
- 在 payload 为 16/128 字节时，Optane SSD 未提升吞吐量
- 在 payload 为 1024 字节时，Optane SSD 吞吐量略有提升
- 在 payload 为 1024 字节时，Optane SSD 写入延迟有所改善
