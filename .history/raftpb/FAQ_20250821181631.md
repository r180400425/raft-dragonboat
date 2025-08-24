## FAQ

Q: I got the following error message when building my dragonboat application in Go.
问： 我在 Go 中构建我的 dragonboat 应用程序时得到了以下错误消息。

internal/logdb/gorocksdb/backup.go:5:23: fatal error: rocksdb/c.h: No such file or directory
compilation terminated.

A: RocksDB is not installed or its installation location is not provided to the go tool when building your application. Assuming RocksDB's header files are installed at /usr/local/include/rocksdb, and its lib files are installed at /usr/local/lib, use the following command when building your package xyz.
答： 在构建应用程序时，没有安装 RocksDB 或没有向 go 工具提供其安装位置。假设 RocksDB 的头文件安装在/usr/local/include/rocksdb，其 lib 文件安装在/usr/local/lib，在构建包 xyz 时使用以下命令。

CGO_LDFLAGS="-L/usr/local/lib -lrocksdb" CGO_CFLAGS="-I/usr/local/include" go build xyz

Q: I got the following error message when building my dragonboat application in Go.
问： 我在 Go 中构建我的 dragonboat 应用程序时得到了以下错误消息。

# github.com/lni/dragonboat/internal/logdb/gorocksdb
/tmp/go-build420214618/b111/_x003.o: In function `_cgo_ee4f3b6122c2_Cfunc_rocksdb_cache_get_pinned_usage':
/tmp/go-build/cgo-gcc-prolog:73: undefined reference to `rocksdb_cache_get_pinned_usage'
/tmp/go-build420214618/b111/_x003.o: In function `_cgo_ee4f3b6122c2_Cfunc_rocksdb_cache_get_usage':
/tmp/go-build/cgo-gcc-prolog:91: undefined reference to `rocksdb_cache_get_usage'
/tmp/go-build420214618/b111/_x005.o: In function `_cgo_ee4f3b6122c2_Cfunc_rocksdb_checkpoint_create':
/tmp/go-build/cgo-gcc-prolog:43: undefined reference to `rocksdb_checkpoint_create'
/tmp/go-build420214618/b111/_x005.o: In function `_cgo_ee4f3b6122c2_Cfunc_rocksdb_checkpoint_object_destroy':
/tmp/go-build/cgo-gcc-prolog:55: undefined reference to `rocksdb_checkpoint_object_destroy'
... ...

A: RocksDB is not installed or its installation location is not provided to the go tool when building your application. Assuming RocksDB is installed at /usr/local/lib, use the following command when building your package xyz.
答： 在构建应用程序时，没有安装 RocksDB 或没有向 go 工具提供其安装位置。假设 RocksDB 安装在/usr/local/lib，在构建软件包 xyz 时使用以下命令。

CGO_LDFLAGS="-L/usr/local/lib -lrocksdb" go build xyz

Q: RocksDB failed to build.
问：RocksDB 无法构建。

A: Please use GCC version 5, 6 or 7 for building RocksDB. When using GCC 8, you may want to use the following command to install a more recent version of RocksDB.
答： 请使用 GCC 版本 5、6 或 7 来构建 RocksDB。使用 GCC 8 时，您可能需要使用以下命令来安装更新版本的 RocksDB。

ROCKSDB_VER=5.16.6 make install-rocksdb-ull

This issue has been discussed in #7 and #16.
这个问题已经在 #7 和 #16 中讨论过了。

Q: I got the following error message when playing with those built-in tests.
问： 我在使用这些内置测试时收到以下错误消息。

==27559==ASan runtime does not come first in initial library list; you should either link runtime to your application or manually preload it with LD_PRELOAD.

A: Run make clean and start over again. This is usually caused by loading a .so plugin built with ASAN from a test program not using ASAN.
答： 运行 “清理” 并重新开始。这通常是由于从未使用 ASAN 的测试程序加载使用 ASAN 构建的.so 插件导致的。

Q: What is the status for other CPU/OS support.
问： 其他 CPU/操作系统支持的状态如何？

A: Linux/AMD64 is the most tested platform. MacOSX/Darwin can be used as your DEV/TEST environment. Dragonboat is also known to work on Linux/ARM64，it has been tested on both Amazon EC2's A1 instances and scaleway.com's ARM cloud. 32bit platforms such as Linux/386 will not be supported. Windows will not be supported.
答：Linux/AMD 64 是测试最多的平台。MacOSX/达尔文可以用作您的开发/测试环境。Dragonboat 也可以在 Linux/ARM 64 上运行，它已经在 Amazon EC2 的 A1 实例和 scaleway.com 的 ARM 云上进行了测试。不支持 32 位平台，如 Linux/386。Windows 将不受支持。

MacOSX/Darwin should not be used in production, see this issue. A warning message is emitted every time when you start NodeHost on Darwin.
MacOSX/达尔文不应用于生产，请参阅此问题 。每次在达尔文上启动 NodeHost 时都会发出警告消息。

Q: Go module support?
Q：Go 模块支持？

A: Go module was added as preliminary support in Go 1.11, it will be finalized in the coming Go 1.12. Go module will be supported by dragonboat once Go 1.12 is released.
答：Go 模块是在 Go 1.11 中作为初步支持添加的，它将在即将到来的 Go 1.12 中完成。Go 1.12 发布后，Dragonboat 将支持 Go 模块。


## Hacking Guide  黑客指南
Running tests  运行测试

To run a selected test say in internal/raft
要运行选定的测试，请使用 internal/raft

TEST_TO_RUN=TestObserverWillNotVoteInElection make test-raft

Run tests with race detector enabled
在启用竞争检测器的情况下运行测试

RACE=1 TEST_TO_RUN=TestObserverWillNotVoteInElection make test-raft

Running monkey tests  做猴子测试

We use 22 cores 2.8GHz Xeon servers each with 64G memory for running monkey tests. See doc/test.md for more details on our monkey tests.
我们使用 22 核 2.8GHz Xeon 服务器，每个服务器具有 64 G 内存，用于运行猴子测试。请参阅 doc/test.md 了解有关我们猴子测试的更多详细信息。

On a Linux machine, first create a 14G RAMDISK using tmpfs
在 Linux 机器上，首先使用 tmpfs 创建一个 14 G RAMDISK

sudo mount -t tmpfs -o size=14000M tmpfs /media/drummermt-ramdisk-test

Deploy the monkey test binaries to the RAMDISK and start running tests.
将 monkey 测试二进制文件部署到 RAMDISK 并开始运行测试。

cd $GOPATH/github.com/lni/dragonboat/scripts/ramdisk_mt
./monkey_testing.sh deploy 64
./monkey_testing.sh start 64

The commands above will start 64 instances each hosting 128 raft groups. Each iteration takes about 30 minutes and by default 100 iterations are scheduled.
上面的命令将启动64个实例，每个实例承载128个筏组。每次迭代大约需要30分钟，默认情况下计划100次迭代。

When running monkey tests, you can check its progress -
当运行猴子测试时，您可以检查其进度-

cd /media/drummermt-ramdisk-test/
./rdttools.sh progress

and unexpected error in there is any
和意外的错误，

./rdttools.sh error

Reduce the number of monkey tests instances when there is less core count or memory on your server.
当服务器上的内核数量或内存较少时，减少猴子测试实例的数量。
When testing your changes
测试更改时

    please add tests to cover your changes
    请添加测试以覆盖您的更改
    make sure your changes doesn't break any existing tests, fix them if you believe they are no longer valid.
    确保你的修改不会破坏任何现有的测试，如果你认为它们不再有效，就修复它们。
    run monkey tests  做猴子测试
