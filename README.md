# kvstore

嵌入式键值存储引擎，仅依赖 Go 标准库与本地文件系统。支持 WAL 崩溃恢复、MVCC 快照隔离事务、快照迭代器、后台版本压缩，以及检查点与 WAL 截断。

```go
db, err := kvstore.Open("/path/to/data", nil) // nil = 默认配置
defer db.Close()

// 单条操作（自动事务，不会冲突）
db.Put("name", []byte("alice"))
val, err := db.Get("name")
db.Delete("name")

// 多步事务（快照隔离 + 写写冲突检测）
tx, _ := db.Begin(false)        // false = 读写事务
tx.Put("a", []byte("1"))
tx.Delete("b")
err = tx.Commit()               // 冲突时返回 ErrConflict，全部写入回滚

// 只读快照事务 + 范围迭代
rtx, _ := db.Begin(true)
defer rtx.Rollback()
it, _ := rtx.NewIterator("user:", "user;") // [lower, upper)
for ; it.Valid(); it.Next() {
    fmt.Println(it.Key(), string(it.Value()))
}
```

## 存储格式

数据目录下有两类文件：

### `wal.log` — 预写日志

每个已提交事务一条记录，顺序追加：

```
crc32(4) | payloadLen(4) | payload
payload = seq(8) | opCount(4) | op...
op      = type(1=Put,2=Del) | keyLen(4) | key | valLen(4) | val
```

- 整数均为小端序；CRC32 覆盖 `payloadLen + payload`。
- 事务的所有操作合并为**一条**记录，重放时要么整体生效、要么整体丢弃，不存在半个事务。
- 提交顺序：编码记录 → 追加写 WAL → `fsync`（`SyncWrites` 开启时）→ 应用到内存索引。因此已提交事务必然已落盘。

### `checkpoint.dat` — 检查点

```
magic(8) "KVCKPT01" | seq(8) | count(8) | entry... | crc32(4)
entry = keyLen(4) | key | valLen(4) | val
```

- 保存截至 `seq` 时每个存活 key 的最新版本（墓碑省略）。
- 写入流程：写 `checkpoint.tmp` → `fsync` → 原子 `rename` 覆盖 `checkpoint.dat` → `fsync` 目录 → 截断 WAL。任意时刻崩溃，要么旧检查点 + 完整 WAL 可用，要么新检查点 + 空 WAL 可用；遗留的 `checkpoint.tmp` 在下次 `Open` 时被忽略并清理。

## 恢复流程（`Open`）

1. 删除遗留的 `checkpoint.tmp`（中断的检查点）。
2. 读取 `checkpoint.dat`（校验 magic 与 CRC），将各 key 以检查点 seq 载入内存索引。
3. 顺序扫描 `wal.log`，逐条校验 CRC，重放 `seq > checkpointSeq` 的已提交记录。
4. 遇到 CRC 不匹配、长度非法或读不完整的记录（崩溃造成的撕裂写）即停止重放，并将文件截断到最后一条有效记录之后，再从该位置继续追加。

已提交写入不会丢失（提交前已 fsync），也不会重复应用（重放按 seq 单调递增应用到内存索引，检查点之后的记录只应用一次）。

## 并发语义

- **MVCC 快照隔离**：每个 key 挂一条按 seq 升序的版本链。事务在 `Begin` 时固定快照 seq，读到的永远是 `≤ 快照 seq` 的最新版本，不受并发写事务影响；事务内可读到自己的未提交写（read-your-own-writes）。
- **写写冲突**：提交时检查写集合中每个 key 是否已有比本事务快照更新的已提交版本（first-committer-wins）。冲突事务返回 `ErrConflict` 并整体回滚，脏数据对外不可见。
- **迭代器**：`NewIterator` 在创建时物化快照数据，其生命周期内不受并发写、版本压缩、检查点影响；代价是内存占用与扫描范围成正比。
- **版本压缩（`Compact` / 后台 `GCInterval`）**：回收所有活跃快照都不可见的旧版本；最老活跃快照可见的版本一定保留。无活跃快照时，最新版本为墓碑的 key 被整体移除。压缩只持锁做内存操作，不阻塞读写事务的语义正确性。
- **检查点（`Checkpoint` / 后台 `CheckpointBytes`）**：生成检查点期间与提交串行（持同一把锁），只读快照不受影响。

## 配置（`Options`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `SyncWrites` | `true` | 提交前 fsync WAL。关闭后更快，但掉电可能丢失最近已提交事务 |
| `GCInterval` | 1 分钟 | 后台版本压缩周期；0 = 关闭 |
| `CheckpointBytes` | 64 MiB | WAL 超过该大小自动检查点并截断；0 = 关闭 |

`Open(dir, nil)` 使用默认值；传入非 nil 的 `Options` 则按原样使用（零值即关闭对应功能）。

## 限制

- 单进程嵌入式使用，无多进程文件锁；同一目录不要同时被两个进程打开。
- 内存索引（跳表）：全量 key 与未压缩的版本链常驻内存，数据集规模受内存限制。
- 迭代器为物化快照，大范围扫描的内存开销与结果集成正比。
- 单把互斥锁串行化提交，写吞吐为单写者模型；读操作同样走该锁，高并发下存在锁竞争。
- 检查点与提交串行，大检查点期间写延迟会上升。
- 空 key 不允许；key/value 大小受 WAL 单条记录上限（1 GiB）约束。

## 测试

```sh
go test -race ./...
```

覆盖：基本 CRUD 与重开恢复、快照隔离、写写冲突回滚、读己之写、迭代器快照稳定性（并发写 + 压缩 + 检查点期间）、范围扫描与 Seek、崩溃后 WAL 重放（含未提交事务不留痕）、WAL 撕裂尾截断、检查点与 WAL 截断、检查点中断安全、压缩对活跃快照的保护、自动检查点、多 goroutine 并发读写（race detector）。
