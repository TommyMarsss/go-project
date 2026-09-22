# go-project / kvstore

一个仅依赖 Go 标准库与本地文件系统的嵌入式事务型键值存储引擎，支持：

- **WAL 持久化**：写事务先落盘（默认 fsync）再应用，崩溃后可完整恢复
- **MVCC 快照隔离**：读事务看到开始时刻的一致快照；写冲突自动检测并回滚
- **快照迭代器**：迭代期间不受并发写入与压缩影响
- **后台压缩（版本 GC）**：安全回收历史版本，不误删活跃快照引用的版本
- **检查点 + WAL 截断**：加速恢复，检查点过程可安全中断

## 快速开始

```go
db, err := kvstore.Open("/path/to/data",
    kvstore.WithSyncWrites(true),              // 每次提交 fsync（默认）
    kvstore.WithGCInterval(5*time.Minute),     // 后台版本压缩（可选）
)
if err != nil { log.Fatal(err) }
defer db.Close()

// 写事务
err = db.Update(func(tx *kvstore.Txn) error {
    if err := tx.Put([]byte("name"), []byte("alice")); err != nil {
        return err
    }
    return tx.Delete([]byte("temp"))
})
// err 可能是 kvstore.ErrTxnConflict（写写冲突，事务已整体回滚）

// 读事务（快照隔离）
err = db.View(func(tx *kvstore.Txn) error {
    v, err := tx.Get([]byte("name")) // 不存在时返回 kvstore.ErrKeyNotFound
    ...
})

// 迭代器（创建时物化快照，之后任意写入/压缩均不影响）
err = db.View(func(tx *kvstore.Txn) error {
    it := tx.NewIterator(&kvstore.IteratorOptions{Prefix: []byte("user:")})
    defer it.Close()
    for it.Rewind(); it.Valid(); it.Next() {
        fmt.Printf("%s = %s\n", it.Key(), it.Value())
    }
    return nil
})

// 手动维护操作
db.Compact()     // 回收不再被任何活跃快照引用的旧版本
db.Checkpoint()  // 生成检查点并截断已覆盖的 WAL 段
```

## 存储格式

数据目录包含三类文件：

```
MANIFEST              JSON：{"checkpoint_ts": N, "min_wal_seq": M}
checkpoint.dat        全量快照（checkpoint_ts 时刻的最新存活版本）
wal-000001.log ...    预写日志段（按序号递增）
```

### WAL 记录格式（小端序）

每条记录对应**一个已提交事务**，是原子应用的最小单位：

```
[4B crc32(payload)] [4B payloadLen] [payload]
payload = [8B commitTS] [4B numOps] [op]...
op      = [1B type: 0=put / 1=delete] [2B keyLen] [key] [4B valLen] [val]
```

提交路径：构造记录 → 追加到当前 WAL 段 → fsync（默认）→ 应用到内存索引。
因此已提交事务绝不会因崩溃丢失；未落盘完整记录的事务在恢复时被整体忽略，
不会重复应用（重放时按 `commitTS > checkpoint_ts` 过滤，且每条记录只出现一次）。

### 检查点文件格式

```
[8B magic "GPKVCP01"] [8B checkpointTS] [4B numEntries] [entry]... [4B crc32]
entry = [2B keyLen] [key] [4B valLen] [val]
```

检查点写入 `checkpoint.tmp` → fsync → 原子 rename 为 `checkpoint.dat` →
写 `MANIFEST.tmp` → 原子 rename 为 `MANIFEST` → 删除 `min_wal_seq` 之前的 WAL 段。
任何一步崩溃都不会破坏已有数据：rename 之前的崩溃保留旧检查点；
rename 之后、MANIFEST 更新之前的崩溃会在恢复时按 commitTS 过滤掉重复记录；
残留的 `.tmp` 文件在 Open 时直接清理。

## 恢复流程（Open）

1. 清理残留的 `*.tmp` 文件；
2. 读取 `MANIFEST` 得到 `checkpoint_ts` 与 `min_wal_seq`；
3. 加载 `checkpoint.dat`（校验 magic 与 CRC），将 `commitTS` 恢复到检查点时刻；
4. 按序号重放所有 `seq >= min_wal_seq` 的 WAL 段，仅应用 `commitTS > checkpoint_ts` 的记录；
   遇到 CRC 或长度校验失败的记录即停止该段解析（容忍崩溃造成的尾部撕裂写）；
5. 开启新的 WAL 段继续写入（绝不在可能撕裂的尾部后追加）。

## 并发语义

- **快照隔离**：事务在 `Begin` 时获得 `startTS = 当前 commitTS`，整个生命周期内
  只能看到 `commitTS <= startTS` 的版本，并发写事务的提交不可见。
- **写写冲突**：提交时检查写集合中每个 key 是否存在 `commitTS > startTS` 的已提交版本
  （first-committer-wins）。冲突事务整体回滚并返回 `ErrTxnConflict`，其写入永不对外可见。
- **读事务**：只读、永不冲突、不阻塞写；但活跃读事务会抬高 GC 水位线，
  长事务会阻止旧版本回收（见限制）。
- **迭代器**：创建时把快照中可见的键值物化为有序副本，之后的写入、删除、
  压缩、检查点均不影响迭代结果。
- **压缩（Compact）**：以"最老活跃事务的 startTS"为水位线，每个 key 仅保留
  水位线之上的全部版本 + 水位线处可见的那一个版本；若该版本是墓碑（删除标记）
  且无任何快照需要，则整个 key 被回收。压缩只持有短暂的临界区，不阻塞读写事务。
- **检查点**：与读写事务并发安全；通过 WAL 轮换保证被封存段中的记录
  全部 `<= checkpoint_ts`，截断不会丢失未覆盖的数据。

## 限制

- 单进程嵌入式使用；不支持多进程同时打开同一数据目录。
- 数据与索引全量驻留内存（map + 版本链），适合内存可容纳的工作集；
  没有 LSM/B+ 树等磁盘索引结构。
- 提交路径在临界区内完成 WAL 写入与 fsync，写吞吐受单线程提交串行化限制；
  读操作使用读写锁，可水平扩展。
- `WithSyncWrites(false)` 可提升写吞吐，但崩溃可能丢失最近未 fsync 的提交。
- 冲突检测粒度为 key 级（无范围冲突检测/SSI），只读快照不保证可串行化中的
  写偏序（write skew）约束——需要时可由应用层对关键 key 加"锁记录"。
- 迭代器在创建时物化快照，内存开销与结果集大小成正比；超长读事务会
  阻止版本 GC，应及时 `Rollback`/`Commit` 释放。
- WAL 段中段的损坏（非尾部）会导致该段后续记录被忽略；CRC 主要防护
  崩溃造成的尾部撕裂写，不防护磁盘静默腐坏后的全量恢复。

## 测试

```sh
go test -race -count=1 ./...
```

覆盖场景：基本 CRUD 与回滚、快照隔离、写写冲突与脏数据不可见、
崩溃后 WAL 重放恢复、撕裂写容忍、迭代器快照隔离、压缩期间活跃快照安全性、
墓碑回收、检查点与 WAL 截断、检查点中断恢复、并发读写/并发冲突写（race 检测）。
