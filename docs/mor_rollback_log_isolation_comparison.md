# MOR 表 Rollback Log 文件隔离方案对比

## 1. 问题背景

在 OCC 场景下，rollback 强制 rollover 到新 log 文件后，需要解决一个关键问题：**如何防止后续正常 writer append 到 rollback 生成的 log 文件**。

如果正常 writer 可以 append 到 rollback 文件，当另一个 rollback 和该 writer 并发运行时，可能再次出现并发 append 同一 log 文件的风险。因此，rollback 文件应当被"密封"，只包含 `ROLLBACK_BLOCK`。

本文档对比两种隔离方案：
- **方案 A**：特殊 ROLLBACK_WRITE_TOKEN 标识
- **方案 B**：Null WriteToken（无 write token 文件名）

---

## 2. 方案 A：特殊 ROLLBACK_WRITE_TOKEN

### 2.1 核心思路

定义一个符合正则 `\d+-\d+-\d+` 的特殊保留 token，用于标识 rollback 文件。正常 writer 在选择要 append 的 log 文件时，**过滤掉**带有该 token 的文件，直接 append 到之前的非 rollback 文件。

### 2.2 常量定义

```java
// HoodieLogFormat.java
/**
 * 用于 rollback 生成的 log 文件的特殊 write token。
 * 正常 writer 检测到此 token 后跳过该文件，继续 append 到之前的 log 文件。
 */
String ROLLBACK_WRITE_TOKEN = "999999999-0-0";
```

### 2.3 Rollback 写入路径

```java
// BaseRollbackHelper.maybeDeleteAndCollectStats()
writer = HoodieLogFormat.newWriterBuilder()
    .onParentPath(...)
    .withFileId(fileId)
    .overBaseCommit(latestBaseInstant)
    .withStorage(...)
    .withLogWriteToken(HoodieLogFormat.ROLLBACK_WRITE_TOKEN)
    .withRolloverLogWriteToken(HoodieLogFormat.ROLLBACK_WRITE_TOKEN)
    .withLogWriteCallback(getRollbackLogMarkerCallback(...))
    .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
    .build();
```

### 2.4 正常 Writer 路径

```java
// HoodieWriteHandle.createLogWriter()
protected HoodieLogFormat.Writer createLogWriter(
    Option<FileSlice> fileSlice, String baseCommitTime, String suffix) throws IOException {

  // 过滤掉 rollback 文件，找到最新的可追加 log 文件
  Option<HoodieLogFile> latestLogFile = fileSlice.isPresent()
      ? Option.fromJavaOptional(
          fileSlice.get().getLogFiles()
              .filter(lf -> !HoodieLogFormat.ROLLBACK_WRITE_TOKEN.equals(lf.getLogWriteToken()))
              .findFirst())  // TreeSet reverse 排序，findFirst = 最新非 rollback 文件
      : Option.empty();

  return HoodieLogFormat.newWriterBuilder()
      .onParentPath(...)
      .withFileId(fileId)
      .overBaseCommit(baseCommitTime)
      .withLogVersion(latestLogFile.map(HoodieLogFile::getLogVersion)
          .orElse(HoodieLogFile.LOGFILE_BASE_VERSION))
      .withFileSize(latestLogFile.map(HoodieLogFile::getFileSize).orElse(0L))
      .withLogWriteToken(latestLogFile.map(HoodieLogFile::getLogWriteToken).orElse(writeToken))
      .withSizeThreshold(config.getLogFileMaxSize())
      .withStorage(storage)
      .withRolloverLogWriteToken(writeToken)
      .withSuffix(suffix)
      .withLogWriteCallback(getLogWriteCallback())
      .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
}
```

### 2.5 文件系统效果

```
初始（Writer A 失败）：
  .log.1_0-0-0         Writer A 数据
  .log.2_0-0-0         Writer A 数据

Rollback 后：
  .log.1_0-0-0         Writer A 数据
  .log.2_0-0-0         Writer A 数据
  .log.3_999999999-0-0 ROLLBACK_BLOCK（密封）

Writer B 写入后：
  .log.1_0-0-0         Writer A 数据
  .log.2_0-0-0         Writer A 数据 + Writer B 数据（直接 append 到已有文件）
  .log.3_999999999-0-0 ROLLBACK_BLOCK（密封，未被触碰）

Writer B 如果 .log.2 已满需要 rollover：
  computeNextLogVersion → max=3 → next=4
  .log.4_0-0-1         Writer B 数据（自然跳过了 rollback 的版本号）
```

### 2.6 正则兼容性分析

```
LOG_FILE_PATTERN = "^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?"

文件名: .abc_20230101.log.3_999999999-0-0
匹配结果:
  group(1) = "abc"            ✅ fileId
  group(2) = "20230101"       ✅ baseCommitTime
  group(3) = "log"            ✅ extension
  group(4) = "3"              ✅ version
  group(6) = "999999999-0-0"  ✅ writeToken（完整匹配）
  group(7) = "999999999"      ✅ taskPartitionId
  group(8) = "0"              ✅ stageId
  group(9) = "0"              ✅ attemptNumber
```

所有 `getLogWriteToken()`、`getTaskPartitionIdFromLogPath()` 等方法**均正常工作**，无 null 值。

---

## 3. 方案 B：Null WriteToken

### 3.1 核心思路

Rollback 创建 log 文件时不带 write token（传入 `null`），生成文件名如 `.abc_20230101.log.3`（无 `_0-0-0` 后缀）。利用现有代码中对 null writeToken 的处理逻辑，使后续 writer **隐式地**不 append 到该文件。

### 3.2 Rollback 写入路径

需要修改 `WriterBuilder.build()` 以支持显式创建无 token 文件（否则现有 null 检查会自动 rollover 并赋予 `rolloverLogWriteToken`）：

```java
// HoodieLogFormat.WriterBuilder — 新增字段
private boolean forceNullWriteToken = false;

public WriterBuilder withForceNullWriteToken(boolean flag) {
    this.forceNullWriteToken = flag;
    return this;
}

// build() 方法中:
if (!forceNullWriteToken && logWriteToken == null) {
    // 现有逻辑：旧格式文件自动 rollover
    logVersion += 1;
    fileLen = 0L;
    logWriteToken = rolloverLogWriteToken;
}
// 当 forceNullWriteToken=true 时，保持 logWriteToken=null
// makeLogFileName(fileId, ext, baseCommitTime, version, null)
// → ".abc_20230101.log.3"（无 write token 后缀）
```

```java
// BaseRollbackHelper — 使用 forceNullWriteToken
writer = HoodieLogFormat.newWriterBuilder()
    .onParentPath(...)
    .withFileId(fileId)
    .overBaseCommit(latestBaseInstant)
    .withStorage(...)
    .withForceNullWriteToken(true)
    .withLogWriteCallback(getRollbackLogMarkerCallback(...))
    .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
    .build();
```

### 3.3 正常 Writer 路径 — 通过 HoodieWriteHandle（主要路径）

**不需要显式修改代码**，依赖 Hudi `Option.map` 的 null 传播链：

```java
// HoodieWriteHandle.createLogWriter() — 代码不变
Option<HoodieLogFile> latestLogFile = fileSlice.isPresent()
    ? fileSlice.get().getLatestLogFile()  // → .log.3（rollback 文件，无 token）
    : Option.empty();

.withLogVersion(latestLogFile.map(HoodieLogFile::getLogVersion).orElse(...))
// → logVersion = 3

.withLogWriteToken(latestLogFile.map(HoodieLogFile::getLogWriteToken).orElse(writeToken))
// 关键链路:
//   getLogWriteToken() 返回 null
//   → Option.map(f) 内部调用 Option.ofNullable(f.apply(val))
//   → Option.ofNullable(null) → Option.empty()
//   → .orElse(writeToken) → 使用 Writer 自己的 token "0-0-1"
```

Builder 用 version=3 + token="0-0-1" 构建文件名 → `.abc_20230101.log.3_0-0-1`。
这是一个**全新的文件**（不是 append 到 `.log.3`）。

### 3.4 正常 Writer 路径 — 通过 WriterBuilder 自动计算版本

```java
// 不通过 HoodieWriteHandle，直接用 WriterBuilder（如 compaction 等场景）
getLatestLogVersion() → Pair(3, null)
logWriteToken = null
→ 进入现有 null 检查分支：
   logVersion = 3 + 1 = 4
   logWriteToken = rolloverLogWriteToken
→ 创建 .log.4_xxx
```

### 3.5 文件系统效果

**通过 HoodieWriteHandle 路径：**

```
初始（Writer A 失败）：
  .log.1_0-0-0     Writer A 数据
  .log.2_0-0-0     Writer A 数据

Rollback 后：
  .log.1_0-0-0     Writer A 数据
  .log.2_0-0-0     Writer A 数据
  .log.3           ROLLBACK_BLOCK（无 write token，密封）

Writer B 写入后（HoodieWriteHandle 路径）：
  .log.1_0-0-0     Writer A 数据
  .log.2_0-0-0     Writer A 数据（未被复用）
  .log.3           ROLLBACK_BLOCK（密封）
  .log.3_0-0-1     Writer B 数据（新文件，同版本不同文件名）
```

**通过 WriterBuilder 自动计算版本路径：**

```
Writer B 写入后（WriterBuilder 路径）：
  .log.1_0-0-0     Writer A 数据
  .log.2_0-0-0     Writer A 数据（未被复用）
  .log.3           ROLLBACK_BLOCK（密封）
  .log.4_0-0-1     Writer B 数据（跳到新版本）
```

**注意：两条路径结果不同，且 .log.2 的空间均未被充分利用。**

### 3.6 正则兼容性分析

```
LOG_FILE_PATTERN = "^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?"

文件名: .abc_20230101.log.3
匹配结果:
  matcher.find() = true（write token 组是 optional，匹配到 version 即停止）
  group(1) = "abc"       ✅ fileId
  group(2) = "20230101"  ✅ baseCommitTime
  group(3) = "log"       ✅ extension
  group(4) = "3"         ✅ version
  group(5) = null        ⚠️ 整个 write token 组
  group(6) = null        ⚠️ writeToken
  group(7) = null        ⚠️ taskPartitionId
  group(8) = null        ⚠️ stageId
  group(9) = null        ⚠️ attemptNumber
  group(10) = null       ⚠️ cdc suffix
```

文件可以被正确识别为 log 文件（`matcher.find()` 成功），但所有 token 相关字段解析为 null。
现有代码中 `getTaskPartitionIdFromLogPath()` 等方法对 null 有容错处理（返回 null 而非抛异常），但需要所有调用方都能正确处理 null 返回值。

---

## 4. 详细对比

### 4.1 正常 Writer 行为对比

| 维度 | 方案 A（特殊 Token） | 方案 B（Null Token） |
|------|---------------------|---------------------|
| **Writer 如何避开 rollback 文件** | 显式过滤 `ROLLBACK_WRITE_TOKEN` | 隐式依赖 `Option.map(null)` → `empty()` 链路 |
| **Writer append 到哪个文件** | 回到 rollback 之前的文件（如 `.log.2`）继续 append | 创建新文件（`.log.3_0-0-1` 或 `.log.4`），**不复用旧文件** |
| **是否产生额外文件** | ❌ 不产生（复用已有文件） | ✅ 产生（每次都是新文件） |
| **行为一致性** | ✅ 所有代码路径行为一致 | ⚠️ HoodieWriteHandle 路径和 WriterBuilder 路径**行为不同** |
| **同版本多文件** | ❌ 不会出现 | ✅ 可能出现（`.log.3` 和 `.log.3_0-0-1`） |

### 4.2 实现复杂度对比

| 维度 | 方案 A（特殊 Token） | 方案 B（Null Token） |
|------|---------------------|---------------------|
| **新增常量** | 1 个（`ROLLBACK_WRITE_TOKEN`） | 0 个 |
| **HoodieLogFormat 修改** | 增加 1 行常量定义 | 增加 `forceNullWriteToken` 字段 + `build()` 逻辑修改 |
| **BaseRollbackHelper 修改** | 设置 `withLogWriteToken(ROLLBACK_WRITE_TOKEN)` | 设置 `withForceNullWriteToken(true)` |
| **HoodieWriteHandle 修改** | 加过滤逻辑（~5 行） | **不需要修改** |
| **WriterBuilder 修改** | **不需要修改** | 加 `forceNullWriteToken` 逻辑（~8 行） |
| **总改动量** | ~10 行 | ~12 行 |

### 4.3 可维护性对比

| 维度 | 方案 A（特殊 Token） | 方案 B（Null Token） |
|------|---------------------|---------------------|
| **意图表达** | ✅ 显式——过滤条件直接体现"跳过 rollback 文件" | ⚠️ 隐式——需要理解 `Option.map(null)` → `ofNullable` → `empty()` → `orElse` 链路 |
| **可调试性** | ✅ 文件名 `.log.3_999999999-0-0` 一眼可辨识为 rollback 文件 | ⚠️ 文件名 `.log.3` 无法与旧格式文件区分 |
| **代码搜索** | ✅ 搜索 `ROLLBACK_WRITE_TOKEN` 可找到所有相关逻辑 | ⚠️ 无特定关键词可搜索，逻辑分散在 null 处理链路中 |
| **新开发者理解成本** | ✅ 低——常量名自解释 | ⚠️ 高——需要理解 Option、WriterBuilder、正则三者的交互 |
| **回归风险** | ✅ 低——独立逻辑，不依赖其他代码副作用 | ⚠️ 高——依赖 `Option.map` 对 null 的处理行为，若未来 `Option` 实现变化会静默 break |

### 4.4 正确性保障对比

| 维度 | 方案 A（特殊 Token） | 方案 B（Null Token） |
|------|---------------------|---------------------|
| **正则解析** | ✅ 全部 group 正常解析，无 null | ⚠️ group(6)~group(9) 为 null |
| **`getLogWriteToken()`** | ✅ 返回 `"999999999-0-0"` | ⚠️ 返回 `null` |
| **`getTaskPartitionIdFromLogPath()`** | ✅ 返回 `999999999`（Integer） | ⚠️ 返回 `null`（Integer） |
| **`LogFileComparator`** | ✅ 正常字符串比较 | ⚠️ 依赖 `Comparator.nullsFirst` 处理 |
| **`FSUtils.getLatestLogVersion()`** | ✅ 返回 `Pair(3, "999999999-0-0")` | ⚠️ 返回 `Pair(3, null)` |
| **未来代码 null 安全** | ✅ 无 null 值，不存在 NPE 风险 | ⚠️ 所有使用 writeToken 的地方都需要处理 null |

### 4.5 读取顺序对比

**方案 A：**

```
升序排列（LogFileComparator）:
  .log.1_0-0-0         (version=1)
  .log.2_0-0-0         (version=2, 包含 Writer A + Writer B 数据)
  .log.3_999999999-0-0 (version=3, ROLLBACK_BLOCK)

读取顺序: 先读数据 → 再读 ROLLBACK_BLOCK ✅
结构清晰，每个版本只有一个文件
```

**方案 B（HoodieWriteHandle 路径）：**

```
升序排列（LogFileComparator）:
  .log.1_0-0-0   (version=1)
  .log.2_0-0-0   (version=2)
  .log.3         (version=3, null token, nullsFirst → 排在同版本最前)
  .log.3_0-0-1   (version=3, "0-0-1", 排在同版本之后)

读取顺序: .log.1 → .log.2 → .log.3(ROLLBACK) → .log.3_0-0-1(Writer B) ✅
功能正确，但同版本出现两个文件，结构不够清晰
```

**方案 B（WriterBuilder 路径）：**

```
升序排列（LogFileComparator）:
  .log.1_0-0-0   (version=1)
  .log.2_0-0-0   (version=2)
  .log.3         (version=3, ROLLBACK_BLOCK)
  .log.4_0-0-1   (version=4, Writer B 数据)

读取顺序: .log.1 → .log.2 → .log.3(ROLLBACK) → .log.4(Writer B) ✅
功能正确，但 .log.2 的空间未充分利用，版本号跳跃
```

---

## 5. 边界场景分析

### 5.1 .log.2 已满需要 rollover

| 方案 | 行为 | 结果 |
|------|------|------|
| **A** | Writer B append `.log.2` → 超过 sizeThreshold → `computeNextLogVersion()` 找到 max=3 → 创建 `.log.4_0-0-1` | ✅ 自然跳过 rollback 版本号 |
| **B (WriteHandle)** | Writer B 创建 `.log.3_0-0-1` → 超过 sizeThreshold → rollover → 创建 `.log.4_0-0-1` | ✅ 但多了一个中间文件 |
| **B (Builder)** | Writer B 创建 `.log.4_xxx` → 超过 sizeThreshold → rollover → 创建 `.log.5_xxx` | ✅ 但版本号跳跃更大 |

### 5.2 多次连续 rollback

```
场景: Writer A(t1) 失败, Writer B(t2) 也失败, 都需要 rollback
```

| 方案 | 文件系统 | 说明 |
|------|---------|------|
| **A** | `.log.1_0-0-0` → `.log.2_0-0-0` → `.log.3_999999999-0-0`(rollback t1) → `.log.4_999999999-0-0`(rollback t2) | 每个 rollback 一个独立文件，`_999999999-0-0` 后缀清晰标识 |
| **B** | `.log.1_0-0-0` → `.log.2_0-0-0` → `.log.3`(rollback t1) → `.log.4`(rollback t2) | 两个无 token 文件，无法从文件名区分是 rollback 还是旧格式遗留 |

### 5.3 File group 中无之前的 log 文件（仅有 rollback 文件）

```
场景: Writer A 只写了 .log.1, rollback 后创建 .log.2_(rollback), Writer B 开始
```

| 方案 | Writer B 行为 | 结果 |
|------|-------------|------|
| **A** | 过滤掉 `.log.2_999999999-0-0`，找到 `.log.1_0-0-0`，append 到 `.log.1` | ✅ 复用已有文件 |
| **B** | 遇到 `.log.2`(null token)，隐式创建 `.log.2_0-0-1` 或 `.log.3_xxx` | ✅ 功能正确但多一个新文件 |

### 5.4 与旧版本数据兼容

| 方案 | 旧版本 Hudi 读取 | 说明 |
|------|-----------------|------|
| **A** | 旧版 Hudi 读取 `_999999999-0-0` 文件 | ✅ 正常识别为 log 文件，正常读取其中的 COMMAND_BLOCK |
| **B** | 旧版 Hudi 读取无 token 的 `.log.3` 文件 | ✅ 被识别为旧格式文件，旧版 Writer 会自动 rollover（现有行为） |

### 5.5 `Option.map` 行为依赖分析（仅影响方案 B）

方案 B 的隔离机制依赖以下调用链：

```java
// Hudi Option.java
public <U> Option<U> map(Function<? super T, ? extends U> mapper) {
    if (!isPresent()) {
        return empty();
    } else {
        return Option.ofNullable(mapper.apply(val));  // ← 关键：null 返回值变为 empty()
    }
}

public static <T> Option<T> ofNullable(T value) {
    return null == value ? empty() : of(value);       // ← 关键：null → empty()
}
```

如果未来 `Option` 实现发生以下任一变化，方案 B 会静默失效：
- `map()` 不再调用 `ofNullable`（改为 `of()`）
- `ofNullable(null)` 不再返回 `empty()`
- `Option` 被替换为其他实现（如 Vavr 的 `Option`）

方案 A 不存在此类隐式依赖。

---

## 6. 总结与推荐

### 6.1 总结评分

| 评估维度 | 方案 A（特殊 Token） | 方案 B（Null Token） |
|----------|:-------------------:|:-------------------:|
| 意图显式性 | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| 可调试性 | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| 正则兼容性 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| Null 安全性 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| 文件空间效率 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| 行为一致性 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| 代码改动量 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| 新增概念 | ⭐⭐⭐⭐ | ⭐⭐⭐ |

### 6.2 推荐

**推荐方案 A（特殊 ROLLBACK_WRITE_TOKEN）**，核心理由：

1. **显式优于隐式**：过滤条件和特殊 token 直接表达了"rollback 文件隔离"的设计意图，任何开发者都能立即理解
2. **复用已有文件**：正常 writer 回到 rollback 之前的 log 文件 append，减少不必要的文件增长，提升空间利用率
3. **行为一致**：无论通过 `HoodieWriteHandle` 还是 `WriterBuilder`，正常 writer 都能正确识别并跳过 rollback 文件
4. **零 null 问题**：所有字段均可正常解析，不引入 null 处理的复杂度和潜在 NPE 风险
5. **运维友好**：从文件名 `_999999999-0-0` 后缀即可识别 rollback 文件，便于线上排查和问题定位
6. **无隐式依赖**：不依赖 `Option.map` 对 null 的特定处理行为，未来代码演进不会静默破坏隔离机制

### 6.3 方案 A 改动清单

| 文件 | 修改内容 | 改动量 |
|------|---------|--------|
| `HoodieLogFormat.java` | 新增 `ROLLBACK_WRITE_TOKEN` 常量定义 | +1 行 |
| `BaseRollbackHelper.java` | Rollback 构建 writer 时指定 `ROLLBACK_WRITE_TOKEN` | +2 行 |
| `HoodieWriteHandle.java` | `createLogWriter()` 中过滤 rollback 文件 | +5 行 |
| **总计** | | **~8 行** |
