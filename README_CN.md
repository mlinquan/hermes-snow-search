# Hermes Snow Search

<p align="center"><img src="assets/avator_default_png8.png" width="500" alt="Snow"></p>

> [English](README.md) | 中文版 | [更新日志](CHANGELOG.md)

Hermes Agent 的内存级并行搜索插件。轻量源（sessions、facts、memory、skills）驻内存，深度搜索（消息正文）直接查 FTS5 数据库索引——启动秒级、搜索毫秒级、零内存开销。

## 为什么选 snow_search？（对比 session_search）

`session_search` 搜聊天记录。`snow_search` 是 Hermès 的**全局记忆检索层**——让 AI 跨端不失忆、一次召回即答案、人格持续在线。

| 价值 | snow_search | session_search |
|------|-------------|----------------|
| **跨端记忆恢复** | 换设备 / 开新会话，AI 接着上次聊，"记得"项目、铁则、偏好——人格连续 | 只搜当前 DB 的消息 |
| **一次召回完整答案** | 跨源聚合 + 排序 + 置信度。不反复检索、不分页——agent 直接拿到答案 | 返回原始消息；agent 要自己翻、组合、再追搜 |
| **人格与偏好持久** | 同时搜 memory（USER.md）+ soul + facts——AI 记得你是谁、怎么对你、哪些铁则不能破 | 只搜"说了什么" |

**本质区别**：session_search 找聊天，snow_search 让 AI 真正记得你。

## 核心优势

| # | 优势 | 说明 |
|---|------|------|
| 1 | **跨端记忆恢复** | 换设备、清上下文——AI"接着上次"。不是"重新认识"，是人格连续 |
| 2 | **一次召回完整答案** | 5 源并行，排序 + 置信度标注。省 token、省往返、早给回应 |
| 3 | **人格与偏好持久** | memory + soul + facts 统一搜索。AI 记得你是谁、怎么对你 |
| 4 | **<3s 启动** | 一次 SQL 探针；深度搜索复用 FTS5 索引 |
| 5 | **~MB 内存** | 仅轻量源驻内存，消息正文留数据库 |
| 6 | **精确 total** | FTS5 COUNT(*) —— agent 能准确回答"出现了几次" |
| 7 | **自动增量更新** | fact_store/memory 写入即时追加；FTS5 触发器保持消息索引实时 |
| 8 | **上下文不爆炸** | post_llm_call 自动清理搜索结果，对话始终流畅 |

## 示例

直接用自然语言问 AI，snow_search 自动检索——不必记参数，像问人一样问：

**时间回忆**
- "回忆一下昨天的聊天主题"
- "回一下最近半个月我在做什么"
- "上周三我们聊的那个 bug，后来怎么解决的"
- "这个项目最早是什么时候开始的"

**跨端记忆恢复**
- "我换了个设备，上次我们聊到哪了？"
- "之前那个讨论进行到哪一步了，接着说"

**跨源召回（答案不只在聊天里）**
- "cdog 的配置文件放哪了？" → 命中 facts / memory
- "我有哪些铁则？" → 命中 memory / soul
- "snow-agent 项目现在什么进度？" → 命中 facts
- "怎么用 cdog skill？" → 命中 skills

**精确计数（"几次/多少"类问题）**
- "「502 报错」这几天出现过几次？"
- "这个月我提了几次要重构 snow-search？"

**角色过滤（"我说过 / 你说过"）**
- "我之前有没有说过要重构 snow-agent？" → 只搜 user 消息
- "你上次怎么教我用 cdog 的？" → 只搜 assistant 消息

## 工作原理

1. **启动加载（轻量）** — 后台线程加载 sessions、facts、memory、skills 元数据
2. **驻内存（仅轻量源）** — sessions、facts、memory、skills 在 Python 列表中
3. **FTS5 深度搜索** — 消息正文留在 SQLite；搜索时查 `messages_fts`（unicode61）和 `messages_fts_trigram`（CJK）
4. **并行搜索** — ThreadPoolExecutor 并发轻量源；深度搜索内联跑 FTS5 查询
5. **增量更新** — post_tool_call 钩子捕获 fact_store / memory 写入，追加缓存
6. **CJK 路由** — ≥3 CJK 字符 → trigram 表；英文/混合 → unicode61；短 CJK（1-2 字）→ LIKE 兜底

## 安装

```bash
pip install hermes-snow-search
hermes plugins enable hermes-snow-search
# 重启 Hermes
```

## 配置

```yaml
plugins:
  hermes-snow-search:
    memory_limit_mb: 500          # 轻量源上限（sessions/facts/memory/skills）
    session_max: 7000
    fact_max: 10000
    deep_search_load_mode: startup  # off | startup | ondemand
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `memory_limit_mb` | 500 | 轻量源上限。深度搜索走 FTS5（数据库侧），不计入此上限 |
| `session_max` | 7000 | 轻量缓存最大 session 数 |
| `fact_max` | 10000 | 最大事实条目数 |
| `deep_search_load_mode` | `startup` | 深度搜索行为：`off`（关闭）、`startup`（启动预加载）、`ondemand`（首次查询懒加载） |

> `memory_limit_mb` 仅约束轻量源。深度搜索复用数据库已有的 FTS5 索引——零额外内存。

## 上下文清理（post_llm_call）

每次 LLM 回复后，`post_llm_call` 钩子会清空 snow_search 工具输出，防止搜索结果跨轮积累——一次搜索增加约 9K–18K 字符，但钩子在下一轮用户消息前将其清空。

> **注意：** 只清理 snow_search 的工具输出，不影响其他工具结果，也不影响搜索索引本身（下次调用仍可用）。

## 深度搜索

默认开启。直接查询数据库已有的 FTS5 索引——无需加载、无内存开销。结果含 `session_id`、`timestamp`、`role`、`snippet`、`search_info`。

### FTS5 路由

| 查询类型 | 表名 | 分词器 |
|---------|------|--------|
| 英文 / 混合 | `messages_fts` | unicode61（单词边界） |
| CJK ≥ 3 字 | `messages_fts_trigram` | trigram（三字滑窗） |
| CJK 1-2 字 | （LIKE 兜底） | 子串匹配 |

FTS5 表由 `hermes_state` 的触发器自动维护——每条消息增删改都同步更新索引。新消息到达无需 reload。

启动输出：

```
  ┊ ❄️ [Hermes Snow Search] Deep search ready (FTS5) | 222500 messages | 44 days (May 13 ~ Jun 26) | ~147 MB indexed on disk | 2.4s
```

### 排序模式

| `sort` | 效果 |
|--------|------|
| `relevance`（默认） | FTS5 rank（BM25）优先，来源优先级作 tiebreaker |
| `oldest` | 最早时间优先 — 回答"第一次" |
| `newest` | 最晚时间优先 — 回答"最近一次" |

### 性能

| 模式 | 搜索范围 | 延迟 | 内存 |
|------|----------|------|------|
| 轻量 | Session 摘要 | <1ms | ~3 MB |
| 深度（FTS5） | 完整消息正文 | 0.1–0.2s | ~0（数据库侧索引） |

启动：<3s（一次探针）。不建索引、不加载消息。此前是 ~125s（全量加载 + 内存倒排索引构建）+ ~147 MB 内存。

## 操作模式

说 **"snow reload"** 从磁盘重建索引，说 **"snow status"** 查看当前索引状态。工具描述引导 Agent 传入正确的 action 参数（`action=reload` 或 `action=status`）。

> **注意：** `snow reload` 重建的是 RAM 搜索索引（sessions、skills、facts、memory），不影响 LLM 上下文——上下文由 Hermes 系统 prompt 注入独立管理。

`action` 参数控制 `snow_search` 的行为：

| `action` | 行为 | 返回值 |
|----------|------|--------|
| `search`（默认） | 跨所有数据源搜索 | hits + search_info |
| `reload` | 清空并重新加载全部索引 | 完整状态 JSON |
| `status` | 返回当前索引状态（零 I/O） | 完整状态 JSON |

### Status / Reload 返回示例

```json
{
  "success": true,
  "action": "status",
  "counts": {"sessions": 263, "facts": 310, "memory": 64, "deep_messages": 222500, "skills": 105},
  "memory": {"current_mb": 0.2, "deep_mb": 0},
  "coverage": {"full_coverage": true, "date_range": "May 13 ~ Jun 26", "fts_mode": true},
  "ready": true,
  "deep_ready": true
}
```

## 技能缓存

`~/.hermes/skills/*/SKILL.md` 的 frontmatter 元数据在启动时预加载为第 5 个数据源（`stores_available` 中显示为 `"skills"`）。每条包含 `name`、`description`、`tags`、`category`（目录名）。默认开启，设置 `include_skills: false` 可跳过。

用 `snow_search` 发现可用技能。不要直接读取 SKILL.md 文件或 Hermes 核心工具描述。

## 全覆盖标记

查看 `search_info.full_coverage`——若为 `true`，snow_search 全覆盖。FTS5 模式下该值恒为 `true`（数据库索引覆盖所有消息）。

## 注意事项

- **启动：** <3s 探针 DB 统计。搜索 0.1–0.2s（FTS5）。
- **仅根会话：** 深度搜索过滤 `parent_session_id IS NULL`，子 agent session 排除。
- **不索引工具输出：** 仅 user/assistant 角色消息。
- **FTS5 依赖：** 深度搜索需要 SQLite FTS5 + trigram tokenizer（Python 3.11+ 自带）。不可用时回退到内存索引。

## 使用建议

- "最近/上次"类问题天然命中第一条（newest 排序 + FTS5 rank）
- "第一次"类问题用 sort="oldest"
- 关键词越具体越好
- 跨进程自动同步——FTS5 触发器保持索引实时，无需手动 reload
- 搜索覆盖全部数据，没找到就是没记录

## 作者

LinQuan & Snow (AI Girl)

## Star History

<a href="https://www.star-history.com/?repos=mlinquan%2Fhermes-snow-search&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=mlinquan/hermes-snow-search&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=mlinquan/hermes-snow-search&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=mlinquan/hermes-snow-search&type=date&legend=top-left" />
 </picture>
</a>
