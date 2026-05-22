# Hermes Snow Search

<p align="center"><img src="assets/avator_default_png8.png" width="500" alt="Snow"></p>

> [English](README.md) | 中文版

Hermes Agent 的内存级并行搜索插件。全量加载到 RAM，多路并发，毫秒级返回。默认开启深度搜索，自动搜完整消息正文。支持热重载、状态查看、技能元数据缓存。

## 核心优势

| # | 优势 | 说明 |
|---|------|------|
| 1 | **毫秒级** | 驻内存搜索，不走磁盘不走 SQLite，结果 <1ms |
| 2 | **5 源并行** | sessions + holographic facts + built-in memory + skill metadata + 全量消息正文，ThreadPoolExecutor 多路并发 |
| 3 | **深度搜索** | 完整消息正文索引（含 session_id、timestamp、role），覆盖 12K+ 条历史消息 |
| 4 | **自动清理上下文** | post_llm_call 钩子清空搜索结果。实测 107 条/34K 字符 → 下一轮只剩 ~7K 固定负载 |
| 5 | **跨会话** | 不限于当前对话，一次搜索覆盖所有历史 session |
| 6 | **热重载** | snow reload 从磁盘重建 RAM 索引，无需重启 Hermes |
| 7 | **零 I/O 查看** | snow status 秒出完整索引快照 |
| 8 | **增量更新** | fact_store/memory 写入即时追加缓存，无需完整重载 |
| 9 | **自动淘汰** | 超 80% 内存上限时自动清除最旧/低信任条目 |
| 10 | **全覆盖保证** | full_coverage=true 时深度搜索覆盖每条已存消息 |

## 工作原理

1. **启动加载** — 后台线程自动加载 sessions、facts、memory、skills 元数据
2. **全程驻内存** — 数据在 Python 列表中，搜索不走磁盘
3. **并行搜索** — ThreadPoolExecutor 多路并发
4. **增量更新** — post_tool_call 钩子捕获写入，追加缓存
5. **自动淘汰** — 超 80% 内存上限时淘汰最旧条目
6. **深度搜索** — 完整消息正文索引，含 session_id + timestamp + role，增量刷新
7. **技能缓存** — `~/.hermes/skills/*/SKILL.md` frontmatter 启动时预加载

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
    memory_limit_mb: 200          # 安全上限，非实际开销
    session_max: 7000
    fact_max: 10000
    deep_search_enabled: true     # 设为 false 仅用轻量模式
    deep_search_load_mode: "ondemand"   # "ondemand" | "startup"
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `memory_limit_mb` | 200 | 内存硬上限，达 80% 触发淘汰 |
| `session_max` | 7000 | 轻量缓存最大 session 数 |
| `fact_max` | 10000 | 最大事实条目数 |
| `deep_search_enabled` | true | 开启完整消息正文搜索。设为 false 仅用轻量模式 |
| `deep_search_load_mode` | ondemand | ondemand = 首次搜索时加载，startup = 启动时后台加载 |

> 200 MB 是安全上限，不是实际开销。一周真实对话（~230 session、~10,000 条消息）轻量数据约 6 MB，深度搜索另需约 6 MB。

### 内存建议

- **轻量模式：** 20 MB 足够存放 session 摘要、facts、memory、skills。设置 `deep_search_enabled: false` 即可使用轻量模式。
- **深度搜索（默认）：** 完整消息正文约 6 MB/周。200 MB 覆盖约 6 个月，500 MB 覆盖约 1 年。
- **多 profile：** 运行多个 Hermes profile 时，预算 N × `memory_limit_mb`，因为每个进程持有独立的内存索引。

## 上下文清理（post_llm_call）

每次 LLM 回复后，`on_post_llm_call` 钩子会清空 snow_search 工具输出，防止搜索结果跨轮积累。一次搜索增加约 9K–18K 字符，但钩子在下一轮用户消息前将其清空。

**实测验证：** 两次深度搜索（107 条命中，合计约 34K 字符）注入上下文。LLM 回复后，`post_llm_call` 清理所有搜索输出——下一轮只保留固定约 7K 字符的 memory + user profile。

```python
# 钩子逻辑
for msg in history:
    if msg.get("role") == "tool" and msg.get("name") == "snow_search":
        msg["content"] = ""  # 从上下文清除
```

> **注意：** 此钩子只清理 snow_search 的工具输出，不影响其他工具结果，也不影响 RAM 中的搜索索引本身。搜索索引在下次调用时仍然可用。

## 深度搜索

默认开启。激活后自动搜索完整消息正文替代轻量摘要。结果含 `session_id`、`timestamp`、`role`、`search_info`。

### 加载模式

| 模式 | 触发 | 表现 |
|------|------|------|
| `ondemand` | 首次搜索 | 阻塞加载，显示进度 |
| `startup` | 后台 2.5 秒 | 不阻塞，打印 ~0/50/100% 进度 |

```
[Hermes Snow Search] Loading deep search index...
[Hermes Snow Search] Session 65/263 | 3,000 messages | 15/200 MB | ~0.5s remaining
[Hermes Snow Search] Deep search ready | 12,000 messages | 10 days (May 13 ~ May 22) | 7.5 MB
```

从最新 session 反向加载，到 85% 内存上限停止。后续调用增量刷新，跨进程自动同步。

### 排序模式

| `sort` | 效果 |
|--------|------|
| `relevance` | 最佳匹配优先（相关度 + 近期加分） |
| `oldest` | 最早时间优先 — 回答"第一次" |
| `newest` | 最晚时间优先 — 回答"最近一次" |

### 性能

| 模式 | 搜索范围 | 延迟 | 内存（周数据） |
|------|----------|------|---------------|
| 轻量 | Session 摘要 | <0.5ms | ~3 MB |
| 深度 | 完整消息正文 | ~1-5ms | ~6 MB |

轻量与深度互斥——深度模式跳过 session 摘要，仅加载 facts + memory + messages。

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
  "counts": {"sessions": 263, "facts": 310, "memory": 64, "deep_messages": 12000, "skills": 105},
  "memory": {"current_mb": 0.2, "deep_mb": 7.5},
  "coverage": {"full_coverage": true, "date_range": "May 13 ~ May 22"},
  "ready": true,
  "deep_ready": true
}
```

## 技能缓存

`~/.hermes/skills/*/SKILL.md` 的 frontmatter 元数据在启动时预加载为第 5 个数据源（`stores_available` 中显示为 `"skills"`）。每条包含 `name`、`description`、`tags`、`category`（目录名）。默认开启，设置 `include_skills: false` 可跳过。

用 `snow_search` 发现可用技能。不要直接读取 SKILL.md 文件或 Hermes 核心工具描述。

## 全覆盖标记

查看 `search_info.full_coverage`——若为 `true`，snow_search 全覆盖。若为 `false`，`session_search` 可能仍需用于较早的 session。

## 注意事项

- **首次延迟：** 首次深度搜索触发索引构建（~1 秒/周数据）。
- **仅根会话：** 只索引顶层对话，子 agent 排除。
- **不索引工具输出：** 仅 user/assistant 角色消息。
- **部分覆盖：** 当 `full_coverage` 为 false 时，结合 `session_search` 获取完整结果。

## 使用建议

- "最近/上次"类问题天然命中第一条
- "第一次"类问题用 sort="oldest"
- 关键词越具体越好
- 跨进程自动同步，无需手动 reload
- 搜索覆盖全部内存数据，没找到就是没记录

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
