# Herme Snow Search（雪搜）

> [![GitHub](https://img.shields.io/badge/GitHub-mlinquan%2Fhermes--snow--search-blue?logo=github)](https://github.com/mlinquan/hermes-snow-search)
> [English](README.md) | 中文版

Hermes Agent 的内存级并行搜索插件。
启动时将聊天记录、事实存储（fact_store）和内置记忆（MEMORY.md / USER.md）全量加载到 RAM 中，
搜索时三路并发，毫秒级返回。

## 工作原理

1. **启动即加载** — Hermes 启动后后台线程自动加载，不阻塞 CLI
2. **全程驻内存** — sessions、facts、memory 三份数据存在 Python 列表中，搜索不走 IO
3. **并行搜索** — `ThreadPoolExecutor` 三路同时检索，按相关性分数合并排序
4. **增量更新** — `post_tool_call` 钩子捕获 `fact_store add` 和 `memory add` 操作，追加到缓存
5. **自动淘汰** — `pre_llm_call` 钩子检查内存使用，超过 80% 上限时淘汰最旧/最低信任度的条目

> **记忆提供方说明：** 当前支持 Hermes 内置的 holographic 记忆（fact_store）。其他记忆提供方（mem0、supermemory、Honcho 等）的支持已在规划中。

## 性能对比

| | 之前 | 现在 |
|--|------|------|
| 查询方式 | 3 次串行查库 | 1 次并发 RAM 搜索 |
| 典型耗时 | ~350–700ms | **<0.5ms** |

数据启动即驻内存，不走磁盘，不串行等待。三路并发一次出结果。

## 启动

静默后台加载，无终端输出。首次调用 `snow_search` 前数据已就绪。

> **内存说明：** 每个 Hermes 实例各自加载一份数据。CLI 和 Gateway 是独立进程，
> 峰值内存可能达到 `memory_limit_mb` 配置值的 2 倍（如设 20 MB 则最多 ~40 MB）。

首次使用时终端显示：

```
preparing snow_search…
  ┊ ⚡ snow_sear <关键词>  0.0s
  ┊ 🔍 recall    "<关键词>"  0.3s
```

成功标志：
- 出现 `preparing snow_search…` 说明工具已注册就绪
- 响应毫秒级返回（RAM 速度）

## 使用方法

`snow_search` 是 AI 侧工具。你跟 Hermes Agent 聊历史——"查一下之前聊过什么"或"我记得…"——它会自动调用。

你也可以明确说：`snow_search query="数据库迁移"`

首次使用时终端显示：

```
preparing snow_search…
  ┊ ⚡ snow_sear <关键词>  0.0s
  ┊ 🔍 recall    "<关键词>"  0.3s
```

结果跟在同一条回复里返回，不需要额外操作。

> **提示：** snow_search 够快（<0.5ms），AI 可能会本能地用 session_search、sqlite3 等慢工具补查。
> 建议在角色定义文件（如 `SOUL.md`、`MEMORY.md` 或 `config.yaml` 的 `agent.personalities`）中添加规则：
> _snow_search 结果就是最终结果，信它，不补查。_

## 安装

```bash
# 1. 在 Hermes venv 中以可编辑模式安装（一次即可）
pip3 install -e ~/works/hermes-snow-search

# 2. 启用插件
hermes plugins enable hermes-snow-search

# 3. 重启 Hermes 会话（/new 或重开）
```

## 配置

在 `~/.hermes/config.yaml` 中：

```yaml
plugins:
  enabled:
    - hermes-bus-plugin
    - hermes-snow-search
  hermes-snow-search:
    memory_limit_mb: 20
    session_max: 7000
    fact_max: 10000
    memory_max: 100
```

| 配置项 | 默认值 | 说明 | 能存多久 |
|--------|--------|------|---------|
| `memory_limit_mb` | 20 | 内存硬上限，达到 80% 触发淘汰 | 6 个月以上（日均 35 个 session） |
| `session_max` | 7000 | 最多缓存的会话数 | 6 个月以上（~200 天，日均 35 个 session） |
| `fact_max` | 10000 | 最多缓存的事实条目数 | session 淘汰前基本不会触发 |
| `memory_max` | 100 | 最多缓存的记忆条目数 | ~<1 KB，不是瓶颈 |

两个主指标平衡在 ~6 个月使用量，对应日均 ~10亿 token（重度使用，~35 个 session/天）。
总增长 ~86 KB/day，`session_max`（7000 条）和 `memory_limit_mb`（20 MB）
大致同时到期，session 略紧一些，设计上它就是淘汰瓶颈。半年数据约 ~13 MB。

### 推荐配置档位（更长留存）

| 档位 | 留存 | `session_max` | `memory_limit_mb` | 搜索速度 |
|------|------|---------------|-------------------|---------|
| 默认 | 6个月 | 7,000 | 20 | <1ms |
| 一年 | 1年 | 13,000 | 35 | <1ms |
| 两年 | 2年 | 26,000 | 70 | ~1ms |
| 全量 | 不清 | 50,000 | 150 | ~2ms |

RAM 搜索线性扩展——数据翻 10 倍搜索速度约降 5 倍，但 2 年以上的量也才 ~2ms。
速度不是瓶颈，按你想要的留存时间调就行。
