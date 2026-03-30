# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

askFSDL 是一个基于 RAG（检索增强生成）的问答系统，使用 LangChain + OpenAI GPT-4 + FAISS 向量搜索，通过 Discord Bot 对外提供服务，部署在 Modal（无服务器平台）上。语料来源包括 PDF 论文、Markdown 讲义和视频字幕。

## 环境与依赖

- Python 3.10，依赖见 `requirements.txt`（主要）和 `requirements-dev.txt`（开发）
- 所有密钥配置在 `.env`（参考 `.env.example`），通过 Modal secrets 注入

## 常用命令

```bash
# 安装依赖
make dev-environment

# Modal 认证
make modal-auth

# 数据管道（首次或重建语料时运行）
make document-store   # ETL → MongoDB
make vector-index     # 从 MongoDB 构建 FAISS 索引

# 部署
make backend          # 部署 Q&A 服务
make frontend         # 部署 Discord bot
make slash-command    # 向 Discord 注册 /ask 指令

# 本地开发
make serve-backend    # 本地运行 FastAPI + Gradio
make serve-frontend   # 本地运行 Discord bot

# 测试查询
make cli-query QUERY="What is chain-of-thought prompting?"

# 全量部署
make it-all

# 代码检查
ruff check .
black --check .
pre-commit run --all-files
```

## 架构

### 数据流

```
ETL（etl/）→ MongoDB（docstore.py）→ FAISS 向量索引（vecstore.py）
                                              ↓
用户提问 → 向量检索 Top-3 文档 → LangChain stuff chain → GPT-4 → 带引用的答案
```

### 核心模块

- **app.py** — 主后端：`qanda()` 函数实现完整 RAG 链，同时暴露 FastAPI endpoint 和 Gradio UI；向量索引通过 Modal Network File System 持久化
- **bot.py** — Discord bot：验证 Discord 签名（pynacl），立即 defer 响应，异步调用 app.py 的 `qanda()`，再通过 webhook 回复
- **prompts.py** — LangChain PromptTemplate，含 few-shot 示例，控制答案格式和引用风格
- **docstore.py** — MongoDB 工具函数（连接、查询）
- **vecstore.py** — FAISS 索引的创建与加载
- **etl/** — 四个 ETL 模块：`shared.py`（公共逻辑）、`pdfs.py`（arxiv PDF）、`markdown.py`（讲义）、`videos.py`（视频字幕）

### 部署模型

每个模块（app.py、bot.py、各 etl/*.py）都是独立的 Modal stub，通过 `@stub.function()` 装饰器声明无服务器函数。bot.py 通过 Modal stub lookup 异步调用 app.py 中的函数，实现跨 stub 调用。

## 关键技术细节

- **LangChain**: `load_qa_with_sources_chain` 使用 "stuff" 方式（直接拼接文档到 prompt）
- **Embedding**: `text-embedding-ada-002`，维度 1536
- **向量搜索**: FAISS，检索 Top-3
- **LLM**: GPT-4，`temperature=0`，`max_tokens=256`
- **文档分块**: `RecursiveCharacterTextSplitter`，500 字符，100 字符重叠
- **MongoDB 批量写入**: 每批 250 条
- **监控**: 可选接入 Gantry（`GANTRY_API_KEY`）
