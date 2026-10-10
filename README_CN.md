# Enterprise AI Fabric (EAF)

[English](README.md) · 中文

## 中文

Enterprise AI Fabric（EAF）是一个面向团队工作场景的 Agent 平台。它把 Agent 能力、知识与经验、持久任务、工作流、人工协作和受控业务集成放在一个统一的运行平台中。

EAF 关注的不只是让模型生成回答，还包括：任务如何获得授权上下文、执行过程如何持久化、何时需要人工处理、业务写入如何审批与核验，以及经过确认的经验如何安全复用。

## 为什么需要 EAF

团队引入模型后，常见难点不止是生成答案：上下文分散在知识库和业务系统中，任务执行缺少持久状态，权限、人工接手和外部写入又往往各自处理，导致能力难以从试验走到可控的日常工作流。EAF 将这些环节放进同一平台，通过 Workspace 隔离、来源授权、持久任务、Workflow、Policy、Approval 和 Execution，让 Agent 在明确边界内完成工作，并留下可检查的执行记录。

EAF 也把评估与经验复用纳入平台设计：先用隔离的合成场景检查行为，再由人审核和发布可复用经验。这样，团队可以逐步连接知识、协作和业务动作，同时保留权限、版本和责任边界。

## 核心能力

### Agent 能力与资源管理

- 管理有版本的 Agent、Capability、Skill、Prompt 和 Tool 资产。
- 按类别发现可用能力，并在任务中绑定明确的资产版本。
- 通过 REST 与 MCP 暴露受限的能力查询和任务入口。
- 支持声明式能力包的导出和受限导入；导入的包作为只读资源保存，不会自动执行其中的代码或发布为可用能力。

### 工作空间、知识与经验

- 按 Workspace 隔离用户、任务、知识和资源访问。
- 从已授权的 Knowledge 与 Memory 来源构建上下文，并保留来源引用和版本信息。
- 在读取或使用前检查来源权限与当前性；来源被撤回或失效时，不把历史快照当成仍可用的授权。
- 将正式知识、个人经验、团队经验和未发布的改进候选分别管理，避免把经验建议当成事实依据。

### 持久任务与 Agent Runtime

- 将 Agent 工作作为异步 Task 执行，保留状态、结果、调用用量和执行记录。
- 使用有界队列、并发槽位和 Workspace 级调度限制控制任务接纳与执行。
- 使用共享 PostgreSQL 状态协调多个 JVM 实例的 Task 分派与 Provider 并发配额。
- 支持受控取消、停止标记和有界恢复流程。
- 为服务请求批次提供固定的只读并行分支与确定性聚合，限制单次工作的范围和并发量。

### 工作流与人工协作

- 使用显式 Workflow 编排准备、人工处理、结果整理和后续动作。
- 将人工工作项、负责人、交接信息和提交结果作为持久状态保存。
- 对需要确认的输入和结果设置明确的人机边界；Agent 不会因为生成了建议就自动获得审批或写入权限。

### 策略控制与业务集成

- 通过 Policy、Approval、Credential 和 Execution 控制业务工具调用。
- Policy 拒绝优先于模型建议；需要审批的写操作必须满足权限、自批限制和执行预算。
- 对外部写入使用幂等操作标识与回读核验。结果不确定时先按原操作标识查询，不盲目创建新的写入。
- CRM、服务台等外部 Connector 默认关闭；本地样例使用合成数据和 loopback fixture。
- 连接团队工作流与业务系统读取、审批后写入、项目简报和人员交接。外部 Connector 默认关闭，本地样例使用合成数据和 loopback fixture。
- 支持有界的工作摘要与订阅，并将结果作为普通任务提供；支持显式发现团队经验，以及经审核、发布和采用的能力改进。
- 将模型选择与任务执行和用量记录绑定，并通过授权资源公开能力声明，供现有任务入口消费。本地检查不证明真实 Provider 质量、成本节省、第三方兼容性或业务收益。

### 评估与受控改进

- 使用版本化合成数据评估固定场景，并将样本答案与普通任务上下文隔离。
- 生成可复现的结构化评估报告，可附加人工复核结果。
- 改进候选必须经过独立审核、明确的人类批准和 Owner 发布；候选不会自动变成运行中的 Prompt、Skill 或 Agent。
- 评估分数和合成检查不等于真实 Provider 质量或业务收益证明。

## 示例工作流

客户跟进是 EAF 中的一个本地样例，用来展示平台能力如何配合：

1. 用户在指定 Workspace 中创建分析任务。
2. Runtime 按当前权限读取知识和经验，并把实际使用的来源与版本绑定到任务。
3. Agent 生成带引用的建议；需要补充资料时，只能在既定范围内发起有限查询。
4. 用户确认后，Workflow 可创建团队跟进并分派给负责人。
5. 人工处理结果通过独立入口提交；任何外部 CRM 写入都需要单独授权和审批。
6. 经确认的结果可以用于复盘或整理为可控的团队经验。

这是一个展示通用平台机制的样例业务。EAF 的目标也覆盖其他团队任务、知识工作、跨角色协作和业务流程。

## 架构概览

EAF 采用 Java 模块化单体。每个模块拥有自己的数据，通过公开 API、端口或事件协作；业务模块不直接写入其他模块的数据表。

| 模块区域 | 主要模块 | 职责 |
| --- | --- | --- |
| 身份与空间 | `organization`, `identity`, `workspace`, `provisioning` | 组织、身份、成员关系和 Workspace 隔离 |
| Agent 资源 | `agent`, `capability`, `skill`, `prompt`, `tool` | 版本化能力资产与工具定义 |
| 上下文与经验 | `knowledge`, `context`, `memory` | 知识检索、上下文授权、个人与团队经验 |
| 任务与执行 | `task`, `agent-runtime`, `workflow`, `execution` | 持久任务、Runtime 编排、业务流程和动作执行 |
| 治理与集成 | `policy`, `approval`, `credential`, `secret`, `connector`, `integration`, `agent-protocol` | 权限决策、审批、凭据边界和外部协议 |
| 质量与运营 | `evaluation`, `learning`, `audit`, `usage`, `observability` | 场景评估、受控改进、审计、用量和运行观测 |
| 应用装配 | `bootstrap` | Spring Boot 启动、配置与运行时装配 |

模型请求由 `model` 模块统一管理。Knowledge、Memory 和未发布候选保持独立；业务工具调用统一经过 Execution。

## 本地快速开始

### 环境要求

- JDK 21
- Docker Desktop 或 Docker Engine，启用 Docker Compose
- Windows PowerShell（以下命令按仓库当前开发环境编写）

### 启动

在仓库根目录运行：

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
# 编辑 .env，为 EAF_DB_PASSWORD 设置非空的本地密码

docker compose -f compose.yml up -d
.\mvnw.cmd -pl bootstrap spring-boot:run -Dspring-boot.run.profiles=local
```

Docker Compose 只启动 PostgreSQL；Spring Boot 应用由 Maven Wrapper 在本机运行。应用默认监听 `127.0.0.1:18080`，数据库映射到 `127.0.0.1:15432`。

- 健康检查：`http://127.0.0.1:18080/actuator/health`
- 本地页面：`http://127.0.0.1:18080/demo/`
- REST API 前缀：`/api/v1`

本地 API 使用配置的 Bearer 身份。演示页面输入的令牌仅保存在当前页面内存中。关闭应用后，可运行以下命令停止数据库；命名卷会保留：

```powershell
docker compose -f compose.yml down
```

删除数据库卷会永久清除本地开发数据。仅在确定要重置数据时才使用 `docker compose -f compose.yml down -v`。

## 模型与凭据配置

默认模型模式是 `deterministic`，无需模型 API Key 即可构建和启动本地项目。配置真实 Provider 前：

1. 使用合成数据，并确认出站数据范围。
2. 在本机 `.env` 或进程环境中设置 Provider 凭据；不要把 Key 放进命令行、任务正文、日志或仓库。
3. 显式切换模型模式并提供覆盖本次运行的有效授权、费用上限和停止条件。

`.env.example` 提供配置项模板，不包含有效凭据。真实 Provider 请求和真实外部系统写入不是默认启动路径。

## 构建与验证

完整验证使用 Maven Wrapper：

```powershell
.\mvnw.cmd -B -ntp verify
```

集成测试会使用 Docker 启动隔离的 PostgreSQL 环境。默认检查不需要真实模型服务；涉及 Provider 的测试按配置启用。可根据变更范围运行相关模块或测试，避免把合成数据结果解释为真实模型质量、生产容量或业务收益。

## 当前边界

- 项目提供的是可本地运行的 Agent 平台实现，不代表已完成企业生产部署或生产级服务承诺。
- 多实例调度与队列检查使用本地合成负载；不代表多机器容量，也不代表同等数量的真实 Provider 并发请求。
- Provider、模型语义质量、外部 CRM/服务台和业务效率收益，需要各自独立的真实授权与验证。
- 外部连接默认关闭；能力包导入不执行任意代码，也不绕过现有身份、Policy、Approval 或 Execution 控制。

## 许可证

本项目采用 Apache License 2.0，详见 [LICENSE](LICENSE)。
