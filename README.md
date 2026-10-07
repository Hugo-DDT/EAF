# Enterprise AI Fabric (EAF)

[Chinese](README_CN.md)· English

Enterprise AI Fabric (EAF) is an Agent platform for team workflows. It brings Agent capabilities, knowledge and experience, persistent tasks, workflows, human collaboration, and controlled business integrations into one runtime.

EAF is designed for more than generating model responses. It also addresses how a task receives authorized context, how execution state is persisted, when a person must take over, how business writes are approved and verified, and how reviewed experience can be reused safely.

## Why EAF

Once teams introduce language models, the hard part is often more than generating an answer. Context is spread across knowledge sources and business systems; execution needs durable state; and permissions, human handoffs, and external writes are often handled separately. That makes it difficult to move an agent capability from an experiment into a controlled day-to-day workflow. EAF brings these parts together. Workspace isolation, source authorization, persistent tasks, workflows, Policy, Approval, and Execution let agents work within explicit boundaries and leave inspectable execution records.

EAF also treats evaluation and experience reuse as platform concerns. Teams can first check behavior against isolated synthetic scenarios, then review and publish reusable experience through explicit controls. This provides a path to connect knowledge, collaboration, and business actions while keeping permissions, versions, and accountability visible.

## Core capabilities

### Agent capabilities and resources

- Manage versioned Agent, Capability, Skill, Prompt, and Tool assets.
- Discover available capabilities by category and bind tasks to explicit asset versions.
- Expose bounded capability discovery and task entry points through REST and MCP.
- Export and import declarative capability packages. Imported packages remain read-only resources; they do not execute embedded code or publish themselves as active capabilities.

### Workspaces, knowledge, and experience

- Isolate users, tasks, knowledge, and resource access by Workspace.
- Build context from authorized Knowledge and Memory sources while retaining source references and version information.
- Recheck source authorization and currentness before use. A revoked or stale source is not treated as continuing authorization just because a previous snapshot exists.
- Keep formal knowledge, personal experience, team experience, and unpublished improvement candidates separate so that experience is not presented as factual evidence.

### Persistent tasks and Agent Runtime

- Run Agent work as asynchronous Tasks with persisted status, results, usage, and execution records.
- Control admission and execution with bounded queues, concurrency slots, and Workspace-level scheduling limits.
- Coordinate Task dispatch and shared Provider concurrency across JVM instances using PostgreSQL state.
- Support controlled cancellation, stop markers, and bounded recovery.
- Run service-request batches through fixed read-only parallel branches and deterministic aggregation, with explicit limits on scope and concurrency.

### Workflows and human collaboration

- Orchestrate preparation, human handling, result summarization, and follow-up actions through explicit Workflows.
- Persist human work items, assignees, handoff data, and submitted results as workflow state.
- Define clear human review and confirmation boundaries. An Agent suggestion does not grant approval or write access.

### Policy enforcement and business integration

- Control business-tool calls through Policy, Approval, Credential, and Execution.
- Policy denials take precedence over model suggestions. Write operations that require approval must satisfy authorization, self-approval restrictions, and execution budgets.
- Use idempotent operation identities and read-back verification for external writes. When the result is uncertain, query using the original operation identity instead of blindly creating a new write.
- External connectors such as CRM and service desks are disabled by default. Local examples use synthetic data and loopback fixtures.

### Evaluation and controlled improvement

- Evaluate fixed scenarios with versioned synthetic datasets and isolate expected answers from ordinary task context.
- Produce reproducible, structured evaluation reports with optional human review.
- Require independent review, explicit human approval, and Owner publication for improvement candidates. Candidates do not automatically become active Prompts, Skills, or Agents.
- Evaluation scores and synthetic checks do not establish real Provider quality or business impact.

## Example workflow

Customer follow-up is one local example that shows how platform capabilities work together:

1. A user creates an analysis task in a Workspace.
2. Runtime reads knowledge and experience allowed by current authorization, then binds the actual sources and versions to the task.
3. An Agent produces a cited recommendation. If more information is needed, it can request only bounded retrieval within the configured scope.
4. After user confirmation, a Workflow can create and assign a team follow-up.
5. A person submits the handling result through a separate step. Any external CRM write requires its own authorization and approval.
6. Reviewed results can support follow-up analysis or be curated into controlled team experience.

This sample illustrates general platform mechanisms. EAF is also intended for other team tasks, knowledge work, cross-role collaboration, and business workflows.

## Architecture overview

EAF uses a Java modular monolith. Each module owns its data and collaborates through public APIs, ports, or events. Business modules do not write directly to another module's tables.

| Area | Main modules | Responsibility |
| --- | --- | --- |
| Identity and workspaces | `organization`, `identity`, `workspace`, `provisioning` | Organizations, identity, membership, and Workspace isolation |
| Agent resources | `agent`, `capability`, `skill`, `prompt`, `tool` | Versioned capability assets and tool definitions |
| Context and experience | `knowledge`, `context`, `memory` | Knowledge retrieval, context authorization, personal and team experience |
| Tasks and execution | `task`, `agent-runtime`, `workflow`, `execution` | Persistent tasks, Runtime orchestration, business workflows, and actions |
| Governance and integration | `policy`, `approval`, `credential`, `secret`, `connector`, `integration`, `agent-protocol` | Authorization decisions, approvals, credential boundaries, and external protocols |
| Quality and operations | `evaluation`, `learning`, `audit`, `usage`, `observability` | Scenario evaluation, controlled improvement, audit, usage, and observability |
| Application assembly | `bootstrap` | Spring Boot startup, configuration, and runtime wiring |

Model requests are owned by the `model` module. Knowledge, Memory, and unpublished candidates remain separate; business-tool calls go through Execution.

## Quick start

### Requirements

- JDK 21
- Docker Desktop or Docker Engine with Docker Compose enabled
- Windows PowerShell for the commands below, which match the current repository setup

### Start the application

From the repository root:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
# Edit .env and set a non-empty local value for EAF_DB_PASSWORD

docker compose -f compose.yml up -d
.\mvnw.cmd -pl bootstrap spring-boot:run -Dspring-boot.run.profiles=local
```

Docker Compose starts PostgreSQL only; Maven Wrapper runs the Spring Boot application on the host. By default, the application listens on `127.0.0.1:18080` and PostgreSQL is mapped to `127.0.0.1:15432`.

- Health check: `http://127.0.0.1:18080/actuator/health`
- Local pages: `http://127.0.0.1:18080/demo/`
- REST API prefix: `/api/v1`

The local API uses a configured Bearer identity. Tokens entered in demo pages are kept in the current page's memory. After stopping the application, stop PostgreSQL with the following command; the named volume is preserved:

```powershell
docker compose -f compose.yml down
```

Removing the database volume permanently deletes local development data. Use `docker compose -f compose.yml down -v` only when you intend to reset it.

## Model and credential configuration

The default model mode is `deterministic`, so building and starting locally does not require a model API key. Before configuring a real Provider:

1. Use synthetic data and confirm the authorized outbound data scope.
2. Set Provider credentials in the local `.env` file or process environment. Do not put keys in command-line arguments, task content, logs, or the repository.
3. Explicitly select the model mode and provide valid authorization, a spend cap, and stop conditions for that run.

`.env.example` lists configuration variables without valid credentials. Real Provider requests and writes to external systems are not part of the default startup path.

## Build and verification

Run the full Maven Wrapper verification with:

```powershell
.\mvnw.cmd -B -ntp verify
```

Integration tests use Docker to start an isolated PostgreSQL environment. Default checks do not require a real model service; Provider tests are enabled by configuration. Run module-level checks when appropriate, and do not interpret synthetic results as proof of real model quality, production capacity, or business impact.

## Current boundaries

- EAF is a locally runnable Agent platform implementation; this README does not claim enterprise production deployment or production service guarantees.
- Multi-instance scheduling and queue checks use local synthetic load. They do not prove multi-machine capacity or the same number of concurrent real Provider calls.
- Provider quality, model semantics, external CRM/service-desk behavior, and business impact each require their own authorization and verification.
- External connectors are disabled by default. Capability-package import does not execute arbitrary code or bypass identity, Policy, Approval, or Execution controls.

