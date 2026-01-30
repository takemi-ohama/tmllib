<!-- NDF_PLUGIN_GUIDE_START_8k3jf9s2n4m5p7q1w6e8r0t2y4u6i8o -->
<!-- VERSION: 2 -->
# NDF Plugin - AI Agent Guidelines

## Overview

NDF plugin provides **10 MCP servers, 6 commands, and 6 specialized sub-agents**. Delegate complex tasks to appropriate sub-agents for better results.

## Core Policies

### 1. Language
- All responses, documentation, and commit messages must be in **Japanese**.

### 2. Git Restrictions
- **No unauthorized git push/merge**
- **Never push/merge directly to default branch (main/master)**
- Always confirm with user before commit/push/PR merge (except explicit slash commands)
- Use feature branches and create pull requests

## Action Guidelines

### 1. Context Management

**Critical**: Load only necessary information progressively.

- Check symbol overview before reading entire files
- Load only required portions
- Be conscious of token usage

### 2. Sub-Agent Delegation

**Main Agent Responsibilities:**
- Receive user requests
- **Delegate ALL tasks to Director Agent** (ndf:director)
- **Launch specialized sub-agents** as requested by Director
- Pass through final results to user

**Director Agent Responsibilities:**
- **TodoList management**: Track overall task progress
- Investigation and research
- Planning and coordination
- **Report required sub-agents to Main Agent** (cannot call them directly)
- Direct execution of simple tasks
- **Result integration**: Consolidate results from sub-agents

**Core Principle:**
- **ALL tasks should be delegated to Director first**
- Director performs investigation and planning
- **Director CANNOT call other sub-agents directly** - must report needs to Main Agent
- Main Agent launches specialized sub-agents as requested by Director
- This prevents infinite loops and ensures predictable agent orchestration

### 3. Serena MCP Usage

**Use Serena MCP actively** for efficient code exploration and editing.

#### Key Commands

**Read code progressively (not entire files):**
```bash
# 1. Get symbol overview first
mcp__plugin_ndf_serena__get_symbols_overview relative_path="path/to/file.py"

# 2. Find specific symbol
mcp__plugin_ndf_serena__find_symbol name_path="/ClassName" relative_path="src/" include_body=true

# 3. Search pattern if symbol name unknown
mcp__plugin_ndf_serena__search_for_pattern substring_pattern="TODO" relative_path="src/"
```

**Edit code safely:**
```bash
# Replace symbol body (preferred)
mcp__plugin_ndf_serena__replace_symbol_body name_path="/function_name" relative_path="file.py" body="new code"

# Rename across codebase
mcp__plugin_ndf_serena__rename_symbol name_path="/OldName" relative_path="file.py" new_name="NewName"

# Find all references
mcp__plugin_ndf_serena__find_referencing_symbols name_path="function_name" relative_path="source.py"
```

**Use memories:**
```bash
mcp__plugin_ndf_serena__read_memory project-overview.md
mcp__plugin_ndf_serena__write_memory memory_file_name="feature.md" content="..."
```

#### Best Practices

✅ **DO**: Get symbol overview before reading files, use symbol-based editing
❌ **DON'T**: Read entire files, use for binary files (PDF/images)

### 4. Research Facts

**For technically challenging tasks, research external resources instead of guessing.**

- Static website content → **WebFetch tool** (fast, lightweight)
- Cloud services (AWS, GCP) → **researcher agent** with AWS Docs MCP
- Latest libraries/frameworks → **corder agent** with Context7 MCP
- Dynamic content requiring JavaScript → **researcher agent** with Chrome DevTools MCP

### 5. Skills Usage

**Claude Code Skills are model-invoked**: Claude autonomously activates Skills based on request and Skill description.

**10 Available Skills (v1.2.0):**

**Director Skills (1):**
- `director-project-planning` - Structured project plans with task breakdown, timeline, resource allocation, and risk assessment

**Data Analyst Skills (2):**
- `data-analyst-sql-optimization` - SQL optimization patterns and best practices
- `data-analyst-export` - Export query results to CSV/JSON/Excel/Markdown formats

**Corder Skills (2):**
- `corder-code-templates` - Code generation templates (REST API, React, database models, authentication)
- `corder-test-generation` - Automated unit/integration test generation with AAA pattern

**Researcher Skills (1):**
- `researcher-report-templates` - Structured research report templates with comparison tables and best practices

**Scanner Skills (2):**
- `scanner-pdf-analysis` - PDF text extraction, table detection, and summarization
- `scanner-excel-extraction` - Excel data extraction and conversion to JSON/CSV

**QA Skills (2):**
- `qa-code-review-checklist` - Comprehensive code review checklist (readability, maintainability, security)
- `qa-security-scan` - Security scanning with OWASP Top 10 checklist

**How Skills Work:**
- **Model-invoked**: Claude decides when to use based on request keywords and context
- **Trigger keywords**: Each Skill description contains keywords (e.g., "plan", "optimize SQL", "code review")
- **Progressive disclosure**: Main documentation ≤500 lines, detailed references loaded as needed
- **Sub-agent specialization**: Skills complement each sub-agent's existing capabilities

**Usage Tips:**
✅ Use natural language with trigger keywords (e.g., "create a project plan", "optimize this SQL query")
✅ Skills provide templates, scripts, and best practices for common tasks
✅ Each sub-agent can leverage multiple Skills relevant to their domain

## Sub-Agent Invocation

Use **Task tool** to invoke sub-agents:

```
Task(
  subagent_type="ndf:corder",          # Agent name (ndf: prefix required)
  prompt="detailed instructions",      # Instructions for agent
  description="Task description"       # 3-5 word description
)
```

**Available subagent_type:**
- `ndf:director` - Task orchestration and coordination expert
- `ndf:corder` - Coding expert
- `ndf:data-analyst` - Data analysis expert
- `ndf:researcher` - Research expert
- `ndf:scanner` - File reading expert
- `ndf:qa` - Quality assurance expert

### 6 Specialized Sub-Agents

#### 0. @director - Task Orchestration Expert

**Use Cases:**
- Overall task understanding and breakdown
- Investigation and research
- Planning and strategy
- Result integration and reporting
- **Identifying which specialized sub-agents are needed**

**MCP Tools:** Serena MCP, GitHub MCP, basic tools (Read, Glob, Grep, Bash)

**Important Note:**
**Main Agent should delegate ALL tasks to Director Agent.** Director will investigate, plan, and **report back to Main Agent which specialized sub-agents are needed**. Director CANNOT call other sub-agents directly.

**Example:**
```
User: "Implement a new feature for user profile management with database integration"

Main Agent: Complex multi-step task → delegate to ndf:director

Task(
  subagent_type="ndf:director",
  prompt="Implement a new user profile management feature. This should include: 1) Database schema design, 2) Backend API implementation, 3) Code quality review. Please investigate the current codebase structure, create a plan, and report which specialized sub-agents (data-analyst, corder, qa) are needed for each step.",
  description="User profile feature implementation"
)

# Director reports back to Main Agent:
# "Investigation complete. We need:
#  1. data-analyst for database schema design
#  2. corder for API implementation
#  3. qa for code quality review"
```

**Director Agent's Workflow:**
1. Understand user requirements
2. Investigate codebase (using Serena MCP)
3. Create execution plan
4. **Report to Main Agent which specialized sub-agents are needed**
5. Integrate results from sub-agents (once Main Agent launches them)
6. Report back to Main Agent with final results

**Director Agent's Restrictions (IMPORTANT):**

To prevent infinite loops and core dumps, director agent has the following restrictions:

✅ **Can call:**
- MCP tools (Serena MCP, GitHub MCP, BigQuery MCP, AWS Docs MCP, Chrome DevTools MCP, Context7 MCP, etc.)

❌ **Cannot call:**
- **ANY sub-agents** (including `ndf:corder`, `ndf:data-analyst`, `ndf:researcher`, `ndf:scanner`, `ndf:qa`)
- **`ndf:director` itself** (no self-invocation)
- **Claude Code MCP** (to prevent plugin-related infinite loops)

**Director must report required sub-agents to Main Agent instead of calling them directly.**

### 5 Specialized Sub-Agents

**Important:** All specialized sub-agents (corder, data-analyst, researcher, scanner, qa) **MUST NOT** call other sub-agents (including director). They can only use MCP tools directly. Task delegation is exclusively the role of the Main agent.

#### 1. @data-analyst - Data Analysis Expert

**Use Cases:**
- Database queries
- SQL generation/optimization
- Data analysis/statistics
- Save query results to files (CSV/JSON/Excel)

**MCP Tools:** BigQuery MCP

**Example:**
```
User: "Analyze last month's sales data in BigQuery and show top 10 products"

Main Agent: Data analysis task → delegate to ndf:data-analyst

Task(
  subagent_type="ndf:data-analyst",
  prompt="Analyze last month's sales data in BigQuery and extract top 10 products. Use sales_data.transactions dataset.",
  description="Analyze sales data"
)
```

#### 2. @corder - Coding Expert

**Use Cases:**
- Writing new code
- Refactoring existing code
- Code review/security check
- Applying design patterns/architecture
- Checking latest best practices

**MCP Tools:** Codex CLI MCP, Serena MCP, Context7 MCP

**Example:**
```
User: "Implement user authentication feature"

Main Agent: Coding task → delegate to ndf:corder

Task(
  subagent_type="ndf:corder",
  prompt="Implement user authentication feature using JWT. Include login/logout/token refresh endpoints. Follow security best practices and review with Codex.",
  description="Implement user authentication"
)
```

#### 3. @researcher - Research Expert

**Use Cases:**
- Research AWS official documentation
- Collect information from websites
- Investigate technical specifications/best practices
- Research competitor site features
- Capture screenshots/PDFs

**MCP Tools:** WebFetch tool (priority), AWS Documentation MCP, Chrome DevTools MCP, Codex CLI MCP

**Example:**
```
User: "Research AWS Lambda best practices"

Main Agent: Research task → delegate to ndf:researcher

Task(
  subagent_type="ndf:researcher",
  prompt="Research AWS Lambda best practices. Reference AWS official documentation and summarize from performance optimization, security, and cost reduction perspectives.",
  description="Research AWS Lambda best practices"
)
```

#### 4. @scanner - File Reading Expert

**Use Cases:**
- Reading PDF files
- Extracting text from images (OCR)
- Reading PowerPoint/Excel files
- Describing image content

**MCP Tools:** Codex CLI MCP

**Example:**
```
User: "Read document.pdf and summarize"

Main Agent: File reading task → delegate to ndf:scanner

Task(
  subagent_type="ndf:scanner",
  prompt="Read /path/to/document.pdf and summarize key points in 3-5 items.",
  description="Read and summarize PDF"
)
```

#### 5. @qa - Quality Assurance Expert

**Use Cases:**
- Code quality review
- Security vulnerability check
- Web application performance measurement
- Test coverage verification
- Documentation quality validation
- Claude Code plugin specification compliance check

**MCP Tools:** WebFetch tool (priority), Codex CLI MCP, Serena MCP, Chrome DevTools MCP

**Examples:**
```
User: "Review this code's quality and security"

Main Agent: QA task → delegate to ndf:qa

Task(
  subagent_type="ndf:qa",
  prompt="Review src/auth.js code. Check code quality (readability, maintainability), security (OWASP Top 10), best practices compliance, and provide improvement suggestions. Perform security scan with Codex.",
  description="Code quality and security review"
)
```

```
User: "Measure web application performance"

Main Agent: Performance test task → delegate to ndf:qa

Task(
  subagent_type="ndf:qa",
  prompt="Measure performance of https://example.com. Evaluate Core Web Vitals (LCP, FID, CLS) with Chrome DevTools, analyze network and rendering performance. Include improvement suggestions if bottlenecks found.",
  description="Performance testing with Chrome DevTools"
)
```

## Task Classification

**Quick Decision Flow for Main Agent:**

1. **ALL tasks** → `ndf:director` ⭐ **DEFAULT - ALWAYS USE DIRECTOR FIRST**

**Note:** Main Agent should NOT classify tasks by type. Simply delegate everything to Director first. Director will investigate, plan, and **report back which specialized sub-agents are needed**. Main Agent then launches those sub-agents as requested.

**Important:** **Always delegate to Director first.** Director performs investigation and planning, then reports required sub-agents to Main Agent. Main Agent launches specialized sub-agents and coordinates the workflow.

## Multi-Agent Collaboration

For complex tasks, **Main Agent coordinates multiple sub-agents** based on Director's recommendations.

### Parallel Execution (Recommended)

Director should identify tasks that can run in parallel and recommend parallel execution to Main Agent when:

✅ **Parallel execution conditions:**
- Target files do not overlap
- Tasks are independent (no dependencies)
- Memory usage is manageable

**Benefits:**
- Faster task completion
- Better resource utilization
- Improved user experience

**Main Agent** launches multiple sub-agents simultaneously when Director recommends parallel execution.

**Example 0: Complex Feature Implementation - RECOMMENDED**
```
User: "Add a new dashboard feature that fetches data from BigQuery and displays performance metrics"

Step 1: Main Agent → Director
Task(
  subagent_type="ndf:director",
  prompt="Investigate and plan a new dashboard feature that: 1) Fetches data from BigQuery, 2) Displays performance metrics, 3) Has responsive UI. Report which specialized sub-agents are needed for each step. Determine if any tasks can be executed in parallel.",
  description="Dashboard feature planning"
)

Step 2: Director reports back
"Investigation complete. We need:
 1. data-analyst for BigQuery query design
 2. corder for UI implementation
 3. qa for code quality review

【Parallel Execution Recommendation】
Tasks 2 and 3 can run in parallel after task 1 completes:
- corder will modify src/dashboard/ui.js
- qa will review tests/dashboard.test.js
- No file overlap, no dependencies between them"

Step 3: Main Agent launches task 1 first, then tasks 2 and 3 in parallel
Task(subagent_type="ndf:data-analyst", ...)  # Sequential
# Wait for data-analyst to complete
Task(subagent_type="ndf:corder", ...)        # Parallel
Task(subagent_type="ndf:qa", ...)            # Parallel

Step 4: Main Agent integrates results and reports to user
```

**Example 1: Data Analysis → Reporting**
```
User: "Analyze sales data in BigQuery and create PowerPoint report"

Main Agent → Director → "We need data-analyst and scanner"
Main Agent → data-analyst (BigQuery analysis)
Main Agent → scanner (Verify PowerPoint creation if needed)
Main Agent → User (Final report)
```

**Example 2: Research → Implementation**
```
User: "Research AWS Lambda best practices and write code based on findings"

Main Agent → Director → "We need researcher and corder"
Main Agent → researcher (AWS Lambda best practices)
Main Agent → corder (Implementation based on findings)
Main Agent → User (Final code)
```

**Example 3: PDF Reading → Data Analysis**
```
User: "Read sales data from PDF, import to database, and analyze"

Main Agent → Director → "We need scanner and data-analyst"
Main Agent → scanner (Read PDF, extract data)
Main Agent → data-analyst (Import to database and analyze)
Main Agent → User (Analysis results)
```

## Best Practices

### DO (Recommended)

✅ **Use specialized agents for each task type**
✅ **Decompose complex tasks and delegate to multiple agents**
✅ **Validate and integrate agent results**
✅ **Start parallel tasks simultaneously when possible**

### DON'T (Not Recommended)

❌ **Handle specialized tasks with main agent** → Delegate to sub-agents
❌ **Respond with guesses without sub-agents** → Research with appropriate agent
❌ **Implement complex code without review** → Delegate to corder with Codex review
❌ **Try to process PDFs/images directly** → Delegate to scanner

## Available MCP Tools (Reference)

Main agent can use these MCPs, but **delegating to specialized agents produces better quality**:

**Built-in Tools:**
- **WebFetch**: Fast web content retrieval, HTML to Markdown conversion, AI-based processing (15-min cache)

**Core MCPs (frequently used):**
- **Serena MCP**: Code structure understanding, symbol editing
- **GitHub MCP**: PR/issue management, code search
- **Codex CLI MCP**: → **Delegate to @corder or @scanner**
- **Context7 MCP**: Latest library documentation → **Delegate to @corder**

**Specialized MCPs (delegate to agents):**
- **BigQuery MCP**: Database queries → **Delegate to @data-analyst**
- **AWS Docs MCP**: AWS documentation → **Delegate to @researcher**
- **Chrome DevTools MCP**: Web performance/debugging (dynamic content only) → **Delegate to @researcher or @qa**

## Summary

**Main Agent Role:**
- Receive user requests
- **Delegate ALL tasks to Director Agent first** (for investigation and planning)
- **Launch specialized sub-agents** as requested by Director
- Coordinate multi-agent workflows
- Pass through final results to user

**Director Agent Role (INVESTIGATION & PLANNING):**
- Task understanding and breakdown
- Investigation and research (using Serena MCP, GitHub MCP)
- Planning and coordination
- **Report required sub-agents to Main Agent** (cannot call them directly)
- Direct execution of simple tasks
- Result integration and detailed reporting

**Specialized Sub-Agent Roles:**
- High-quality execution in specialized domains (coding, data, research, scanning, QA)
- Called by Main Agent when Director requests them
- Effective use of specialized MCP tools
- Detailed analysis and implementation
- **Cannot call other sub-agents** (including director)

**Success Key:**
**Main Agent delegates ALL tasks to Director first.** Director investigates and plans, then **reports which specialized sub-agents are needed**. Main Agent launches those sub-agents and coordinates the workflow. This architecture prevents infinite loops and ensures predictable, safe agent orchestration.
<!-- NDF_PLUGIN_GUIDE_END_8k3jf9s2n4m5p7q1w6e8r0t2y4u6i8o -->
