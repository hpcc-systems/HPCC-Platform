# Using `/rubber-duck` in Copilot: Get a Second Opinion from an Independent Agent

Most developers have heard of the classic "rubber duck debugging" technique: Explain the problem out loud to a rubber duck and, somehow, the solution often reveals itself.

It's true. The act of vocalizing a problem to someone (or something) forces you to slow down, clarify your thinking, and identify gaps in your understanding.

I've tried a modern variation by talking through problems with my cat. The results have been mixed. While she's an excellent listener, her troubleshooting advice typically involves walking across the keyboard, closing terminals, or demanding food halfway through the explanation.

Fortunately, GitHub Copilot's `/rubber-duck` offers a much more productive way to challenge your thinking.

## What Is `/rubber-duck`?

`/rubber-duck` is a slash command that invokes the Rubber Duck agent to provide an independent second opinion.

`/rubber-duck` acts like an independent reviewer, examining your ideas, plans, and implementations from a fresh perspective. It is designed to challenge your assumptions and surface risks, edge cases, and alternative approaches.

This can help you:

* Uncover hidden assumptions
* Identify blind spots
* Challenge architectural decisions
* Surface edge cases
* Highlight risks before they become problems

Think of it as having another experienced engineer review your approach before you invest time implementing it.

One caveat: `/rubber-duck` is intended to complement, not replace, your normal development and review processes. Its suggestions should be validated with tests and, where appropriate, a formal security review, human peer review, and authoritative project or product documentation. Treat its feedback as a source of questions and possible risks rather than as a definitive assessment.

## When Should You Use It?

The best time to engage `/rubber-duck` is whenever the cost of being wrong is high or the solution space is complex.

## How to Use It

Enter `/rubber-duck` in GitHub Copilot Chat, followed by a specific question or request for review. The command is available wherever the Rubber Duck agent is configured, including supported VS Code Copilot Chat environments. It can inspect the current conversation, relevant files in your workspace, open or selected code, and other available project context. For the most useful response, describe the decision, implementation, or change you want reviewed and identify the risks or questions you want it to consider.

### Scenario 1: Reviewing a New Feature Design

Before writing a single line of code, ask `/rubber-duck` to critique your implementation plan.

Example:

```text
/rubber-duck Review our plan for implementing dark mode across the web application. Identify any accessibility concerns, browser compatibility issues, or edge cases we may have overlooked.
```

The review might remind you to consider:

* High-contrast accessibility modes
* User preference persistence
* Embedded third-party components
* Theme switching performance

Finding these issues before you begin coding could save significant rework later.

### Scenario 2: Validating a Migration Plan

Migrations are one of the highest-risk activities in software development.

Example:

```text
/rubber-duck Review our migration from SQL Server to PostgreSQL. Identify rollback concerns, data integrity risks, deployment issues, or operational blind spots.
```

A second review can reveal:

* Missing rollback procedures
* Data conversion challenges
* Monitoring requirements
* Downtime assumptions

### Scenario 3: Challenging Security Decisions

Security issues often hide in assumptions.

Example:

```text
/rubber-duck Review our approach for introducing API key authentication between services. Highlight possible attack vectors, secret-management concerns, and operational risks.
```

This often uncovers concerns involving:

* Secret rotation
* Credential storage
* Logging sensitive information
* Key revocation procedures

### Scenario 4: Before Submitting a Pull Request

A final sanity check before requesting human review can be extremely valuable.

Example:

```text
/rubber-duck Analyze the changes in my current branch. What concerns would a senior reviewer likely raise during a pull request review?
```

This often identifies:

* Missing tests
* Naming inconsistencies
* Error handling gaps
* Documentation omissions

### Why It Works

One of the biggest challenges when solving technical problems is becoming too close to the solution. Once you've spent hours designing, coding, or debugging, it's easy to miss flaws that would be obvious to someone encountering the problem for the first time.

`/rubber-duck` helps break that tunnel vision by introducing an independent perspective. It won't replace peer reviews or architecture discussions, but it can help you discover issues earlier, improve design quality, and approach decisions more critically.

The next time you're preparing to implement a major feature, refactor a subsystem, roll out a migration, or submit a pull request, ask `/rubber-duck` to challenge your thinking and identify what you might be missing. I even used `/rubber-duck` to help me with this article.
