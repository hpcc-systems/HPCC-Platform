# Why `@workspace` matters in Copilot Chat

`@workspace` is a Copilot Chat context selector that enables Copilot to use files from your currently opened project as grounding context.

When you chat with a domain agent like the `@ecl` participant, it can be highly knowledgeable about ECL concepts, syntax, and documentation. But that does not automatically mean it can read your local files.

That is the key difference:

- Without `@workspace`: the agent answers from its built-in knowledge, documentation context, and tools, but not your local project files unless you paste content.
- With `@workspace`: you explicitly grant access to your open workspace context, so Copilot can read referenced files and use nearby project structure as context.

## What this looks like in practice

### Example 1 (no workspace context)

```text
@ecl explain #file:initfiles/examples/TutorialYourName/BWR_ProcessRawData.ecl in the context of initfiles/examples/TutorialYourName
```

Typical result:

```text
“I can only answer questions about ECL based on the documentation and tools available to me. The file you referenced is a local workspace file that I don’t have access to view.”
```

Why this happens:

- You asked for a local file explanation.
- The agent was not given workspace scope.
- So it could not open or inspect that file, even though your prompt referenced it.

### Example 2 (workspace context enabled)

```text
@workspace @ecl explain #file:initfiles/examples/TutorialYourName/BWR_ProcessRawData.ecl in the context of initfiles/examples/TutorialYourName
```

Typical result:

Now Copilot explains the file clearly and specifically.

## Why this happens

- `@workspace` tells Copilot to use your workspace as an allowed source of truth.
- The `#file` reference can now be resolved to the actual file.
- The agent can combine ECL knowledge with the real code in your project folder.

### Simple mental model

Think of `@ecl` as the specialist and `@workspace` as the permission slip.

- `@ecl` = “I know ECL.”
- `@workspace` = “You may read my project files.”
- Together = “Explain my real ECL code, in my real repo context.”

### When to add @workspace

Use `@workspace` whenever your question depends on:

- A specific local file
- Project-relative imports/includes
- Folder-level conventions
- Cross-file behavior in your repo

If your question is purely conceptual (for example, “What is a SuperFile?”), `@workspace` is often unnecessary.

### Practical prompt pattern

A reliable pattern is:

```text
@workspace @ecl explain #file:path/to/file.ecl in the context of path/to/folder
```

This gives Copilot three things at once:

- Permission to read files
- The exact file to inspect
- The surrounding folder context to interpret intent

## Bottom line

Adding `@workspace` is what turns a general expert response into a code-aware explanation of your actual files. In the example above, that single addition is exactly why the second prompt works and the first one doesn't.
