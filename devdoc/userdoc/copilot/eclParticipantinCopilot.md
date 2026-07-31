# The `@ecl` Participant in Copilot

The **`@ecl`** participant in the ECL Extension for VS Code enhances answers about ECL in several key ways.

## How to Use It

In VS Code's Copilot chat window, begin your chat with `@ecl`. This adds the participant to the conversation.

**Note:** You must have the ECL extension installed and enabled.

## How It Works

1. **Documentation Retrieval** — When you ask an ECL question, the participant searches the official HPCC Systems® ECL documentation and retrieves relevant reference material (ECL Language Reference, ECL Programmer's Guide, ECL Standard Library Reference, etc.). This grounds answers in **authoritative sources** rather than general knowledge.

2. **Tool Access** — The participant can use specialized tools to:
   - **Look up ECL syntax, functions, and keywords** from the documentation
   - **Search and inspect workunits** on a connected HPCC Platform
   - **Search logical files** on the cluster
   - **Syntax-check ECL code** you provide

3. **Contextual Awareness** — It considers your active editor content and workspace, so answers can be tailored to what you're actually working on.

## Why This Produces Better Answers

| Without `@ecl` | With `@ecl` |
| --- | --- |
| Generic LLM knowledge (may be outdated or inaccurate) | Grounded in official, current ECL documentation |
| Cannot inspect your cluster | Can query workunits and logical files directly |
| Cannot validate code | Can syntax-check your ECL snippets |
| May fabricate URLs or function signatures | Uses only verified documentation URLs and content |

## Example

If you ask about index compression options, the participant retrieves the exact `BUILDINDEX` documentation entry — including the `COMPRESSED` option variants (LZW, inplace, lz4, lz4hc, etc.), their syntax, and examples — and builds its answer from that, rather than relying on potentially stale training data.

It also provides links to the relevant topics in the online ECL Language Reference.

In short, **`@ecl`** acts as a bridge between the AI and the real ECL ecosystem — documentation, your cluster, and your code — so you get accurate, actionable answers.

See also: [Why `@workspace` matters in Copilot Chat](UsingWorkspace.md).