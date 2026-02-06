#!/usr/bin/env node
/* eslint-disable no-console */

import { spawnSync, SpawnSyncReturns } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, unlinkSync } from "node:fs";
import { join, dirname, basename, extname, relative } from "node:path";

interface SpawnOptions {
    check?: boolean;
}

interface PullRequest {
    number: number;
    title: string;
    url: string;
    updatedAt: string;
    headRefName: string;
    baseRefName: string;
}

interface WorkflowRun {
    databaseId: string;
    status: string;
    conclusion: string | null;
    name: string;
    url: string;
    updatedAt: string;
}

interface PrInfo {
    headRefOid?: string;
    headRefName?: string;
}

interface CliOptions {
    branch?: string;
    pr?: number;
    repo?: string;
    output: string;
    help: boolean;
}

function run(cmd: string, args: string[], options: SpawnOptions = {}): SpawnSyncReturns<string> {
    const result = spawnSync(cmd, args, {
        encoding: "utf8",
        shell: false,
    });
    if (options.check !== false && result.status !== 0) {
        const message = (result.stderr || result.stdout || "").trim();
        throw new Error(`Command failed: ${cmd} ${args.join(" ")}\n${message}`.trim());
    }
    return result;
}

function requireGhAuth(): void {
    try {
        run("gh", ["auth", "status", "-h", "github.com"], { check: true });
    } catch (err) {
        throw new Error("GitHub CLI is not authenticated. Run: gh auth login");
    }
}

function getRepoRoot(): string {
    return run("git", ["rev-parse", "--show-toplevel"], { check: true }).stdout.trim();
}

function getCurrentBranch(): string {
    const branch = run("git", ["rev-parse", "--abbrev-ref", "HEAD"], { check: true }).stdout.trim();
    if (branch === "HEAD") {
        throw new Error("Detached HEAD. Provide --branch or --pr.");
    }
    return branch;
}

function selectLatestPr(prs: PullRequest[]): PullRequest {
    return prs.reduce((latest, pr) => {
        const latestDate = new Date(latest.updatedAt || "1970-01-01T00:00:00Z");
        const prDate = new Date(pr.updatedAt || "1970-01-01T00:00:00Z");
        return prDate > latestDate ? pr : latest;
    }, prs[0]);
}

function listPrsForBranch(branch: string, repo?: string): PullRequest[] {
    const args = [
        "pr",
        "list",
        "--state",
        "all",
        "--head",
        branch,
        "--json",
        "number,title,url,updatedAt,headRefName,baseRefName",
    ];
    if (repo) {
        args.push("--repo", repo);
    }
    const result = run("gh", args, { check: true });
    return JSON.parse(result.stdout || "[]");
}

function listRunsForPr(prNumber: number, repo?: string): WorkflowRun[] {
    const prInfo = getPrInfo(prNumber, repo);
    const args = [
        "run",
        "list",
        "--json",
        "databaseId,status,conclusion,name,url,updatedAt",
    ];
    if (prInfo.headRefOid) {
        args.push("--commit", prInfo.headRefOid);
    } else if (prInfo.headRefName) {
        args.push("--branch", prInfo.headRefName);
    }
    if (repo) {
        args.push("--repo", repo);
    }
    const result = run("gh", args, { check: true });
    return JSON.parse(result.stdout || "[]");
}

function getPrInfo(prNumber: number, repo?: string): PrInfo {
    const args = [
        "pr",
        "view",
        String(prNumber),
        "--json",
        "headRefOid,headRefName",
    ];
    if (repo) {
        args.push("--repo", repo);
    }
    const result = run("gh", args, { check: true });
    return JSON.parse(result.stdout || "{}");
}

function downloadArtifacts(runId: number, outputDir: string, repo?: string): boolean {
    mkdirSync(outputDir, { recursive: true });
    const args = ["run", "download", String(runId), "--dir", outputDir];
    if (repo) {
        args.push("--repo", repo);
    }
    const result = run("gh", args, { check: false });
    return result.status === 0;
}

function commandExists(command: string): boolean {
    const result = run(
        process.platform === "win32" ? "where" : "command",
        process.platform === "win32" ? [command] : ["-v", command],
        { check: false }
    );
    return result.status === 0;
}

function extractZip(zipPath: string): boolean {
    const extractDir = join(dirname(zipPath), basename(zipPath, extname(zipPath)));
    mkdirSync(extractDir, { recursive: true });

    if (process.platform === "win32") {
        const args = [
            "-NoProfile",
            "-Command",
            `Expand-Archive -LiteralPath "${zipPath}" -DestinationPath "${extractDir}" -Force`,
        ];
        const result = run("powershell", args, { check: false });
        return result.status === 0;
    }

    if (commandExists("unzip")) {
        const result = run("unzip", ["-o", zipPath, "-d", extractDir], { check: false });
        return result.status === 0;
    }

    return false;
}

function extractZipFiles(outputDir: string): void {
    const stack = [outputDir];
    while (stack.length) {
        const current = stack.pop();
        if (!current) continue;
        const entries = readdirSync(current, { withFileTypes: true });
        for (const entry of entries) {
            const fullPath = join(current, entry.name);
            if (entry.isDirectory()) {
                stack.push(fullPath);
            } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".zip")) {
                const extracted = extractZip(fullPath);
                if (extracted) {
                    unlinkSync(fullPath);
                }
            }
        }
    }
}

function parseArgs(argv: string[]): CliOptions {
    const options: CliOptions = {
        branch: undefined,
        pr: undefined,
        repo: undefined,
        output: "tmp-gh-pr-status",
        help: false,
    };

    for (let i = 0; i < argv.length; i += 1) {
        const arg = argv[i];
        if (arg === "--branch") {
            options.branch = argv[i + 1];
            i += 1;
        } else if (arg === "--pr") {
            options.pr = Number.parseInt(argv[i + 1], 10);
            i += 1;
        } else if (arg === "--repo") {
            options.repo = argv[i + 1];
            i += 1;
        } else if (arg === "--output") {
            options.output = argv[i + 1];
            i += 1;
        } else if (arg === "--help" || arg === "-h") {
            options.help = true;
        }
    }

    return options;
}

function showHelp(): void {
    console.log(`
GitHub Actions PR Checker
Check GitHub Actions status for pull requests and download failed workflow artifacts.

USAGE:
  npx tsx check_pr_actions.ts [OPTIONS]

OPTIONS:
  --branch <name>     Override branch name (default: current git branch)
  --pr <number>       Check specific PR by number (default: auto-detect from branch)
  --repo <owner/repo> Repository to check (default: current git repository)
  --output <dir>      Output directory for artifacts (default: tmp-gh-pr-status)
  --help, -h          Show this help message

EXAMPLES:
  # Check current branch
  npx tsx check_pr_actions.ts

  # Check specific PR
  npx tsx check_pr_actions.ts --pr 12345

  # Check different branch
  npx tsx check_pr_actions.ts --branch feature/my-feature

  # Custom output directory
  npx tsx check_pr_actions.ts --output ./test-failures

  # Check different repository
  npx tsx check_pr_actions.ts --repo owner/repo-name --pr 123

EXIT CODES:
  0 - All runs succeeded (or no runs found)
  1 - One or more runs failed (artifacts downloaded)
  2 - Error occurred (authentication, git, etc.)

For more information, see:
  .github/skills/gh-pr-status/SKILL.md
`);
}

function main(): number {
    const args = parseArgs(process.argv.slice(2));

    if (args.help) {
        showHelp();
        return 0;
    }

    requireGhAuth();
    const repoRoot = getRepoRoot();

    const branch = args.branch || getCurrentBranch();
    let prNumber = args.pr;

    if (!prNumber) {
        const prs = listPrsForBranch(branch, args.repo);
        if (!prs.length) {
            throw new Error(`No PR found for branch '${branch}'.`);
        }
        if (prs.length > 1) {
            console.log(`Found ${prs.length} PR(s) for branch '${branch}':`);
            for (const pr of prs) {
                const age = new Date(pr.updatedAt);
                console.log(`  #${pr.number}: ${pr.title}`);
                console.log(`    ${pr.headRefName} → ${pr.baseRefName}`);
                console.log(`    Updated: ${age.toLocaleString()}`);
                console.log(`    ${pr.url}`);
            }
            const pr = selectLatestPr(prs);
            console.log(`\nUsing most recently updated: PR #${pr.number}`);
            prNumber = Number.parseInt(String(pr.number), 10);
        } else {
            const pr = prs[0];
            prNumber = Number.parseInt(String(pr.number), 10);
            console.log(`Found PR #${prNumber}: ${pr.title}`);
            console.log(`  ${pr.url}`);
        }
    } else {
        console.log(`Using PR #${prNumber}`);
    }

    const runs = listRunsForPr(prNumber, args.repo);
    if (!runs.length) {
        console.log("No workflow runs found for this PR.");
        return 0;
    }

    console.log(`\nFound ${runs.length} workflow run(s):`);

    const completed = runs.filter((run) => run.status === "completed");
    const failed = completed.filter((run) => run.conclusion !== "success");
    const pending = runs.filter((run) => run.status !== "completed");

    // Identify ECL Watch specific workflow
    const eclWatchRuns = runs.filter((run) => run.name && run.name.includes("Test ECL Watch"));
    const eclWatchFailed = eclWatchRuns.filter((run) => run.status === "completed" && run.conclusion !== "success");
    const eclWatchPending = eclWatchRuns.filter((run) => run.status !== "completed");

    // Show summary
    console.log(`  ✓ Completed: ${completed.length - failed.length} successful`);
    if (failed.length) {
        console.log(`  ✗ Completed: ${failed.length} failed`);
    }
    if (pending.length) {
        console.log(`  ⏳ In progress: ${pending.length}`);
    }

    // Highlight ECL Watch workflow status
    if (eclWatchRuns.length > 0) {
        console.log(`\n  📊 ECL Watch Test Status:`);
        if (eclWatchFailed.length > 0) {
            console.log(`    ✗ Failed - Playwright tests need attention!`);
        } else if (eclWatchPending.length > 0) {
            console.log(`    ⏳ Running - waiting for Playwright tests...`);
        } else {
            console.log(`    ✓ Passed - UI tests successful!`);
        }
    }

    if (!failed.length && !pending.length) {
        console.log("\n✓ All completed workflow runs succeeded.");
        return 0;
    }

    if (pending.length) {
        console.log(`\n⏳ ${pending.length} workflow run(s) are still in progress:`);
        for (const runInfo of pending) {
            console.log(`  - ${runInfo.name} (${runInfo.url})`);
        }
    }

    if (!failed.length) {
        console.log("\nNo failed workflow runs detected.");
        return 0;
    }

    const outputDir = join(repoRoot, args.output);
    mkdirSync(outputDir, { recursive: true });

    console.log(`\n✗ Downloading artifacts for ${failed.length} failed run(s) to ${args.output}/...`);

    // Prioritize ECL Watch failures
    const sortedFailed = [...failed].sort((a, b) => {
        const aIsECLWatch = a.name && a.name.includes("Test ECL Watch");
        const bIsECLWatch = b.name && b.name.includes("Test ECL Watch");
        if (aIsECLWatch && !bIsECLWatch) return -1;
        if (!aIsECLWatch && bIsECLWatch) return 1;
        return 0;
    });

    for (const runInfo of sortedFailed) {
        const runId = Number.parseInt(runInfo.databaseId, 10);
        const runName = runInfo.name || "run";
        const conclusion = runInfo.conclusion || "unknown";
        const runDir = join(outputDir, `run-${runId}`);
        const isECLWatch = runName.includes("Test ECL Watch");
        console.log(`\n  ${runName} (${conclusion})${isECLWatch ? " 📊 [ECL Watch Tests]" : ""}`);
        console.log(`    URL: ${runInfo.url}`);
        process.stdout.write(`    Downloading artifacts... `);
        const downloaded = downloadArtifacts(runId, runDir, args.repo);
        if (downloaded) {
            console.log("✓");
            process.stdout.write(`    Extracting archives... `);
            extractZipFiles(runDir);
            console.log("✓");
            const relPath = relative(repoRoot, runDir);
            console.log(`    Saved to: ${relPath}/`);
            if (isECLWatch) {
                // Check for Playwright report
                const playwrightReport = join(runDir, "eclwatch-test-results", "playwright-report", "index.html");
                if (existsSync(playwrightReport)) {
                    console.log(`    📊 Playwright report: ${relative(repoRoot, playwrightReport)}`);
                }
            }
        } else {
            console.log("(no artifacts)");
        }
    }

    console.log(`\nArtifacts downloaded to: ${args.output}/`);
    return 1;
}

try {
    process.exit(main());
} catch (err) {
    console.error(`Error: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
}
