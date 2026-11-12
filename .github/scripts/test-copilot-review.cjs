const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { test } = require('node:test');

const workflow = readFileSync(`${__dirname}/../workflows/build-vcpkg.yml`, 'utf8');
const job = workflow.split('\n  wait-for-copilot-review:\n')[1].split('\n  build-workflow-dispatch:')[0];
const script = job.split('          script: |\n')[1]
    .split('\n').map(line => line.replace(/^ {12}/, '')).join('\n');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const runScript = new AsyncFunction('github', 'context', 'core', 'setTimeout', script);

function overview(severities, resolved = '') {
    const findings = severities.map(severity =>
        `- <picture><img src="badge.png" alt="${severity} severity"></picture> Issue`
    ).join('\n');
    return `<!-- ccr-overview-v2 -->\n<details open>
<summary><strong>${severities.length} open issues</strong></summary>
${findings}
</details>
${resolved}`;
}

function review(body, overrides = {}) {
    return {
        id: 123,
        is_copilot_code_review: true,
        commit_id: 'head-sha',
        submitted_at: '2026-11-11T10:11:14Z',
        state: 'COMMENTED',
        body,
        ...overrides,
    };
}

async function run(batches) {
    const outputs = {};
    let calls = 0;
    const listReviews = () => {};
    const github = {
        rest: { pulls: { listReviews } },
        async paginate(method, params) {
            assert.equal(method, listReviews);
            assert.deepEqual(params, {
                owner: 'owner', repo: 'repo', pull_number: 1, per_page: 100,
            });
            return batches[Math.min(calls++, batches.length - 1)];
        },
    };
    const context = {
        repo: { owner: 'owner', repo: 'repo' },
        issue: { number: 1 },
        payload: { pull_request: { head: { sha: 'head-sha' } } },
    };
    const core = {
        setOutput(name, value) { outputs[name] = String(value); },
        info() {},
    };
    try {
        await runScript(github, context, core, callback => callback());
        return { outputs, calls };
    } catch (error) {
        assert.equal(outputs['review-completed'], 'false');
        assert.equal(outputs['has-high-issues'], undefined);
        throw error;
    }
}

test('flags High and Critical badges, not arbitrary summary prose', async () => {
    for (const severity of ['High', 'Critical']) {
        const { outputs } = await run([[review(overview([severity]))]]);
        assert.deepEqual(outputs, { 'review-completed': 'true', 'has-high-issues': 'true' });
    }
    const { outputs } = await run([[review(overview(['Medium', 'Low']) + '\nNo HIGH or CRITICAL issues found.')]]);
    assert.equal(outputs['has-high-issues'], 'false');
});

test('ignores High badges in resolved findings', async () => {
    const resolved = '<details><summary><strong>1 resolved since last review</strong></summary>\n'
        + '- <picture><img alt="High severity"></picture> Fixed\n</details>';
    for (const body of [
        overview(['Low'], resolved),
        `<!-- ccr-overview-v2 -->\n**No open issues**\n${resolved}`,
    ]) {
        const { outputs } = await run([[review(body)]]);
        assert.equal(outputs['has-high-issues'], 'false');
        assert.equal(outputs['review-completed'], 'true');
    }
});

test('waits for a submitted Copilot review of this head, then uses the latest', async () => {
    const high = overview(['High']);
    const ignored = [
        review(high, { commit_id: 'old-sha' }),
        review(high, { is_copilot_code_review: false }),
        review(high, { state: 'PENDING', submitted_at: null }),
        review(high, { state: 'DISMISSED' }),
    ];
    const result = await run([ignored, [
        ...ignored,
        review(high),
        review(overview(['Low']), { id: 124, state: 'APPROVED' }),
    ]]);
    assert.equal(result.calls, 2);
    assert.equal(result.outputs['has-high-issues'], 'false');
});

test('rejects unknown, incomplete, or inconsistent overview formats', async () => {
    for (const body of [
        null,
        'No HIGH or CRITICAL issues found.',
        '<!-- ccr-overview-v2 -->\nNo findings listed',
        overview(['Unknown']),
        overview(['High']).replace('1 open issues', '2 open issues'),
        '<!-- ccr-overview-v2 -->\n<details><summary>Resolved</summary>\n**No open issues**\n</details>',
    ]) {
        await assert.rejects(run([[review(body)]]), /Copilot review/);
    }
});

test('times out rather than reporting missing reviews as clean', async () => {
    await assert.rejects(run([[]]), /Copilot review timeout/);
});
