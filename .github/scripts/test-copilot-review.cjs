const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { test } = require('node:test');

const workflow = readFileSync(`${__dirname}/../workflows/build-vcpkg.yml`, 'utf8');
const labelWorkflow = readFileSync(`${__dirname}/../workflows/triage-label.yml`, 'utf8');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

function workflowJob(name, source = workflow) {
    return source.split(`\n  ${name}:\n`)[1].split(/\n  [\w-]+:\n/)[0];
}

function workflowScript(name, source = workflow) {
    return workflowJob(name, source).split('          script: |\n')[1]
        .split(/\n(?= {0,10}\S)/)[0]
        .split('\n').map(line => line.replace(/^ {12}/, '')).join('\n');
}

const runScript = new AsyncFunction('github', 'context', 'core', 'setTimeout',
    workflowScript('wait-for-copilot-review'));

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

async function run(batches, commitTrees = {}) {
    const outputs = {};
    let calls = 0;
    const listReviews = () => {};
    const github = {
        rest: {
            pulls: { listReviews },
            git: {
                async getCommit({ owner, repo, commit_sha }) {
                    assert.equal(owner, 'owner');
                    assert.equal(repo, 'repo');
                    if (!(commit_sha in commitTrees)) {
                        throw new Error(`No mock tree SHA for commit ${commit_sha}`);
                    }
                    return { data: { tree: { sha: commitTrees[commit_sha] } } };
                },
            },
        },
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
        assert.equal(outputs['has-medium-issues'], undefined);
        assert.equal(outputs['has-low-issues'], undefined);
        throw error;
    }
}

test('flags High and Critical badges, not arbitrary summary prose', async () => {
    for (const severity of ['High', 'Critical']) {
        const { outputs } = await run([[review(overview([severity]))]]);
        assert.deepEqual(outputs, {
            'review-completed': 'true',
            'has-high-issues': 'true',
            'has-medium-issues': 'false',
            'has-low-issues': 'false',
        });
    }
    const { outputs } = await run([[review(overview(['Medium', 'Low']) + '\nNo HIGH or CRITICAL issues found.')]]);
    assert.equal(outputs['has-high-issues'], 'false');
    assert.equal(outputs['has-medium-issues'], 'true');
    assert.equal(outputs['has-low-issues'], 'true');
});

test('reports each open severity independently', async () => {
    for (const [severities, high, medium, low] of [
        [['Medium'], false, true, false],
        [['Low'], false, false, true],
        [['critical', 'medium', 'low'], true, true, true],
        [[], false, false, false],
    ]) {
        const { outputs } = await run([[review(overview(severities))]]);
        assert.deepEqual(outputs, {
            'review-completed': 'true',
            'has-high-issues': String(high),
            'has-medium-issues': String(medium),
            'has-low-issues': String(low),
        });
    }
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

test('ignores Medium and Low badges in resolved findings', async () => {
    const resolved = '<details><summary><strong>2 resolved since last review</strong></summary>\n'
        + '- <img alt="Medium severity"> Fixed\n- <img alt="Low severity"> Fixed\n</details>';
    for (const body of [
        overview(['High'], resolved),
        `<!-- ccr-overview-v2 -->\n**No open issues**\n${resolved}`,
    ]) {
        const { outputs } = await run([[review(body)]]);
        assert.equal(outputs['has-medium-issues'], 'false');
        assert.equal(outputs['has-low-issues'], 'false');
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
    ]], { 'head-sha': 'head-tree', 'old-sha': 'other-tree' });
    assert.equal(result.calls, 2);
    assert.equal(result.outputs['has-high-issues'], 'false');
});

test('accepts Copilot review after force push with unchanged content', async () => {
    // After a force push, head SHA changed but tree content is identical
    // The head is always 'head-sha' in the test context, so both old and new commit map to same tree
    const result = await run([[
        review(overview(['Medium']), { commit_id: 'old-sha-before-force-push' }),
    ]], { 'head-sha': 'shared-tree', 'old-sha-before-force-push': 'shared-tree' });
    assert.equal(result.outputs['has-high-issues'], 'false');
    assert.equal(result.outputs['has-medium-issues'], 'true');
    assert.equal(result.calls, 1);  // Should succeed on first poll
});

test('prefers exact SHA match over tree match', async () => {
    // Both exact-SHA and tree-SHA reviews exist; prefer exact SHA to match current behavior
    const result = await run([[
        review(overview(['Low']), { commit_id: 'old-sha-with-same-tree' }),
        review(overview(['High']), { commit_id: 'head-sha' }),
    ]], { 'head-sha': 'head-tree', 'old-sha-with-same-tree': 'head-tree' });
    assert.equal(result.outputs['has-high-issues'], 'true');  // Uses exact match, not tree match
    assert.equal(result.calls, 1);
});

test('rejects different-tree reviews when better SHA matches exist', async () => {
    // Ensure force-push matching doesn't accidentally accept unrelated reviews
    const result = await run([[
        review(overview(['High']), { commit_id: 'different-sha-different-tree' }),
        review(overview(['Low']), { commit_id: 'head-sha' }),
    ]], { 'head-sha': 'head-tree', 'different-sha-different-tree': 'other-tree' });
    assert.equal(result.outputs['has-high-issues'], 'false');  // Uses exact match
    assert.equal(result.calls, 1);
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

function triageNeeds() {
    return {
        'clear-ready-to-test': { result: 'success' },
        'analyze-changes': {
            result: 'success',
            outputs: { impacts: JSON.stringify(Object.fromEntries([
                'platform', 'eclwatch', 'docs', 'unittests', 'regression-suite',
                'roxie', 'thor', 'hthor', 'esp', 'dali',
            ].map(name => [name, false]))) },
        },
        'wait-for-copilot-review': {
            result: 'success',
            outputs: {
                'review-completed': 'true',
                'has-high-issues': 'false',
                'has-medium-issues': 'false',
                'has-low-issues': 'true',
            },
        },
    };
}

async function runTriageJob(name, {
    needs = triageNeeds(), labels = [], head = 'head-sha', eventName = 'pull_request',
    apiError, removeError,
} = {}) {
    const removed = [];
    const github = {
        rest: {
            pulls: {
                async get(params) {
                    assert.deepEqual(params, { owner: 'owner', repo: 'repo', pull_number: 1 });
                    assert.equal(eventName, 'pull_request');
                    if (apiError) throw apiError;
                    return { data: { head: { sha: head }, labels: labels.map(name => ({ name })) } };
                },
            },
            issues: {
                async removeLabel(params) {
                    assert.deepEqual(params, {
                        owner: 'owner', repo: 'repo', issue_number: 1, name: 'ready-to-test',
                    });
                    if (removeError) throw removeError;
                    removed.push(params.name);
                },
            },
        },
    };
    const context = {
        repo: { owner: 'owner', repo: 'repo' },
        issue: { number: 1 },
        eventName,
        // Deliberately stale: both jobs must use the live labels instead.
        payload: { pull_request: { head: { sha: 'head-sha' }, labels: [{ name: 'ready-to-test' }] } },
    };
    const execute = new AsyncFunction('github', 'context', 'core', 'process', workflowScript(name));
    await execute(github, context, { info() {} }, { env: { NEEDS: JSON.stringify(needs) } });
    return removed;
}

test('triage blocks High and Medium findings unless the live override label is present', async () => {
    await runTriageJob('triage');
    for (const severity of ['has-high-issues', 'has-medium-issues']) {
        const needs = triageNeeds();
        needs['wait-for-copilot-review'].outputs[severity] = 'true';
        await assert.rejects(runTriageJob('triage', { needs, labels: ['other'] }), /Copilot found/);
        await runTriageJob('triage', { needs, labels: ['ready-to-test'] });
    }
});

test('the override cannot bypass failed prerequisites, incomplete reviews, or invalid outputs', async () => {
    for (const job of ['clear-ready-to-test', 'analyze-changes', 'wait-for-copilot-review']) {
        for (const result of ['failure', 'skipped', 'cancelled']) {
            const needs = triageNeeds();
            needs[job].result = result;
            await assert.rejects(runTriageJob('triage', { needs, labels: ['ready-to-test'] }),
                /must (succeed|complete successfully)/);
        }
    }
    for (const [output, value] of [
        ['review-completed', 'false'],
        ['has-high-issues', undefined],
        ['has-medium-issues', 'unknown'],
    ]) {
        const needs = triageNeeds();
        needs['wait-for-copilot-review'].outputs[output] = value;
        await assert.rejects(runTriageJob('triage', { needs, labels: ['ready-to-test'] }),
            /must complete successfully|missing or invalid/);
    }
});

test('tag pushes pass triage with successful PR-only no-ops, but still require analysis', async () => {
    const needs = triageNeeds();
    needs['wait-for-copilot-review'] = { result: 'success', outputs: {} };
    await runTriageJob('triage', { needs, eventName: 'push' });
    needs['analyze-changes'].result = 'failure';
    await assert.rejects(runTriageJob('triage', { needs, eventName: 'push' }), /analysis must succeed/);
});

test('triage rejects missing, malformed, or non-boolean impact categories even with an override', async () => {
    for (const impacts of [undefined, '{', 'null', '[]', '{}',
        JSON.stringify({ ...JSON.parse(triageNeeds()['analyze-changes'].outputs.impacts), platform: 'true' }),
    ]) {
        const needs = triageNeeds();
        needs['analyze-changes'].outputs.impacts = impacts;
        await assert.rejects(runTriageJob('triage', { needs, labels: ['ready-to-test'] }),
            /JSON|boolean/);
    }
});

test('cleanup removes only a live ready-to-test label and propagates removal errors', async () => {
    assert.deepEqual(await runTriageJob('clear-ready-to-test', { labels: ['other'] }), []);
    assert.deepEqual(await runTriageJob('clear-ready-to-test', { labels: ['other', 'ready-to-test'] }),
        ['ready-to-test']);
    assert.deepEqual(await runTriageJob('clear-ready-to-test', {
        labels: ['ready-to-test'],
        removeError: Object.assign(new Error('Label not found'), { status: 404 }),
    }), []);
    await assert.rejects(runTriageJob('clear-ready-to-test', {
        labels: ['ready-to-test'], removeError: new Error('Permission denied'),
    }), /Permission denied/);
});

test('cleanup and triage reject obsolete PR heads and propagate label-fetch errors', async () => {
    for (const name of ['clear-ready-to-test', 'triage']) {
        await assert.rejects(runTriageJob(name, { head: 'new-head', labels: ['ready-to-test'] }),
            /PR head changed/);
        await assert.rejects(runTriageJob(name, { apiError: new Error('API unavailable') }),
            /API unavailable/);
    }
});

function expressionValue(expression, eventName, action, label = 'ready-to-test', cancelled = false) {
    const github = {
        event_name: eventName, event: { action, label: { name: label }, pull_request: { number: 1 } },
        workflow: 'build', ref: 'refs/pull/1/merge', run_id: 123,
    };
    const evaluate = new Function('github', 'contains', 'format', 'cancelled',
        `return (${expression.replace(/^\s*\$\{\{|\}\}\s*$/g, '')});`);
    return evaluate(github, (list, value) => list.includes(value),
        (template, value) => template.replace('{0}', value), () => cancelled);
}

test('label events use a separate handler and never repeat prerequisite jobs or clear the override', () => {
    assert.match(workflow, /types: \[opened, synchronize, reopened\]/);
    assert.match(labelWorkflow, /types: \[labeled, unlabeled\]/);
    const handlerCondition = workflowJob('rerun-triage', labelWorkflow).match(/^    if: (.+)$/m)[1];
    assert.equal(expressionValue(handlerCondition, 'pull_request', 'labeled'), true);
    assert.equal(expressionValue(handlerCondition, 'pull_request', 'unlabeled'), true);
    assert.equal(expressionValue(handlerCondition, 'pull_request', 'labeled', 'other'), false);
    const group = labelWorkflow.match(/^  group: (.+)$/m)[1];
    const concurrencyGroup = (action, label) => group.replace(/\$\{\{([\s\S]*?)\}\}/g,
        (_, expression) => expressionValue(expression, 'pull_request', action, label));
    assert.equal(concurrencyGroup('labeled', 'ready-to-test'), concurrencyGroup('unlabeled', 'ready-to-test'));
    assert.notEqual(concurrencyGroup('labeled', 'other'), concurrencyGroup('labeled', 'ready-to-test'));
    assert.match(labelWorkflow, /^  cancel-in-progress: false$/m);
    for (const name of ['clear-ready-to-test', 'wait-for-copilot-review', 'analyze-changes', 'triage']) {
        const job = workflowJob(name);
        assert.doesNotMatch(job.match(/^    if: (.+)$/m)[1], /label/);
        const condition = job.match(/^    if: (.+)$/m)[1];
        for (const action of ['opened', 'reopened', 'synchronize']) {
            assert.equal(expressionValue(condition, 'pull_request', action), true, `${name}: ${action}`);
        }
        assert.equal(expressionValue(condition, 'push'), true, `${name}: tag push must not skip an ancestor`);
        for (const event of ['workflow_dispatch', 'schedule']) {
            assert.equal(expressionValue(condition, event), false);
        }
    }
    const cleanupCondition = workflowJob('clear-ready-to-test').match(/^        if: (.+)$/m)[1];
    assert.equal(expressionValue(cleanupCondition, 'pull_request', 'synchronize'), true);
    for (const action of ['opened', 'reopened', 'labeled', 'unlabeled']) {
        assert.equal(expressionValue(cleanupCondition, 'pull_request', action), false);
    }
    assert.equal(expressionValue(cleanupCondition, 'push'), false);
    const reviewCondition = workflowJob('wait-for-copilot-review').match(/^        if: (.+)$/m)[1];
    assert.equal(expressionValue(reviewCondition, 'push'), false);
    for (const action of ['opened', 'reopened', 'synchronize']) {
        assert.equal(expressionValue(reviewCondition, 'pull_request', action), true);
    }
    const triageCondition = workflowJob('triage').match(/^    if: (.+)$/m)[1];
    assert.equal(expressionValue(triageCondition, 'pull_request', 'synchronize', 'ready-to-test', true), false);
});

test('impact consumers depend on triage and the retired path-filter job is no longer referenced', () => {
    assert.doesNotMatch(workflow, /\bpre_job\b/);
    assert.doesNotMatch(labelWorkflow, /\bpre_job\b/);
    for (const [, name] of workflow.matchAll(/^  ([\w-]+):\n/g)) {
        const job = workflowJob(name);
        if (job.includes('needs.triage.outputs.impacts')) {
            assert.match(job, /^    needs: \[[^\]]*\btriage\b[^\]]*\]/m);
            assert.doesNotMatch(job, /needs\.analyze-changes/);
        }
    }
    assert.match(workflowJob('triage'),
        /needs: \[clear-ready-to-test, wait-for-copilot-review, analyze-changes\]/);
});

function impactGate(name, impacts, eventName = 'pull_request') {
    const output = workflowJob('triage').match(/^      impacts: (.+)$/m)[1];
    const exportImpacts = new Function('needs',
        `return (${output.replace(/^\$\{\{|\}\}$/g, '')
            .replaceAll('needs.analyze-changes', "needs['analyze-changes']")});`);
    const forwarded = exportImpacts({ 'analyze-changes': { outputs: { impacts: JSON.stringify(impacts) } } });
    const job = workflowJob(name);
    const condition = job.match(/^    if: (.+)$/m)[1];
    const expression = condition === '>-'
        ? job.split('    if: >-\n')[1].split(/\n(?=    \S)/)[0]
        : condition;
    const evaluate = new Function('github', 'needs', 'contains', 'fromJSON',
        `return (${expression.replace(/^\s*\$\{\{|\}\}\s*$/g, '')});`);
    return evaluate({ event_name: eventName }, { triage: { outputs: { impacts: forwarded } } },
        (list, value) => list.includes(value), JSON.parse);
}

test('triage forwards categories so docs, frontend, regression and unit changes select only their consumers', () => {
    const consumers = [
        'build-docker-ubuntu-24_04', 'build-docker-ubuntu-22_04',
        'test-smoke-docker-ubuntu-22_04', 'test-regression-suite-k8s-ubuntu-22_04',
        'test-unit-docker-ubuntu-22_04', 'test-bundles-on-thor-ubuntu-22_04',
        'test-eclwatch-gh_runner-ubuntu-22_04', 'test-documentation-ubuntu-22_04',
    ];
    for (const [category, expected] of [
        ['docs', ['test-documentation-ubuntu-22_04']],
        ['eclwatch', ['build-docker-ubuntu-24_04', 'build-docker-ubuntu-22_04',
            'test-unit-docker-ubuntu-22_04', 'test-eclwatch-gh_runner-ubuntu-22_04']],
        ['regression-suite', ['build-docker-ubuntu-22_04', 'test-smoke-docker-ubuntu-22_04',
            'test-regression-suite-k8s-ubuntu-22_04', 'test-bundles-on-thor-ubuntu-22_04']],
        ['unittests', ['build-docker-ubuntu-22_04', 'test-unit-docker-ubuntu-22_04']],
        ['platform', ['build-docker-ubuntu-24_04', 'build-docker-ubuntu-22_04', 'test-unit-docker-ubuntu-22_04']],
        ['roxie', ['build-docker-ubuntu-24_04', 'build-docker-ubuntu-22_04',
            'test-smoke-docker-ubuntu-22_04', 'test-unit-docker-ubuntu-22_04']],
        ['esp', ['build-docker-ubuntu-24_04', 'build-docker-ubuntu-22_04',
            'test-regression-suite-k8s-ubuntu-22_04', 'test-unit-docker-ubuntu-22_04']],
    ]) {
        const impacts = { ...JSON.parse(triageNeeds()['analyze-changes'].outputs.impacts), [category]: true };
        if (['eclwatch', 'roxie', 'esp'].includes(category)) {
            impacts.platform = true;
        }
        assert.deepEqual(consumers.filter(name => impactGate(name, impacts)), expected, category);
        assert.deepEqual(consumers.filter(name => impactGate(name, impacts, 'push')), expected, `${category}: tag`);
        assert.deepEqual(consumers.filter(name => impactGate(name, impacts, 'schedule')), []);
    }
});

test('every package-consuming test selects its producer for all valid category combinations', () => {
    const categories = Object.keys(JSON.parse(triageNeeds()['analyze-changes'].outputs.impacts));
    const consumers = ['test-smoke-docker-ubuntu-22_04', 'test-unit-docker-ubuntu-22_04',
        'test-bundles-on-thor-ubuntu-22_04', 'test-eclwatch-gh_runner-ubuntu-22_04'];
    for (let mask = 0; mask < 2 ** categories.length; mask++) {
        const impacts = Object.fromEntries(categories.map((name, index) => [name, Boolean(mask & (1 << index))]));
        // Frontend and engine categories imply platform under the analyzer's rules.
        if (!impacts.platform && ['eclwatch', 'roxie', 'thor', 'hthor', 'esp', 'dali'].some(name => impacts[name])) {
            continue;
        }
        if (consumers.some(name => impactGate(name, impacts))) {
            assert.equal(impactGate('build-docker-ubuntu-22_04', impacts), true, JSON.stringify(impacts));
        }
    }
});

test('all additional native build variants require platform rather than test or documentation impacts', () => {
    const variants = [...workflow.matchAll(/^  (build-[\w-]+):\n/g)]
        .map(([, name]) => name)
        .filter(name => name !== 'build-docker-ubuntu-22_04' &&
            workflowJob(name).includes('needs: [triage]'));
    const impacts = JSON.parse(triageNeeds()['analyze-changes'].outputs.impacts);
    for (const category of Object.keys(impacts)) {
        for (const name of variants) {
            assert.equal(impactGate(name, { ...impacts, [category]: true }),
                category === 'platform', `${name}: ${category}`);
        }
    }
});

function buildRun(overrides = {}) {
    return {
        id: 10, head_sha: 'head-sha', head_branch: 'feature', status: 'completed',
        pull_requests: [{ number: 1 }], ...overrides,
    };
}

function buildJobs(triageStatus = 'completed', triageConclusion = 'success') {
    return [
        { id: 20, name: 'triage', status: triageStatus, conclusion: triageConclusion },
        ...['clear-ready-to-test', 'wait-for-copilot-review', 'analyze-changes']
            .map((name, index) => ({ id: index + 1, name, status: 'completed' })),
    ];
}

async function runLabelHandler({
    states = [{ runs: [buildRun()], jobs: buildJobs() }],
    heads = ['head-sha'], prState = 'open', action = 'unlabeled',
    labels = action === 'labeled' ? ['ready-to-test'] : [],
    cancelError, rerunError, listError,
} = {}) {
    const calls = [];
    let attempt = 0;
    const state = () => states[Math.min(attempt, states.length - 1)];
    const listWorkflowRuns = () => {};
    const listJobsForWorkflowRun = () => {};
    const github = {
        rest: {
            pulls: {
                async get() {
                    return { data: {
                        number: 1, state: prState,
                        head: { sha: heads[Math.min(attempt, heads.length - 1)], ref: 'feature' },
                        labels: (state().labels ?? labels).map(name => ({ name })),
                    } };
                },
            },
            actions: {
                listWorkflowRuns,
                listJobsForWorkflowRun,
                async cancelWorkflowRun(params) {
                    calls.push(['cancel', params.run_id]);
                    if (cancelError) throw cancelError;
                },
                async reRunJobForWorkflowRun(params) {
                    calls.push(['rerun', params.job_id]);
                    if (rerunError) throw rerunError;
                },
            },
        },
        async paginate(method, params) {
            assert.equal(params.owner, 'owner');
            assert.equal(params.repo, 'repo');
            assert.equal(params.per_page, 100);
            if (method === listWorkflowRuns) {
                assert.equal(params.workflow_id, 'build-vcpkg.yml');
                assert.equal(params.event, 'pull_request');
                assert.equal(params.head_sha, 'head-sha');
                assert.equal(params.branch, 'feature');
                if (listError) throw listError;
                return [...state().runs];
            }
            assert.equal(method, listJobsForWorkflowRun);
            assert.equal(params.run_id, 10);
            assert.equal(params.filter, 'all');
            return [...state().jobs];
        },
    };
    const context = {
        repo: { owner: 'owner', repo: 'repo' }, issue: { number: 1 },
        payload: { action, pull_request: { head: { sha: 'head-sha' } } },
    };
    const execute = new AsyncFunction('github', 'context', 'core', 'setTimeout',
        workflowScript('rerun-triage', labelWorkflow));
    await execute(github, context, { info() {} }, callback => {
        attempt++;
        calls.push(['wait']);
        callback();
    });
    return calls;
}

test('label handler reruns only the newest triage job in the latest matching PR build', async () => {
    const runs = [
        buildRun({ id: 100, head_sha: 'old-head' }),
        buildRun({ id: 101, head_branch: 'other-branch' }),
        buildRun({ id: 102, pull_requests: [{ number: 2 }] }),
        buildRun({ id: 9 }),
        buildRun(),
    ];
    const jobs = [...buildJobs(), { id: 21, name: 'triage', status: 'completed' }];
    assert.deepEqual(await runLabelHandler({ states: [{ runs, jobs }] }), [['rerun', 21]]);
    assert.deepEqual(await runLabelHandler({
        states: [{ runs: [buildRun({ pull_requests: [] })], jobs: buildJobs() }],
    }), [['rerun', 20]]);
});

test('adding ready-to-test preserves a build after successful triage but reruns a failed gate', async () => {
    assert.deepEqual(await runLabelHandler({ action: 'labeled' }), []);
    assert.deepEqual(await runLabelHandler({
        action: 'labeled',
        states: [{
            runs: [buildRun({ status: 'in_progress' })],
            jobs: buildJobs(),
        }],
    }), []);
    assert.deepEqual(await runLabelHandler({
        action: 'labeled',
        states: [{ runs: [buildRun()], jobs: buildJobs('completed', 'failure') }],
    }), [['rerun', 20]]);
});

test('adding ready-to-test recovers a cancelled build even when retained triage succeeded', async () => {
    assert.deepEqual(await runLabelHandler({
        action: 'labeled',
        states: [{ runs: [buildRun({ conclusion: 'cancelled' })], jobs: buildJobs() }],
    }), [['rerun', 20]]);
});

test('label changes during cancellation cannot abandon the pending triage rerun', async () => {
    const active = buildRun({ status: 'in_progress' });
    assert.deepEqual(await runLabelHandler({ states: [
        { runs: [active], jobs: buildJobs() },
        { runs: [active], jobs: buildJobs(), labels: ['ready-to-test'] },
        { runs: [buildRun({ conclusion: 'cancelled' })], jobs: buildJobs(), labels: ['ready-to-test'] },
    ] }), [['cancel', 10], ['wait'], ['wait'], ['rerun', 20]]);
});

test('queued label handlers use live labels rather than the original event action', async () => {
    assert.deepEqual(await runLabelHandler({
        action: 'unlabeled', labels: ['ready-to-test'],
    }), []);
    assert.deepEqual(await runLabelHandler({
        action: 'labeled', labels: [],
        states: [
            { runs: [buildRun({ status: 'in_progress' })], jobs: buildJobs() },
            { runs: [buildRun({ conclusion: 'cancelled' })], jobs: buildJobs() },
        ],
    }), [['cancel', 10], ['wait'], ['rerun', 20]]);
});

test('label handler waits for prerequisites, cancels running descendants, then reruns triage', async () => {
    const active = buildRun({ status: 'in_progress' });
    const incompleteJobs = buildJobs();
    incompleteJobs.find(job => job.name === 'analyze-changes').status = 'in_progress';
    const calls = await runLabelHandler({ states: [
        { runs: [active], jobs: incompleteJobs },
        { runs: [active], jobs: buildJobs('in_progress') },
        { runs: [active], jobs: buildJobs() },
        { runs: [active], jobs: buildJobs() },
        { runs: [buildRun()], jobs: buildJobs() },
    ] });
    assert.deepEqual(calls, [
        ['wait'], ['wait'], ['cancel', 10], ['wait'], ['wait'], ['rerun', 20],
    ]);
});

test('label handler tolerates a cancellation/completion race, but surfaces other API errors', async () => {
    assert.deepEqual(await runLabelHandler({
        states: [
            { runs: [buildRun({ status: 'in_progress' })], jobs: buildJobs() },
            { runs: [buildRun()], jobs: buildJobs() },
        ],
        cancelError: Object.assign(new Error('Already completed'), { status: 409 }),
    }), [['cancel', 10], ['wait'], ['rerun', 20]]);
    for (const options of [
        { rerunError: new Error('Rerun forbidden') },
        { listError: new Error('API unavailable') },
        {
            states: [{ runs: [buildRun({ status: 'in_progress' })], jobs: buildJobs() }],
            cancelError: Object.assign(new Error('Cancel forbidden'), { status: 403 }),
        },
    ]) {
        await assert.rejects(runLabelHandler(options), /forbidden|unavailable/);
    }
});

test('label handler ignores superseded events and explicitly fails missing builds or triage jobs', async () => {
    assert.deepEqual(await runLabelHandler({ heads: ['new-head'] }), []);
    assert.deepEqual(await runLabelHandler({ prState: 'closed' }), []);
    assert.deepEqual(await runLabelHandler({
        states: [{ runs: [buildRun({ status: 'in_progress' })], jobs: buildJobs('queued') }],
        heads: ['head-sha', 'new-head'],
    }), [['wait']]);
    assert.deepEqual(await runLabelHandler({ states: [
        { runs: [], jobs: [] }, { runs: [buildRun()], jobs: buildJobs() },
    ] }), [['wait'], ['rerun', 20]]);
    await assert.rejects(runLabelHandler({ states: [{ runs: [], jobs: [] }] }), /Timed out/);
    await assert.rejects(runLabelHandler({ states: [{ runs: [buildRun()], jobs: [] }] }), /no triage job/);
});
