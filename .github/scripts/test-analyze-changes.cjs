const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { dirname, join } = require('node:path');
const { test } = require('node:test');

const action = readFileSync(`${__dirname}/../actions/analyze-changes/action.yaml`, 'utf8');
const script = action.split('      run: |\n')[1]
    .split('\n').map(line => line.replace(/^ {8}/, '')).join('\n');

function analyze(paths, { unknownBase = false, contents = {}, extraCategories = '' } = {}) {
    const directory = mkdtempSync(join(tmpdir(), 'hpcc-analyze-changes-'));
    const git = (...args) => execFileSync('git', [
        '-c', 'user.name=Change analyzer test',
        '-c', 'user.email=change-analyzer@example.invalid',
        '-c', 'commit.gpgsign=false',
        ...args,
    ], { cwd: directory, encoding: 'utf8' }).trim();
    try {
        git('init', '--quiet');
        git('commit', '--quiet', '--allow-empty', '-m', 'Base');
        const base = git('rev-parse', 'HEAD');
        for (const path of paths) {
            const filename = join(directory, path);
            mkdirSync(dirname(filename), { recursive: true });
            writeFileSync(filename, contents[path] ?? 'test content\n');
        }
        git('add', '--', '.');
        git('commit', '--quiet', '-m', 'Change fixtures');
        const output = join(directory, 'action-output');
        const configuredScript = script.replace('\nconfigure_changes\n',
            `\nconfigure_changes\n${extraCategories}\n`);
        execFileSync('bash', ['-c', configuredScript], {
            cwd: directory,
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'pipe'],
            env: {
                ...process.env,
                BASE_SHA: unknownBase ? '0'.repeat(40) : base,
                HEAD_SHA: git('rev-parse', 'HEAD'),
                GITHUB_OUTPUT: output,
            },
        });
        return Object.fromEntries(readFileSync(output, 'utf8').trim().split('\n')
            .map(line => {
                const separator = line.indexOf('=');
                return [line.slice(0, separator), JSON.parse(line.slice(separator + 1))];
            }));
    } finally {
        rmSync(directory, { recursive: true, force: true });
    }
}

test('generator inputs report codegen and select compiled rather than ECL', () => {
    for (const path of ['ecl/hql/example.l', 'ecl/hql/example.y', 'esp/services/example.ecm']) {
        const { changes, impacts } = analyze([path]);
        assert.deepEqual(changes['has-type'], { codegen: true });
        assert.equal(impacts.compiled, true, path);
        assert.equal(impacts['regression-suite'], false, path);
    }
    assert.equal(analyze(['system/jlib/example.cpp']).impacts.compiled, true);
    assert.equal(analyze(['ecllibrary/example.ecl']).impacts.compiled, false);
    assert.equal(analyze(['CMakeLists.txt']).impacts.compiled, false);
});

test('compiled ignores apparent C++ comment-only changes but never filters codegen comments', () => {
    const cppPath = 'system/jlib/example.cpp';
    const cpp = analyze([cppPath], { contents: { [cppPath]: '// comment only\n' } });
    assert.equal(cpp.changes['has-type'].cpp, true);
    assert.equal(cpp.impacts.compiled, false);
    assert.equal(cpp.impacts.platform, false);

    for (const extension of ['l', 'y', 'ecm']) {
        const path = `ecl/hql/example.${extension}`;
        const generated = analyze([path], { contents: { [path]: '// generator input\n' } });
        assert.equal(generated.impacts.compiled, true, path);
        assert.equal(generated.impacts.roxie, true, path);
    }
});

test('C++ engine filters remain component-specific while codegen triggers all codegen consumers', () => {
    const engines = ['roxie', 'thor', 'hthor', 'esp', 'dali'];
    for (const [prefix, expectedCpp] of [
        ['roxie', [true, false, false, false, false]],
        ['thorlcr', [false, true, false, false, false]],
        ['esp/services', [false, false, true, true, false]],
        ['system/jlib', [true, true, true, true, true]],
        ['dali/base', [true, true, true, true, true]],
    ]) {
        const cpp = analyze([`${prefix}/example.cpp`]);
        const codegen = analyze([`${prefix}/example.ecm`]);
        assert.deepEqual(engines.map(name => cpp.impacts[name]), expectedCpp, prefix);
        assert.deepEqual(engines.map(name => codegen.impacts[name]),
            [true, true, true, true, false], prefix);
    }
    const mixed = analyze(['thorlcr/example.cpp', 'esp/services/example.ecm']);
    assert.equal(mixed.impacts.compiled, true);
    assert.equal(mixed.impacts.roxie, true);
    assert.equal(mixed.impacts.thor, true);
    assert.equal(mixed.impacts.esp, true);
    assert.equal(mixed.impacts.dali, false);
});

test('documentation build inputs do not select codebuild or engine tests, even with other platform changes', () => {
    const engines = ['roxie', 'thor', 'hthor', 'esp', 'dali'];
    for (const path of ['docs/CMakeLists.txt', 'aidoc/CMakeLists.txt']) {
        const { changes, impacts } = analyze([path]);
        assert.deepEqual(changes['has-type'], { build: true });
        assert.equal(impacts.docs, path.startsWith('docs/'));
        assert.equal(impacts.codebuild, false, path);
        assert.deepEqual(engines.map(name => impacts[name]), [false, false, false, false, false], path);

        const mixed = analyze([path, 'configuration/example.json']).impacts;
        assert.equal(mixed.platform, true, path);
        assert.equal(mixed.codebuild, false, path);
        assert.deepEqual(engines.map(name => mixed[name]), [false, false, false, false, false], path);
    }
});

test('codebuild still selects all engine tests for non-documentation build inputs', () => {
    const engines = ['roxie', 'thor', 'hthor', 'esp', 'dali'];
    for (const paths of [
        ['CMakeLists.txt'],
        ['cmake_modules/example.cmake', 'docs/CMakeLists.txt', 'aidoc/CMakeLists.txt'],
    ]) {
        const { impacts } = analyze(paths);
        assert.equal(impacts.codebuild, true);
        assert.deepEqual(engines.map(name => impacts[name]), [true, true, true, true, true]);
    }
});

test('category references support declaration-order chaining and OR with local rules', () => {
    const extraCategories = `
declare_category test-source "@compiled"
declare_category test-mixed "#config" "configuration" "@test-source"
declare_category test-chain "@test-mixed"
`;
    for (const [path, source, mixed] of [
        ['ecl/hql/example.y', true, true],
        ['configuration/example.json', false, true],
        ['README.md', false, false],
    ]) {
        const { impacts } = analyze([path], { extraCategories });
        assert.equal(impacts['test-source'], source, path);
        assert.equal(impacts['test-mixed'], mixed, path);
        assert.equal(impacts['test-chain'], mixed, path);
    }
});

test('undefined, forward and self references and duplicate declarations fail immediately', () => {
    for (const [extraCategories, message] of [
        ['declare_category test-invalid "#cpp" "@missing"', /undefined category 'missing'/],
        ['declare_category test-self "#cpp" "@test-self"', /undefined category 'test-self'/],
        [`declare_category test-a "@compiled" "@test-b"
declare_category test-b "@compiled"`, /undefined category 'test-b'/],
        [`declare_category test-duplicate "@compiled"
declare_category test-duplicate "#cpp"`, /Duplicate category declaration 'test-duplicate'/],
        [`declare_category test-empty-definition
declare_category test-empty-definition "#cpp"`, /Duplicate category declaration 'test-empty-definition'/],
        ['declare_category test-empty "@"', /empty category reference/],
        ['declare_category test-filter "@compiled" "shared"', /Component listed before type filter/],
    ]) {
        assert.throws(() => analyze(['system/jlib/example.cpp'], { extraCategories }), error => {
            assert.match(error.stderr.toString(), message);
            return true;
        });
    }
});

test('regression-suite matches regression cases, runner scripts, and expected results', () => {
    for (const path of [
        'testing/regress/ecl/example.ecl',
        'testing/regress/hpcc/runner.py',
        'testing/regress/ecl/key/example.xml',
    ]) {
        const { changes, impacts } = analyze([path]);
        assert.deepEqual(changes['has-component'], { regress: true });
        assert.equal(impacts['regression-suite'], true);
        assert.equal(impacts.unittests, false);
        assert.equal(impacts.docs, false);
        assert.equal(impacts.platform, false);
    }
});

test('compiler regressions and older testing trees do not trigger regression-suite', () => {
    const compiler = analyze(['ecl/regress/example.ecl']);
    assert.deepEqual(compiler.changes['has-component'], { eclccregress: true });
    assert.equal(compiler.impacts['regression-suite'], false);
    assert.equal(compiler.impacts.unittests, false);

    const older = analyze(['testing/ecl/roxie/example.ecl']);
    assert.deepEqual(older.changes['has-component'], { testing: true });
    assert.equal(older.impacts['regression-suite'], false);
    assert.equal(older.impacts.unittests, true);
    assert.equal(older.impacts.platform, true);
});

test('ECL library changes trigger regression-suite without selecting unrelated tests', () => {
    for (const path of [
        'ecllibrary/example.ecl',
        'ecllibrary/std/example.ecl',
        'ecllibrary/teststd/example.ecl',
    ]) {
        const { impacts } = analyze([path]);
        assert.equal(impacts['regression-suite'], true, path);
        assert.equal(impacts.platform, true, path);
        assert.equal(impacts.unittests, false, path);
        assert.equal(impacts.docs, false, path);
    }
});

test('plugin ECL changes trigger regression-suite without selecting other plugin changes', () => {
    const ecl = analyze(['plugins/h3/lib_h3.ecllib']);
    assert.equal(ecl.impacts['regression-suite'], true);
    assert.equal(ecl.impacts.platform, true);

    const nonEcl = analyze(['plugins/h3/plugin.cpp']);
    assert.equal(nonEcl.impacts['regression-suite'], false);
});

test('ECL and JSON changes outside library and regression trees do not trigger regression-suite', () => {
    for (const path of [
        'ecl/example.ecl',
        'configuration/example.json',
    ]) {
        const { impacts } = analyze([path]);
        assert.equal(impacts['regression-suite'], false);
        assert.equal(impacts.unittests, false);
        assert.equal(impacts.docs, false);
    }
});

test('Java changes outside regress still trigger regression-suite', () => {
    const { changes, impacts } = analyze(['plugins/example.java']);
    assert.deepEqual(changes['has-component'], { plugins: true });
    assert.equal(impacts['regression-suite'], true);
    assert.equal(impacts.unittests, false);
    assert.equal(impacts.docs, false);
});

test('unittests follows testing while mixed test changes trigger both test categories', () => {
    const unit = analyze(['testing/unittests/example.cpp']);
    assert.deepEqual(unit.changes['has-component'], { testing: true });
    assert.equal(unit.impacts.unittests, true);
    assert.equal(unit.impacts['regression-suite'], false);

    const mixed = analyze(['testing/unittests/example.cpp', 'testing/regress/ecl/example.ecl']);
    assert.equal(mixed.impacts.unittests, true);
    assert.equal(mixed.impacts['regression-suite'], true);
});

test('docs includes existing documentation trees and root Markdown documentation', () => {
    for (const path of [
        'README.md',
        'BUILD_ME.md',
        'index.md',
        'docs/EN_US/example.xml',
        'devdoc/example.md',
        'esp/esdllib/docs/example.md',
        'esp/src/dgrid/doc/example.md',
        'helm/hpcc/docs/example.md',
    ]) {
        const { changes, impacts } = analyze([path]);
        assert.deepEqual(changes['has-component'], { docs: true });
        assert.equal(impacts.docs, true);
        assert.equal(impacts.platform, false);
        assert.equal(impacts.eclwatch, false);
        assert.equal(impacts.unittests, false);
        assert.equal(impacts['regression-suite'], false);
    }
});

test('the documentation-build workflow is CI/CD rather than documentation or a package input', () => {
    const { changes, impacts } = analyze(['.github/workflows/test-documentation.yml']);
    assert.deepEqual(changes['has-component'], { cicd: true });
    assert.equal(impacts.docs, false);
    assert.equal(impacts.platform, false);
});

test('frontend and unrelated workflow changes do not trigger docs or test categories', () => {
    for (const path of ['esp/src/Example.tsx', '.github/workflows/build-vcpkg.yml']) {
        const { impacts } = analyze([path]);
        assert.equal(impacts.docs, false);
        assert.equal(impacts.unittests, false);
        assert.equal(impacts['regression-suite'], false);
    }
});

test('platform covers package components and conservatively includes unknown paths', () => {
    for (const path of [
        'system/jlib/example.cpp', 'lib2/example.hpp', 'unmapped/example.ecm',
        'testing/unittests/example.cpp', 'cmake_modules/example.cmake', 'package/example.xml',
        'initfiles/etc/example.conf', 'configuration/example.json',
        'vcpkg', 'vcpkg_overlays/example/portfile.cmake',
        'vcpkg.json.in', 'vcpkg-configuration.json',
        'services/example.xml', 'clienttools/example.sh', 'ecllibrary/example.ecl',
        'plugins/example/example.ecllib', 'common/example.md', 'unmapped/example.asset',
        'vcpkg-linux.code-workspace', 'version.cmake.backup',
    ]) {
        const { impacts } = analyze([path]);
        assert.equal(impacts.platform, true, path);
    }
});

test('platform includes build inputs but excludes non-package components regardless of type', () => {
    for (const path of [
        'CMakeLists.txt', 'version.cmake', 'build-config.h.cmake',
    ]) {
        assert.equal(analyze([path]).impacts.platform, true, path);
    }
    for (const path of [
        '.github/workflows/build-vcpkg.yml', '.github/workflows/build-docker.yml',
        '.github/workflows/build-gh_runner.yml',
        '.github/workflows/notify.yml', 'helm/hpcc/values.yaml',
        '.github/actions/download-eclwatch-nightly/action.yml',
        'README.md', 'BUILD_ME.md', 'index.md',
        'devdoc/example.cpp', 'docs/CMakeLists.txt', 'aidoc/example.json',
        'esp/esdllib/docs/example.xml', 'esp/src/dgrid/doc/example.md',
        'helm/hpcc/example.cpp', 'testing/regress/CMakeLists.txt', 'ecl/regress/example.cpp',
        '.github/workflows/build-docker.yml.backup',
    ]) {
        assert.equal(analyze([path]).impacts.platform, false, path);
    }
});

test('platform still triggers when package changes accompany excluded changes', () => {
    assert.equal(analyze([
        'helm/hpcc/values.yaml', 'docs/CMakeLists.txt',
        '.github/workflows/build-vcpkg.yml', 'ecllibrary/example.ecl',
    ]).impacts.platform, true);
});

test('all dockerfiles paths trigger platform regardless of filename, type or nesting', () => {
    for (const path of [
        'dockerfiles/ubuntu-22.04.dockerfile',
        'dockerfiles/build.sh',
        'dockerfiles/examples/example/Dockerfile',
        'dockerfiles/README.md',
    ]) {
        const { changes, impacts } = analyze([path]);
        assert.deepEqual(changes['has-component'], { container: true });
        assert.equal(impacts.platform, true, path);
    }
});

test('frontend components, including a submodule pointer, select eclwatch and platform builds', () => {
    for (const path of ['esp/src/Example.tsx', 'esp/eclwatch/example.css', 'esp/src/dgrid']) {
        const { impacts } = analyze([path]);
        assert.equal(impacts.eclwatch, true, path);
        assert.equal(impacts.platform, true, path);
    }
});

test('an unknown base still conservatively triggers regression-suite with the new component', () => {
    const { impacts } = analyze(['configuration/example.json'], { unknownBase: true });
    assert.equal(impacts['regression-suite'], true);
    assert.equal(impacts.unittests, true);
    assert.equal(impacts.docs, true);
    assert.equal(impacts.platform, true);
    assert.equal(impacts.eclwatch, true);
});
