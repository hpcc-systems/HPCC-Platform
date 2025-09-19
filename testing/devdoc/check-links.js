#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const args = process.argv.slice(2);

function parseArgs(argList) {
    const out = { root: 'devdoc/.vitepress/dist', basePrefix: '/HPCC-Platform' };
    for (let i = 0; i < argList.length; ++i) {
        const a = argList[i];
        if (a === '--root') out.root = argList[++i];
        else if (a === '--base-prefix') out.basePrefix = (argList[++i] || '').trim();
        else if (a === '--help' || a === '-h') {
            console.log(`Usage: node devdoc/scripts/check-links.js [options]\n\n` +
                `Options:\n` +
                `  --root <dir>       Root dist directory (default devdoc/.vitepress/dist)\n` +
                `  --base-prefix <p>  Site base prefix for repo lookup (default /HPCC-Platform)\n` +
                `  -h, --help         Show help\n`);
            process.exit(0);
        } else {
            console.warn(`Unknown argument (ignored): ${a}`);
        }
    }
    return out;
}

const options = parseArgs(args);
const rootDir = path.resolve(process.cwd(), options.root);

let ignoreRegexes = [];

const GLOB_DOUBLE_STAR_PLACEHOLDER = '__GLOB_DOUBLE_STAR__';
function globToRegex(glob) {
    let re = glob.replace(/[-/\\^$+?.()|{}\[\]]/g, '\\$&');
    re = re.replace(/\*\*/g, GLOB_DOUBLE_STAR_PLACEHOLDER);
    re = re.replace(/\*/g, '[^/]*');
    re = re.replace(/\?/g, '[^/]');
    re = re.replace(new RegExp(GLOB_DOUBLE_STAR_PLACEHOLDER, 'g'), '.*');
    return new RegExp('^' + re + '$');
}

const includeRegexes = [globToRegex('**/*.html')];
const excludeRegexes = [];

function walk(dir, fileList = []) {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const e of entries) {
        const full = path.join(dir, e.name);
        const rel = path.relative(rootDir, full).replace(/\\/g, '/');
        if (e.isDirectory()) {
            walk(full, fileList);
        } else if (e.isFile()) {
            const inc = includeRegexes.some(r => r.test(rel));
            const exc = excludeRegexes.some(r => r.test(rel));
            if (inc && !exc) fileList.push(full);
        }
    }
    return fileList;
}

if (!fs.existsSync(rootDir)) {
    console.error(`Root directory does not exist: ${rootDir}`);
    process.exit(2);
}

function readHtml(file) {
    try { return fs.readFileSync(file, 'utf8'); } catch (e) { return ''; }
}

const allIdsPerFile = new Map();

function extractIds(html) {
    const idSet = new Set();
    const idRegex = /id\s*=\s*"([^"]+)"/g;
    let m;
    while ((m = idRegex.exec(html))) idSet.add(m[1]);
    return idSet;
}

const htmlFiles = walk(rootDir);
for (const f of htmlFiles) {
    const html = readHtml(f);
    allIdsPerFile.set(f, extractIds(html));
}

function isExternalLink(href) {
    return /^(?:[a-zA-Z][a-zA-Z0-9+.-]*:)?\/\//.test(href) || /^(?:mailto:|tel:)/.test(href) || href.startsWith('data:');
}

function normalizePath(baseFile, target) {
    if (target.startsWith('/')) {
        // treat as site root relative. The initial join keeps any basePrefix which may be stripped later in existence logic.
        return path.join(rootDir, target.replace(/^\/+/, ''));
    }
    return path.resolve(path.dirname(baseFile), target);
}

function stripHash(href) {
    const i = href.indexOf('#');
    return i >= 0 ? href.slice(0, i) : href;
}

function getHash(href) {
    const i = href.indexOf('#');
    return i >= 0 ? href.slice(i + 1) : undefined;
}

function extractLinks(html) {
    const results = [];
    // anchor tags
    const anchorRegex = /<a\s+[^>]*href\s*=\s*"([^"]+)"/gi;
    let m;
    while ((m = anchorRegex.exec(html))) results.push(m[1]);
    // image tags
    const imgRegex = /<img\s+[^>]*src\s*=\s*"([^"]+)"/gi;
    while ((m = imgRegex.exec(html))) results.push(m[1]);
    return results;
}

const broken = new Map(); // key -> { pages: Set, reasons: Set }

// Pre-compute mapping from html pages back to probable markdown sources.
// Heuristics:
//   index.html => README.md | index.md in same directory
//   *.html     => *.md (case variants) in same directory
//   Special case for paths starting with 'devdoc/': also try within devdoc/ root without duplicating 'devdoc/' prefix.
const htmlToSources = new Map(); // relHtml -> [relSourceMd...]

// Cache markdown file contents split into lines for quick lookup
const markdownCache = new Map(); // relPath -> [lines]

function getMarkdownLines(relPath) {
    if (markdownCache.has(relPath)) return markdownCache.get(relPath);
    try {
        const abs = path.join(process.cwd(), relPath);
        const txt = fs.readFileSync(abs, 'utf8');
        const lines = txt.split(/\r?\n/);
        markdownCache.set(relPath, lines);
        return lines;
    } catch (_) {
        markdownCache.set(relPath, []);
        return [];
    }
}

// Try to locate the first occurrence of a link in a markdown file and return {line,col}
// Strategy:
//  1. Build candidate raw patterns for the link target inside () or []()
//  2. Scan lines for the first pattern occurrence.
//  3. Column is 1-based index of first character of URL inside the markdown line.
function locateLinkInMarkdown(relMarkdown, linkTarget, basePrefix) {
    const lines = getMarkdownLines(relMarkdown);
    if (!lines.length) return { line: 1, col: 1, found: false };

    const hashIdx = linkTarget.indexOf('#');
    const bare = hashIdx >= 0 ? linkTarget.slice(0, hashIdx) : linkTarget;

    const candidates = new Set();
    const add = (v) => { if (v && v.length < 4096) candidates.add(v); };

    add(linkTarget);
    add(bare);

    if (bare.endsWith('.html')) add(bare.slice(0, -5) + '.md');

    if (bare.startsWith('./')) add(bare.slice(2));
    if (!bare.startsWith('/') && !bare.startsWith('./')) add('./' + bare);

    let simplified = bare.replace(/\\+/g, '/');
    simplified = simplified.replace(/\.\/+/g, ''); // leading ./ segments
    simplified = simplified.replace(/(\/|^)\.\//g, '$1'); // internal ./
    simplified = simplified.replace(/\/\.\//g, '/');
    simplified = simplified.replace(/\.\.\/\.\//g, '../');
    add(simplified);
    if (simplified.endsWith('.html')) add(simplified.slice(0, -5) + '.md');
    if (simplified.startsWith('./')) add(simplified.slice(2));

    for (const c of Array.from(candidates)) {
        if (c.endsWith('/')) add(c.slice(0, -1)); else add(c + '/');
    }

    if (basePrefix) {
        const normBase = '/' + basePrefix.replace(/^\/+/, '').replace(/\/+$/, '') + '/';
        if (bare.startsWith(normBase)) {
            const stripped = '/' + bare.slice(normBase.length); // ensure leading slash
            add(stripped);
            if (stripped === '/') {
                // Root link likely authored as (/)
                add('/');
                add('(/)'); // pattern inside markdown link
            }
        }
    }

    for (const c of Array.from(candidates)) {
        if (c.includes('../')) {
            const compact = path.posix.normalize(c);
            add(compact);
        }
    }

    const searchTokens = new Set();
    for (const c of candidates) {
        searchTokens.add(c);
        // For root link avoid raw '/' only unless we cannot find other matches; we'll prioritize '(/)'
        if (c === '/') searchTokens.add('(/)');
        if (!c.startsWith('(')) searchTokens.add('(' + c + ')');
        if (!c.startsWith('./') && !c.startsWith('/') && !c.startsWith('(')) {
            searchTokens.add('(' + './' + c + ')');
        }
    }

    for (let i = 0; i < lines.length; ++i) {
        const line = lines[i];
        for (const token of searchTokens) {
            const idx = line.indexOf(token);
            if (idx !== -1) {
                return { line: i + 1, col: idx + 1, found: true };
            }
        }
    }

    const loweredTokens = Array.from(searchTokens).map(t => t.toLowerCase());
    for (let i = 0; i < lines.length; ++i) {
        const lowerLine = lines[i].toLowerCase();
        for (const token of loweredTokens) {
            const idx = lowerLine.indexOf(token);
            if (idx !== -1) {
                return { line: i + 1, col: idx + 1, found: true };
            }
        }
    }

    const lastSlash = bare.lastIndexOf('/') >= 0 ? bare.lastIndexOf('/') + 1 : 0;
    let baseName = bare.slice(lastSlash);
    if (baseName.endsWith('.html')) baseName = baseName.slice(0, -5) + '.md';
    if (!baseName.endsWith('.md')) baseName = baseName + '.md';
    for (let i = 0; i < lines.length; ++i) {
        const line = lines[i];
        const idx = line.indexOf(baseName);
        if (idx !== -1) return { line: i + 1, col: idx + 1, found: true };
    }

    return { line: 1, col: 1, found: false };
}

// Choose the most appropriate markdown source file for a generated html page
// Preference order:
//   1. README.md (case-insensitive) if present (commonly source for index.html)
//   2. File whose basename matches the html basename (foo.html -> foo.md)
//   3. First candidate returned by mapping heuristic
function chooseSourceForPage(relHtml) {
    const sources = htmlToSources.get(relHtml) || [];
    if (!sources.length) return relHtml; // fallback to html if no mapping
    // Prefer README.md variants
    const readme = sources.find(s => /\/readme\.md$/i.test(s));
    if (readme) return readme;
    const htmlBase = relHtml.toLowerCase().endsWith('.html') ? relHtml.slice(0, -5) : relHtml;
    const baseName = htmlBase.substring(htmlBase.lastIndexOf('/') + 1);
    const match = sources.find(s => {
        const stem = s.toLowerCase().endsWith('.md') ? s.slice(0, -3) : s;
        return stem.substring(stem.lastIndexOf('/') + 1).toLowerCase() === baseName.toLowerCase();
    });
    if (match) return match;
    return sources[0];
}

function findSourceMarkdown(relHtml) {
    const results = new Set();
    const repoRoot = process.cwd();
    const dir = path.posix.dirname(relHtml);
    const base = path.posix.basename(relHtml);
    const candidates = [];
    if (/^(?:index|readme)\.html$/i.test(base)) {
        candidates.push('README.md', 'readme.md', 'index.md');
    } else if (base.toLowerCase().endsWith('.html')) {
        const stem = base.slice(0, -5); // remove .html
        candidates.push(stem + '.md', stem.toLowerCase() + '.md');
    }

    function tryAdd(rootPrefix, subDir, file) {
        const full = path.join(repoRoot, rootPrefix, subDir === '.' ? '' : subDir, file);
        if (fs.existsSync(full)) {
            const rel = path.relative(repoRoot, full).replace(/\\/g, '/');
            results.add(rel);
        }
    }

    const relDirNormalized = dir === '.' ? '' : dir;
    for (const c of candidates) {
        // Generic repo-root attempt
        tryAdd('', relDirNormalized, c);
    }

    if (relHtml.startsWith('devdoc/')) {
        const subPath = relHtml.substring('devdoc/'.length);
        const subDir = path.posix.dirname(subPath);
        for (const c of candidates) {
            tryAdd('devdoc', subDir === '.' ? '' : subDir, c);
        }
    }
    return Array.from(results);
}

function buildSourceMap() {
    for (const file of htmlFiles) {
        const relFile = path.relative(rootDir, file).replace(/\\/g, '/');
        htmlToSources.set(relFile, findSourceMarkdown(relFile));
    }
}

buildSourceMap();

for (const file of htmlFiles) {
    const html = readHtml(file);
    const relFile = path.relative(rootDir, file).replace(/\\/g, '/');
    const links = extractLinks(html);
    for (const href of links) {
        if (!href || href.startsWith('#') || isExternalLink(href)) continue;
        if (/^javascript:/i.test(href)) continue;
        if (href.startsWith('?')) continue;

        const baseNoHash = stripHash(href);
        const hash = getHash(href);

        if (!baseNoHash) continue;

        if (ignoreRegexes.length && ignoreRegexes.some(r => r.test(href))) {
            continue;
        }

        const normalized = normalizePath(file, baseNoHash);
        let exists = fs.existsSync(normalized);
        let outsideMdMapped = false;
        let baseStrippedPath; // candidate after stripping basePrefix

        if (!exists && baseNoHash.startsWith('/')) {
            let sitePath = baseNoHash; // e.g. /HPCC-Platform/assets/img.png or /HPCC-Platform/
            let strippedForDist = sitePath;
            if (options.basePrefix) {
                const bpNoSlashes = options.basePrefix.replace(/^\/+/, '').replace(/\/+$/, '');
                const bpPattern = '/' + bpNoSlashes + '/';
                if (sitePath === '/' + bpNoSlashes || sitePath === bpPattern) {
                    // root of site
                    strippedForDist = '/';
                } else if (sitePath.startsWith(bpPattern)) {
                    strippedForDist = '/' + sitePath.slice(bpPattern.length); // retain leading '/'
                }
            }
            const distRel = strippedForDist.replace(/^\/+/, '');
            if (distRel === '') {
                if (fs.existsSync(path.join(rootDir, 'index.html'))) {
                    exists = true;
                }
            } else {
                const candidate = path.join(rootDir, distRel);
                if (fs.existsSync(candidate)) {
                    exists = true;
                } else if (fs.existsSync(candidate + '.html')) {
                    exists = true;
                } else if (fs.existsSync(path.join(candidate, 'index.html'))) {
                    exists = true;
                }
            }
            if (!exists) {
                let repoSitePath = sitePath;
                if (options.basePrefix) {
                    const bp = options.basePrefix.endsWith('/') ? options.basePrefix : options.basePrefix + '/';
                    if (repoSitePath.startsWith(bp)) repoSitePath = '/' + repoSitePath.slice(bp.length);
                }
                let rel = repoSitePath.replace(/^\/+/, '');
                if (rel.endsWith('.html')) {
                    const mdCandidate = rel.slice(0, -5) + '.md';
                    const mdFull = path.join(process.cwd(), mdCandidate);
                    if (fs.existsSync(mdFull) && !mdFull.startsWith(rootDir)) {
                        exists = true;
                        outsideMdMapped = true;
                    }
                }
            }
        }

        if (!exists && fs.existsSync(normalized + '.html')) {
            exists = true;
        } else if (!exists && fs.existsSync(path.join(normalized, 'index.html'))) {
            exists = true;
        } else if (!exists && !path.extname(normalized)) {
            // Try adding .html
            if (fs.existsSync(normalized + '.html')) exists = true;
        }

        let anchorOk = true;
        if (exists && hash && !outsideMdMapped) {
            let targetFile = normalized;
            if (fs.existsSync(targetFile) && fs.statSync(targetFile).isDirectory()) {
                targetFile = path.join(targetFile, 'index.html');
            } else if (!fs.existsSync(targetFile) && fs.existsSync(targetFile + '.html')) {
                targetFile = targetFile + '.html';
            }
            if (fs.existsSync(targetFile) && path.extname(targetFile) === '.html') {
                const ids = allIdsPerFile.get(targetFile) || new Set();
                if (!ids.has(hash)) anchorOk = false;
            }
        }

        if (!exists || !anchorOk) {
            const key = href;
            if (!broken.has(key)) {
                broken.set(key, { pages: new Set(), reasons: new Set() });
            }
            const entry = broken.get(key);
            entry.pages.add(relFile);
            if (!exists) entry.reasons.add('missing-target');
            if (exists && !anchorOk) entry.reasons.add('missing-anchor');
        }
    }
}

if (broken.size === 0) {
    console.log('No broken links found.');
} else {
    const pageMap = new Map();
    for (const [link, info] of broken.entries()) {
        for (const page of info.pages) {
            if (!pageMap.has(page)) pageMap.set(page, []);
            pageMap.get(page).push({ link, reasons: Array.from(info.reasons).sort() });
        }
    }
    const pages = Array.from(pageMap.keys()).sort();
    for (const page of pages) {
        const list = pageMap.get(page);
        const problemFile = chooseSourceForPage(page);
        for (const item of list) {
            let line = 1, col = 1;
            if (problemFile.endsWith('.md')) {
                const loc = locateLinkInMarkdown(problemFile, item.link, options.basePrefix);
                line = loc.line; col = loc.col;
                if (!loc.found && !item.reasons.includes('unlocated')) item.reasons.push('unlocated');
            }
            console.log(`${problemFile}:${line}:${col}: error Broken link: ${item.link} (${item.reasons.join(',')})`);
        }
    }
}

process.exit(broken.size === 0 ? 0 : 1);
