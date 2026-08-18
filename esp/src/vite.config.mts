// Vite build for the src-react entry point.
//
// TypeScript 7 (tsc, tsconfig.json) and Vite compile and bundle the modern
// React app and legacy Dojo modules directly from source.  Keeping all entries in
// one module graph makes the Dojo registry and topic bus shared by the React
// adapters and legacy widgets.
import { ConfigEnv, defineConfig, Plugin, UserConfig } from "vite";
import react from "@vitejs/plugin-react";
import fs from "fs";
import path from "path";
import { pathToFileURL } from "url";

const distPublicPath = (process.env.ECLWATCH_DIST_URL || "/esp/files/dist/").replace(/\/?$/, "/");
const distUrl = distPublicPath.replace(/\/$/, "");
const distFolder = distUrl.split("/").filter(Boolean).pop() || "dist";
const outputPath = path.resolve(__dirname, `build/${distFolder}`);

const wasmDuckdbPath = path.resolve(__dirname, "../../../hpcc-js-wasm/packages/duckdb");
const wasmGraphvizPath = path.resolve(__dirname, "../../../hpcc-js-wasm/packages/graphviz");
const hpccjsPath = path.resolve(__dirname, "../../../hpcc-js/packages");
const visualizationPath = path.resolve(__dirname, "../../../Visualization/packages");
const nodeModulesVitePluginsEntry = path.resolve(__dirname, "node_modules/@hpcc-js/vite-plugins/dist/index.js");
const siblingVitePluginsEntry = path.resolve(__dirname, "../../../Visualization/packages/vite-plugins/dist/index.js");

type DojoFactory = (opts: Record<string, unknown>) => Plugin;

async function loadDojoFactory(): Promise<DojoFactory> {
    for (const entry of [nodeModulesVitePluginsEntry, siblingVitePluginsEntry]) {
        if (!fs.existsSync(entry)) continue;
        const mod = await import(pathToFileURL(entry).href);
        if (typeof mod.dojo === "function") {
            return mod.dojo as DojoFactory;
        }
    }

    throw new Error(`Unable to resolve dojo plugin from ${nodeModulesVitePluginsEntry} or ${siblingVitePluginsEntry}`);
}

// Redirect @hpcc-js/* imports to a sibling monorepo checkout only when that
// checkout has an actual built dist/index.js; otherwise resolve from node_modules.
function hpccJsSiblingFallback(): Plugin {
    const wasmPackages: Record<string, string> = {
        "@hpcc-js/wasm-duckdb": wasmDuckdbPath,
        "@hpcc-js/wasm-graphviz": wasmGraphvizPath
    };
    const monorepoBases = [hpccjsPath, visualizationPath].filter(dir => fs.existsSync(dir));

    return {
        name: "hpcc-js-sibling-fallback",
        enforce: "pre",
        resolveId(source) {
            const builtEntry = (dir: string) => {
                const candidates = [
                    path.join(dir, "dist", "browser", "index.js"),
                    path.join(dir, "dist", "index.js")
                ];
                return candidates.find(full => fs.existsSync(full));
            };

            if (wasmPackages[source]) {
                return builtEntry(wasmPackages[source]);
            }

            const match = /^@hpcc-js\/(.+)$/.exec(source);
            if (match) {
                for (const base of monorepoBases) {
                    const resolved = builtEntry(path.join(base, match[1]));
                    if (resolved) return resolved;
                }
            }
            return null;
        }
    };
}

// Read the HPCC backend target from LWS_TARGET env var or lws.target.txt (same
// logic as lws.config.js so the two dev setups stay in sync).
function readDevTarget(): string {
    if (process.env.LWS_TARGET) {
        const t = process.env.LWS_TARGET;
        return t.includes("://") ? t : `http://${t}:8010`;
    }
    try {
        const raw = fs.readFileSync(path.resolve(__dirname, "lws.target.txt"), "utf8")
            .replace(/\r\n/g, "\n").split("\n")[0].trim();
        if (raw) return raw.includes("://") ? raw : `http://${raw}:8010`;
    } catch { /* fall through to default */ }
    return "http://localhost:8010";
}

// Serve built artifacts at their /esp/files/... URL paths so that Vite's dev
// server can stand alone without needing lws in front of it.
function devStaticAssets(): Plugin {
    const mimeTypes: Record<string, string> = {
        ".css": "text/css",
        ".eot": "application/vnd.ms-fontobject",
        ".gif": "image/gif",
        ".html": "text/html; charset=utf-8",
        ".jpg": "image/jpeg",
        ".js": "application/javascript",
        ".json": "application/json",
        ".map": "application/json",
        ".png": "image/png",
        ".svg": "image/svg+xml",
        ".ttf": "font/ttf",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
    };

    function serveFile(res: any, filePath: string): void {
        res.setHeader("Content-Type", mimeTypes[path.extname(filePath).toLowerCase()] ?? "application/octet-stream");
        res.statusCode = 200;

        fs.createReadStream(filePath).pipe(res);
    }

    return {
        name: "dev-static-assets",
        apply: "serve",
        configureServer(server) {
            server.middlewares.use((req, res, next) => {
                const pathname = (req.url ?? "").split("?")[0];

                if (/^\/esp\/files\/(?:GetUserName|index|Login|nightly|stub)\.html$/.test(pathname)) {
                    req.url = (req.url ?? "").replace("/esp/files", "");
                    next();
                    return;
                }

                // HTML pages retain their deployed /esp/files/ URL, but Vite serves
                // source modules relative to its project root. Let Vite transform
                // these entries instead of falling through to its HTML fallback.
                if (
                    /^\/esp\/files\/(?:eclwatch|lib|src|src-dojo|src-react)\/.+\.(?:[cm]?[jt]sx?)$/.test(pathname)
                ) {
                    req.url = (req.url ?? "").replace("/esp/files", "");
                    next();
                    return;
                }

                // /esp/files/dist/* → build/dist/*  (Vite bundles, font-awesome, …)
                const distMatch = /^\/esp\/files\/dist\/(.+)$/.exec(pathname);
                if (distMatch) {
                    const baseDir = path.resolve(__dirname, "build", "dist");
                    const localPath = path.resolve(baseDir, distMatch[1]);
                    if (localPath.startsWith(`${baseDir}${path.sep}`) && fs.existsSync(localPath) && fs.statSync(localPath).isFile()) {
                        serveFile(res, localPath);
                        return;
                    }
                }

                // /esp/files/img/* → build/eclwatch/img/*  (favicon etc.)
                const imgMatch = /^\/esp\/files\/img\/(.+)$/.exec(pathname);
                if (imgMatch) {
                    const baseDir = path.resolve(__dirname, "build", "eclwatch", "img");
                    const localPath = path.resolve(baseDir, imgMatch[1]);
                    if (localPath.startsWith(`${baseDir}${path.sep}`) && fs.existsSync(localPath) && fs.statSync(localPath).isFile()) {
                        serveFile(res, localPath);
                        return;
                    }
                }

                // /esp/files/* → build/*  (general fallback matching lws's catch-all;
                // covers eclwatch/img/, node_modules/@hpcc-js/*/dist/…, etc.)
                const espFilesMatch = /^\/esp\/files\/(.+)$/.exec(pathname);
                if (espFilesMatch) {
                    const baseDir = path.resolve(__dirname, "build");
                    const localPath = path.resolve(baseDir, espFilesMatch[1]);
                    if (localPath.startsWith(`${baseDir}${path.sep}`) && fs.existsSync(localPath) && fs.statSync(localPath).isFile()) {
                        serveFile(res, localPath);
                        return;
                    }
                }

                next();
            });
        }
    };
}

// After Vite writes HTML entries into outDir (build/dist/), move them up to
// build/ so their deployed URLs remain independent of the asset base path.
function htmlToRoot(): Plugin {
    return {
        name: "html-to-root",
        apply: "build",
        closeBundle() {
            for (const htmlFile of ["GetUserName.html", "index.html", "Login.html", "stub.html"]) {
                const src = path.join(outputPath, htmlFile);
                if (fs.existsSync(src)) {
                    fs.copyFileSync(src, path.join(__dirname, "build", htmlFile));
                    fs.rmSync(src);
                }
            }
        }
    };
}

export default defineConfig(async (env: ConfigEnv): Promise<UserConfig> => {
    const { mode, command } = env;
    const isServe = command === "serve";
    const devTarget = isServe ? readDevTarget() : "";
    const dojo = await loadDojoFactory();
    // WsECL is served from a separate port (8002) per lws.config.js convention.
    const wsEclTarget = devTarget.replace(/:\d+$/, ":8002");
    const hpccBaseUrl = fs.existsSync("./node_modules/@hpcc-js") ? "./node_modules/@hpcc-js" : "./../../../hpcc-js/packages";

    console.log(`Building: ${mode} (serve=${isServe})`);

    const retVal: UserConfig = {
        root: __dirname,
        base: isServe ? "/" : distPublicPath,

        define: {
            __ECLWATCH_DIST_URL__: JSON.stringify(distUrl)
        },
        resolve: {
            alias: [
                { find: "src-dojo/index", replacement: path.resolve(__dirname, "src-dojo/index-shim.ts") },
                { find: "src-react-css", replacement: path.resolve(__dirname, "src-react") },
                { find: "src-react", replacement: path.resolve(__dirname, "src-react") },
                { find: "src", replacement: path.resolve(__dirname, "src") },
                { find: "ganglia", replacement: path.resolve(__dirname, "ganglia") },
                { find: "hpcc", replacement: path.resolve(__dirname, "eclwatch") }
            ]
        },
        optimizeDeps: {
            entries: ["index.html"],
            include: ["octokit"]
        },
        server: {
            proxy: {
                "/esp/getauthtype": { target: devTarget, changeOrigin: true },
                "/esp/titlebar": { target: devTarget, changeOrigin: true },
                "/esp/login": { target: devTarget, changeOrigin: true },
                "/esp/logout": { target: devTarget, changeOrigin: true },
                "/esp/lock": { target: devTarget, changeOrigin: true },
                "/esp/reset_session_timeout": { target: devTarget, changeOrigin: true },
                "/esp/unlock.json": { target: devTarget, changeOrigin: true },
                "/WsECL/": { target: wsEclTarget, changeOrigin: true },
                "/main": { target: devTarget, changeOrigin: true },
                "^/(FileSpray|WsCloud|WsDali|WsSasha|WsDfu|WsDfuXRef|WsESDLConfig|WsFileIO|WsPackageProcess|WsSMC|WsStore|WsTopology|WsWorkunits|WsResources|ws_access|ws_account|ws_codesign|ws_elk|ws_esdlconfig|ws_logaccess|ws_machine|ws_store|wsstore)/":
                { target: devTarget, changeOrigin: true },
            }
        },
        build: {
            // JS/CSS artefacts go into build/dist/ (matching base = distPublicPath).
            // HTML entry files are emitted here too by Vite, then the htmlToRoot
            // plugin moves them up to build/ so their URL path (/esp/files/*.html)
            // is independent of the asset base (/esp/files/dist/).
            outDir: outputPath,
            emptyOutDir: false,
            sourcemap: mode !== "production",
            minify: mode === "production",
            cssMinify: false,
            // locale: "pt-br",   // Testing only  ---

            rolldownOptions: {
                input: {
                    "src-dojo": path.resolve(__dirname, "lib/src-dojo/index.js"),
                    "src-lib": path.resolve(__dirname, "src/index.ts"),
                    index: path.resolve(__dirname, "index.html"),
                    stub: path.resolve(__dirname, "stub.html"),
                    GetUserName: path.resolve(__dirname, "GetUserName.html"),
                    Login: path.resolve(__dirname, "Login.html")
                },
                output: {
                    entryFileNames: (chunk: { name: string }) => `${chunk.name}.eclwatch.js`,
                    // Keep emitted filenames short. Some Dojo plugin module ids can
                    // expand into very long encoded names, which overflow installer
                    // path length limits on unpack.
                    chunkFileNames: "index-chunks/c-[hash].js",
                    assetFileNames: "index-assets/a-[hash][extname]",
                    // Rolldown normally writes sourcemap "sources" as paths relative to
                    // the .map file's own location on disk (e.g.
                    // "../../../src-react/components/Foo.tsx"). Since this app is
                    // served under a non-root "base" (distPublicPath, e.g.
                    // "/esp/files/dist/"), resolving that relative path against the
                    // *served URL* walks up the wrong number of path segments (the
                    // "esp" in the base collides with the real "esp" folder in the
                    // repo path), producing a bogus doubled ".../esp/esp/..."-style
                    // path when a debugger maps the source back to disk. Emitting
                    // absolute disk paths here sidesteps relative-URL resolution
                    // entirely so debuggers open the correct file.
                    sourcemapPathTransform: (relativeSourcePath: string, sourcemapPath: string) =>
                        path.resolve(path.dirname(sourcemapPath), relativeSourcePath)
                }
            }
        },
        plugins: [
            react(),
            hpccJsSiblingFallback(),
            ...(isServe ? [devStaticAssets()] : [htmlToRoot()]),
            dojo({
                esmInterop: "namespace",
                ...({ globalRequireMode: "context" } as Record<string, unknown>),
                loaderConfig: {
                    baseUrl: ".",
                    deps: ["hpcc/stub"],
                    async: true,
                    selectorEngine: "lite",
                    blankGif: "/esp/files/eclwatch/img/blank.gif",
                    has: {
                        "dojo-loader": 0,
                        "dojo-sync-loader": 0
                    },
                    paths: {
                        "hpcc": "./eclwatch",
                        "src-dojo": "./lib/src-dojo",
                        "src": "./lib/src",
                        "src-react": "./lib/src-react",
                        "src-react-css": "./src-react",
                        "ganglia": "./ganglia",
                        "templates": "./eclwatch/templates",
                        "ecl": "./eclwatch/ecl",
                        "css": "./loader/css",
                        "@hpcc-js/api": hpccBaseUrl + "/api/dist/index",
                        "@hpcc-js/chart": hpccBaseUrl + "/chart/dist/index",
                        "@hpcc-js/codemirror": hpccBaseUrl + "/codemirror/dist/index",
                        "@hpcc-js/common": hpccBaseUrl + "/common/dist/index",
                        "@hpcc-js/comms": hpccBaseUrl + "/comms/dist/browser/index",
                        "@hpcc-js/composite": hpccBaseUrl + "/composite/dist/index",
                        "@hpcc-js/dataflow": hpccBaseUrl + "/dataflow/dist/index",
                        "@hpcc-js/dgrid": hpccBaseUrl + "/dgrid/dist/index",
                        "@hpcc-js/eclwatch": hpccBaseUrl + "/eclwatch/dist/index",
                        "@hpcc-js/form": hpccBaseUrl + "/form/dist/index",
                        "@hpcc-js/graph": hpccBaseUrl + "/graph/dist/index",
                        "@hpcc-js/layout": hpccBaseUrl + "/layout/dist/index",
                        "@hpcc-js/phosphor": hpccBaseUrl + "/phosphor/dist/index",
                        "@hpcc-js/html": hpccBaseUrl + "/html/dist/index",
                        "@hpcc-js/map": hpccBaseUrl + "/map/dist/index",
                        "@hpcc-js/other": hpccBaseUrl + "/other/dist/index",
                        "@hpcc-js/react": hpccBaseUrl + "/react/dist/index",
                        "@hpcc-js/timeline": hpccBaseUrl + "/timeline/dist/index",
                        "@hpcc-js/tree": hpccBaseUrl + "/tree/dist/index",
                        "@hpcc-js/util": hpccBaseUrl + "/util/dist/index",
                        "@hpcc-js/TopoJSON": hpccBaseUrl + "/map/TopoJSON",
                        "clipboard": "./node_modules/clipboard/dist/clipboard",
                        "codemirror": "./node_modules/codemirror",
                        "crossfilter": "./node_modules/crossfilter2/crossfilter.min",
                        "font-awesome": hpccBaseUrl + "/common/font-awesome",
                        "tslib": "./node_modules/tslib/tslib"

                    },
                    packages: [
                        {
                            name: "dojo",
                            location: "./node_modules/dojo"
                        },
                        {
                            name: "dijit",
                            location: "./node_modules/dijit"
                        },
                        {
                            name: "dojox",
                            location: "./node_modules/dojox"
                        },
                        {
                            name: "dijit-themes",
                            location: "./node_modules/dijit-themes"
                        },
                        {
                            name: "dgrid",
                            location: "./dgrid"
                        },
                        {
                            name: "xstyle",
                            location: "./xstyle"
                        },
                        {
                            name: "put-selector",
                            location: "./put-selector"
                        }
                    ]
                },
                // environment: { dojoRoot: "node_modules", distUrl },
                // buildEnvironment: { dojoRoot: "node_modules", distUrl },
                // // dijit drags in the full CLDR set otherwise; without this every locale is bundled.
                // locales: ["en", "de"]
            })
        ]
    };
    return retVal;
});
