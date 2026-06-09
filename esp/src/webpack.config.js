var DojoWebpackPlugin = require("dojo-webpack-plugin");

var fs = require("fs");
var path = require("path");
var webpack = require("webpack");

//  Dev Environment ---
let debugServerIP = "play.hpccsystems.com";
if (fs.existsSync("./lws.target.txt")) {
    debugServerIP = fs.readFileSync("./lws.target.txt").toString().replace("\r\n", "\n").split("\n")[0];
}
console.log("debugServerIP:  ", debugServerIP);
const proxy = {};
const proxyItems = ["/WsWorkunits", "/WsStore", "/WsSMC", "/WsTopology", "/WsDfu", "/FileSpray", "/ws_machine", "/ws_account", "/Ws_Account", "WsResources", "/ws_logaccess", "/ws_elk", "/esp/getauthtype", "/esp/reset_session_timeout", "/esp/titlebar"];
proxyItems.forEach(item => {
    proxy[item] = {
        target: "http://" + debugServerIP + ":8010",
        secure: false
    };
});

const distPublicPath = (process.env.ECLWATCH_DIST_URL || "/esp/files/dist/").replace(/\/?$/, "/");
const distUrl = distPublicPath.replace(/\/$/, "");
const distFolder = distUrl.split("/").filter(Boolean).pop() || "dist";
const outputPath = path.resolve(__dirname, `build/${distFolder}`);

const plugins = [
    new DojoWebpackPlugin({
        loaderConfig: require("./eclwatch/dojoConfig"),
        environment: { dojoRoot: `build/${distFolder}`, distUrl },
        buildEnvironment: { dojoRoot: "node_modules" }, // used at build time
        locales: ["en", "bs", "es", "fr", "hr", "hu", "pt-br", "sr", "zh"]
    }),
    new webpack.DefinePlugin({
        __ECLWATCH_DIST_URL__: JSON.stringify(distUrl)
    }),
    // For plugins registered after the DojoAMDPlugin, data.request has been normalized and
    // resolved to an absMid and loader-config maps and aliases have been applied
    new webpack.NormalModuleReplacementPlugin(/^dojox\/gfx\/renderer!/, "dojox/gfx/canvas"),
    new webpack.NormalModuleReplacementPlugin(
        /^css!/, function (data) {
            data.request = data.request.replace(/^css!/, "!style-loader!css-loader!");
        }
    ),
    new webpack.NormalModuleReplacementPlugin(
        /^xstyle\/css!/, function (data) {
            data.request = data.request.replace(/^xstyle\/css!/, "!style-loader!css-loader!");
        }
    )
];

module.exports = function (env) {
    const isDev = (env && env.development) || env === "development";
    const isProduction = !isDev;
    console.log(isProduction ? "Production bundle" : "Debug bundle");

    // Build fallback paths, only including paths that actually exist
    const fallback = {};

    const wasmDuckdbPath = path.resolve(__dirname, "../../../hpcc-js-wasm/packages/duckdb");
    const wasmGraphvizPath = path.resolve(__dirname, "../../../hpcc-js-wasm/packages/graphviz");
    const hpccjsPath = path.resolve(__dirname, "../../../hpcc-js/packages");
    const visualizationPath = path.resolve(__dirname, "../../../Visualization/packages");

    if (fs.existsSync(wasmDuckdbPath)) {
        fallback["@hpcc-js/wasm-duckdb"] = [wasmDuckdbPath];
    }

    if (fs.existsSync(wasmGraphvizPath)) {
        fallback["@hpcc-js/wasm-graphviz"] = [wasmGraphvizPath];
    }

    const hpccjsFallbackPaths = [];
    if (fs.existsSync(hpccjsPath)) {
        hpccjsFallbackPaths.push(hpccjsPath);
    }
    if (fs.existsSync(visualizationPath)) {
        hpccjsFallbackPaths.push(visualizationPath);
    }
    if (hpccjsFallbackPaths.length > 0) {
        fallback["@hpcc-js"] = hpccjsFallbackPaths;
    }

    return {
        context: path.resolve(__dirname),
        entry: {
            "src-dojo": {
                import: "./lib/src-dojo/index",
            },
            "src-lib": {
                // @griffel/core is listed first so it lands in src-lib and is
                // fully initialized before index.eclwatch.js runs. Without this,
                // webpack places @griffel/core inside index.eclwatch.js where a
                // circular CJS require chain causes its __styles export to be
                // undefined (TDZ) when useFluentProviderStyles.styles.js calls
                // it at module init time.
                import: ["@griffel/core", "./lib/src/index"],
                dependOn: ["src-dojo"],
            },
            stub: {
                import: "./eclwatch/stub",
                dependOn: ["src-lib", "src-dojo"],
            },
            index: {
                import: "./lib/src-react/index",
                dependOn: ["src-lib", "src-dojo"],
            }
        },
        output: {
            filename: "[name].eclwatch.js",
            path: outputPath,
            publicPath: distPublicPath,
            // pathinfo adds module path comments; disable in production to avoid
            // interfering with terser's source-map-aware minimization pass.
            pathinfo: !isProduction
        },
        module: {
            rules: [
                {
                    test: /\.css$/,
                    use: ["style-loader", "css-loader"]
                }, {
                    test: /\.[cm]?js$/,
                    enforce: "pre",
                    use: [{
                        loader: "source-map-loader",
                        options: {
                            filterSourceMappingUrl: (url, resourcePath) => {
                                if (resourcePath.includes("node_modules") && !resourcePath.includes("hpcc-js")) {
                                    return false;
                                }
                                return true;
                            }
                        }
                    }]
                }, {
                    test: /\.js$/,
                    loader: "string-replace-loader",
                    options: {
                        search: isProduction ? "RELEASE_ONLY" : "DEBUG_ONLY",
                        replace(match, p1, offset, string) {
                            return "DEBUG_ONLY */";
                        },
                        flags: "g"
                    }
                }]
        },
        resolve: {
            alias: {
                [path.resolve(__dirname, "_firebug/firebug")]: path.resolve(__dirname, "node_modules/dojo/_firebug/firebug.js")
            },
            fallback: fallback
        },
        plugins: plugins,
        resolveLoader: {
            modules: ["node_modules"]
        },

        target: "web",
        mode: isProduction ? "production" : "development",
        devtool: isProduction ? undefined : "source-map",

        // Production optimizations tuned to avoid CJS circular-dep init failures
        // in @fluentui/tokens and @griffel/core:
        //
        //   concatenateModules: false  — disable ESM scope hoisting so module
        //     factories run in the correct order (fixes statusColorMapping TDZ).
        //
        //   usedExports: false  — skip "mark used exports" analysis so terser
        //     keeps all exported variables (e.g. statusColorMapping) even when
        //     they aren't statically imported by name in an ESM context. Without
        //     this, webpack marks statusColorMapping as unused and terser removes
        //     its var assignment, making the CJS getter return undefined.
        optimization: isProduction ? { concatenateModules: false, usedExports: false } : undefined,

        watchOptions: isProduction ? undefined : {
            aggregateTimeout: 600
        },

        devServer: isProduction ? undefined : {
            hot: "only",
            static: {
                directory: path.resolve(__dirname, "build"),
                publicPath: "/esp/files"
            },
            liveReload: false,
            proxy,
            port: 8080
        },

        // Silence noisy missing / failed source map warnings coming from third-party deps
        ignoreWarnings: [
            (warning) => typeof warning.message === "string" && warning.message.includes("Failed to parse source map")
        ]
    };
};
