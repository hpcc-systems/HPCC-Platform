/* eslint-disable no-undef */
/* eslint-disable @typescript-eslint/no-var-requires */
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

module.exports = function (env) {
    const isDev = (env && env.development) || env === "development";
    const isProduction = !isDev;
    console.log(isProduction ? "Production bundle" : "Debug bundle");

    const entry = {
        stub: "eclwatch/stub",
        dojoLib: "lib/src/dojoLib",
        index: "lib/src-react/index"
    };

    const plugins = [
        new DojoWebpackPlugin({
            loaderConfig: require("./eclwatch/dojoConfig"),
            environment: { dojoRoot: "build/dist" },
            buildEnvironment: { dojoRoot: "node_modules" }, // used at build time
            locales: ["en", "bs", "es", "fr", "hr", "hu", "pt-br", "sr", "zh"]
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

    return {
        context: path.resolve(__dirname),
        entry: entry,
        output: {
            filename: "[name].eclwatch.js",
            chunkFilename: "[name].eclwatch.js",
            path: path.resolve(__dirname, "build/dist"),
            publicPath: "/esp/files/dist/",
            pathinfo: true
        },
        module: {
            rules: [
                {
                    test: /\.css$/,
                    use: ["style-loader", "css-loader"]
                }, {
                    test: /\.js$/,
                    use: ["source-map-loader"],
                    enforce: "pre"
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
                "@hpcc-js/wasm/dist/duckdb": path.resolve(__dirname, "node_modules/@hpcc-js/wasm/dist/duckdb.js"),
            },
            fallback: {
                "@hpcc-js/wasm": [
                    path.resolve(__dirname, "../../../hpcc-js-wasm"),
                ],
                "@hpcc-js": [
                    path.resolve(__dirname, "../../../hpcc-js/packages"),
                    path.resolve(__dirname, "../../../Visualization/packages")
                ]
            }
        },
        plugins: plugins,
        resolveLoader: {
            modules: ["node_modules"]
        },

        target: "web",
        mode: isProduction ? "production" : "development",
        devtool: isProduction ? undefined : "cheap-module-source-map",

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
        }
    };
};