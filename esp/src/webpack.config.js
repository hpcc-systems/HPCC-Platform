/* eslint-disable */
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
const proxyItems = ["/WsWorkunits", "/WsStore", "/WsSMC", "/WsTopology", "/WsDfu", "/FileSpray", "/ws_machine", "/ws_account", "/ws_elk", "/esp/getauthtype", "/esp/reset_session_timeout", "/esp/titlebar"];
proxyItems.forEach(item => {
    proxy[item] = {
        target: "http://" + debugServerIP + ":8010",
        secure: false
    };
});

module.exports = function (env) {
    const isDev = (env && env.development) || env === "development";
    const isProduction = !isDev;

    const entry = {
        stub: "eclwatch/stub",
        dojoLib: "lib/src/dojoLib"
    };
    if (!isProduction) {
        entry.index = "lib/src-react/index"
    }

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
        context: __dirname,
        entry: entry,
        output: {
            filename: "[name].eclwatch.js",
            chunkFilename: "[name].eclwatch.js",
            path: path.join(__dirname, "build/dist"),
            publicPath: "/esp/files/dist/",
            pathinfo: true
        },
        module: {
            rules: [
                {
                    test: /\.(png|jpg|gif)$/,
                    use: [
                        {
                            loader: "url-loader",
                            options: {
                                limit: 100000
                            }
                        }
                    ]
                }, {
                    test: /\.css$/,
                    use: ["style-loader", "css-loader"]
                }, {
                    test: /.(ttf|otf|eot|svg|woff(2)?)(\?[a-z0-9]+)?$/,
                    use: [{
                        loader: "file-loader",
                        options: {
                            name: "[name].[ext]"
                        }
                    }]
                }, {
                    test: /\.js$/,
                    use: ["source-map-loader"],
                    enforce: "pre"
                }]
        },
        resolve: {
            alias: {
                "clipboard": path.resolve(__dirname, "node_modules/clipboard/dist/clipboard")
            }
        },
        plugins: plugins,
        resolveLoader: {
            modules: ["node_modules"]
        },

        mode: isProduction ? "production" : "development",
        devtool: isProduction ? undefined : "cheap-module-source-map",

        watchOptions: isProduction ? undefined : {
            aggregateTimeout: 600
        },

        devServer: isProduction ? undefined : {
            contentBase: path.join(__dirname, "build"),
            contentBasePublicPath: "/esp/files",
            proxy
        }
    }
};