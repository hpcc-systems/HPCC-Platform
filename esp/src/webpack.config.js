
var DojoWebpackPlugin = require("dojo-webpack-plugin");
var CopyWebpackPlugin = require("copy-webpack-plugin");
var UglifyJsPlugin = require("uglifyjs-webpack-plugin");

var path = require("path");
var webpack = require("webpack");
var BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

module.exports = function (env) {
    const isProduction = env && env.build === "prod";

    const plugins = [
        new DojoWebpackPlugin({
            loaderConfig: require("./eclwatch/dojoConfig"),
            environment: { dojoRoot: "build/dist" },
            buildEnvironment: { dojoRoot: "node_modules" }, // used at build time
            locales: ["en", "bs", "es", "hr", "hu", "pt-br", "sr", "zh"]
        }),

        // Copy non-packed resources needed by the app to the release directory
        new CopyWebpackPlugin([{
            context: "node_modules",
            from: "dojo/resources/blank.gif",
            to: "dojo/resources"
        }]),

        // For plugins registered after the DojoAMDPlugin, data.request has been normalized and
        // resolved to an absMid and loader-config maps and aliases have been applied
        new webpack.NormalModuleReplacementPlugin(/^dojox\/gfx\/renderer!/, "dojox/gfx/canvas"),
        new webpack.NormalModuleReplacementPlugin(
            /^css!/, function (data) {
                data.request = data.request.replace(/^css!/, "!style-loader!css-loader!")
            }
        ),
        new webpack.NormalModuleReplacementPlugin(
            /^xstyle\/css!/, function (data) {
                data.request = data.request.replace(/^xstyle\/css!/, "!style-loader!css-loader!")
            }
        )
    ];

    return {
        context: __dirname,
        entry: {
            //stub: "eclwatch/stub",
            react: "./src/index.tsx",
            //dojoLib: "lib/src/dojoLib"
        },
        output: {
            // filename: "[name].eclwatch.js",
            // chunkFilename: "[name].eclwatch.js",
            // path: path.join(__dirname, "build/dist"),
            // publicPath: "/esp/files/dist/",
            // pathinfo: true
            filename: "reactBundle.js",
            path: path.join(__dirname, "build/dist")
        },
        module: {
            rules: [
                {
                    test: /\.tsx?$/, loader: "awesome-typescript-loader"
                },
                {
                    enforce: "pre", test: /\.js$/, loader: "source-map-loader"
                },
                {
                    test: /\.(png|jpg|gif)$/,
                    use: [
                        {
                            loader: 'url-loader',
                            options: {
                                limit: 100000
                            }
                        }
                    ]
                }, {
                    test: /\.css$/,
                    use: ['style-loader', 'css-loader']
                }, {
                    test: /.(ttf|otf|eot|svg|woff(2)?)(\?[a-z0-9]+)?$/,
                    use: [{
                        loader: 'file-loader',
                        options: {
                            name: '[name].[ext]'
                        }
                    }]
                }]
        },
        resolve: {
            extensions: [".ts", ".tsx", ".js", ".json"],
            alias: {
                "clipboard": path.resolve(__dirname, 'node_modules/clipboard/dist/clipboard')
            }
        },
        plugins: plugins,
        resolveLoader: {
            modules: ["node_modules"]
        },
        mode: isProduction ? "production" : "development",
        optimization: {
            // runtimeChunk: "single",
            minimizer: [
                // we specify a custom UglifyJsPlugin here to get source maps in production
                new UglifyJsPlugin({
                    cache: true,
                    parallel: true,
                    uglifyOptions: {
                        compress: isProduction,
                        mangle: isProduction,
                        output: { comments: !isProduction }
                    },
                    sourceMap: false
                })
            ]
        },
        externals: {
            "react": "React",
            "react-dom": "ReactDOM"
        },
        //devtool: false
        devtool: "source-map"
    }
};
