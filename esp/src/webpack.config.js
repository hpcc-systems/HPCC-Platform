
var DojoWebpackPlugin = require("dojo-webpack-plugin");

var path = require("path");
var webpack = require("webpack");
var BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

module.exports = function (env) {
    const isProduction = env && env.build === "prod";

    const plugins = [
        new DojoWebpackPlugin({
            loaderConfig: require.resolve("./eclwatch/dojoConfig"),
            environment: { dojoRoot: "node_modules" },
            locales: ["en", "bs", "es", "hr", "hu", "pt-br", "sr", "zh"]
        }),
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
        ),
        new webpack.optimize.CommonsChunkPlugin({
            name: 'node_modules',
            filename: 'node_modules.js',
            minChunks(module, count) {
                var context = module.context;
                return context && context.indexOf('node_modules') >= 0;
            }
        }),
        new webpack.optimize.CommonsChunkPlugin({
            children: true,
            minChunks: 4
        })
    ];

    if (isProduction) {
        plugins.push(new webpack.optimize.UglifyJsPlugin({
            cache: true,
            parallel: true,
            output: { comments: false },
            compress: { warnings: false },
            sourceMap: false
        }));
    }

    return {
        context: __dirname,
        entry: {
            stub: ["eclwatch/stub",
                "eclwatch/ActivityWidget",
                "eclwatch/DFUQueryWidget",
                "eclwatch/QuerySetQueryWidget",
                "eclwatch/WUDetailsWidget",
                "eclwatch/WUQueryWidget",
                "eclwatch/TopologyWidget"
            ]
        },
        output: {
            filename: "[name].eclwatch.js",
            path: path.join(__dirname, "build/dist"),
            publicPath: "/esp/files/dist/",
            pathinfo: false
        },
        module: {
            loaders: [
                {
                    test: /\.(png|jpg|gif)$/,
                    use: [
                        {
                            loader: 'url-loader',
                            options: {
                                limit: 8192
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
            alias: {
                "clipboard": path.resolve(__dirname, 'node_modules/clipboard/dist/clipboard')
            }
        },
        plugins: plugins,
        resolveLoader: {
            modules: [
                path.join(__dirname, "node_modules")
            ]
        },
        devtool: false,
        node: {
            process: false,
            global: false
        }
    }
};
