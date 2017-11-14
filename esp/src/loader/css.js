/*
 * (C) Copyright IBM Corp. 2012, 2016 All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
define([
    'require',
    'dojo/_base/window',
    'dojo/_base/html',
    'dojo/dom-construct',
    'dojo/has',
    'dojo/query',
    'dojo/_base/array',
    'dojo/_base/lang',
    'dojo/io-query',
    'dojo/_base/Deferred',
    'dojo/has!dojo-combo-api?:postcss?dojo/promise/all',
    'dojo/has!dojo-combo-api?:postcss?postcss',
    'dojo/has!css-inject-api?dojo/aspect',
    'dojo/text'
], function (require, dwindow, dhtml, domConstruct, has, query, arrays, lang, ioQuery, Deferred, all, postcss, aspect) {
	/*
	 * module:
	 *    css
	 * summary:
	 *    This plugin handles AMD module requests for CSS/LESS files.  Required files are
	 *    loaded by delegating to the dojo/text plugin and then inserting the CSS into
	 *    a style sheet element that is appended to the HEAD element in the DOM, and
	 *    the style element is returned as the value of the module.
	 *
	 *    For LESS files, the LESS pre-processor specified by the 'lesspp' module identifier
	 *    is used to convert the LESS markup into CSS.  The app is responsible for defining
	 *    the path to the LESS pre-processor in the Dojo loader config.  This plugin has been 
	 *    tested with //cdnjs.cloudflare.com/ajax/libs/less.js/1.7.3/less.min.js, which 
	 *    would be specifed in the loader config as:
	 *
	 *    <code>
	 *      paths: {
	 *        lesspp: "//cdnjs.cloudflare.com/ajax/libs/less.js/1.7.3/less.min"
	 *      }
	 *    </code>
	 *
	 *    You do not need to define the path to 'lesspp' if your app does not use LESS
	 *
	 *    URLs for url(...) and @import statements in the CSS are fixed up to make them
	 *    relative to the requested module's path.
	 *
	 *    This plugin guarantees that style elements will be inserted into the DOM in
	 *    the same order that the associated CSS modules are required, except when a
	 *    previously requested module is requested again.  In other words, if
	 *    stylesA.css is required before stylesB.css, then the styles for stylesA.css
	 *    will be inserted into the DOM ahead of the styles for stylesB.css.  This
	 *    behavior helps to ensure proper cascading of styles based on order of request.
	 *
	 *    This plugin supports two modes of operation.  In the default mode, style
	 *    elements are injected into the DOM when the plugin's load method is called
	 *    for the resource.  This ensures that styles will be inserted into the DOM
	 *    in the order that they appear in the dependency list, but the order of
	 *    insertion for styles loaded by different modules is undefined.
	 *
	 *    The second mode of operation is enabled by the 'css-inject-api' feature.
	 *    In this mode, CSS is not injected into the DOM directly by the plugin.  Instead,
	 *    the application calls the inject() method to inject the CSS in the dependency
	 *    list from within the require() or define() callback.  For example:
	 *
	 *    <code>
	 *    	define(['app/foo', 'css!app/styles/foo.css'], function(foo) {
	 *        require('css').inject.apply(this, arguments);
	 *    </code>
	 *
	 *    The inject() function iterates over the arguments passed and injects into
	 *    the DOM any style elements that have not previously been injected.  This mode
	 *    provides for more predictable order of injection of styles since the order
	 *    for styles injected from different modules corresponds to the define order
	 *    of the JavaScript modules doing the injecting.
	 *
	 *    The task of calling inject() at the beginning of every require() or define()
	 *    callback can be automated by calling installAutoInjectHooks().  This method
	 *    installs intercepts for the global require() and define() functions.  The
	 *    intercepts hook the callback functions passed to require() and define()
	 *    for the purpose of automatically invoking the inject() method before the
	 *    callback function is executed.  These hooks also watch for context require()
	 *    instances and install intercepts for the context require() callbacks as well.
	 *
	 *    The only caveat is that installAutoInjectHooks() must be called before any
	 *    require() or define() functions that use this plugin to load css, or that
	 *    reference a context require which uses this plugin to load css, are called,
	 *    otherwise the inject api will not automatically be invoked for those cases.
	 */
    var
        head = dwindow.doc.getElementsByTagName('head')[0],

        urlPattern = /(^[^:\/]+:\/\/[^\/\?]*)([^\?]*)(\??.*)/,

        isLessUrl = function (url) {
            return /\.less(?:$|\?)/i.test(url);
        },

        isRelative = function (url) {
            return !/^[^:\/]+:\/\/|^\//.test(url);
        },

        dequote = function (url) {
            // remove surrounding quotes and normalize slashes
            return url.replace(/^\s\s*|\s*\s$|[\'\"]|\\/g, function (s) {
                return s === '\\' ? '/' : '';
            });
        },

        joinParts = function (parts) {
            // joins URL parts into a single string, handling insertion of '/' were needed
            var result = '';
            arrays.forEach(parts, function (part) {
                result = result +
                    (result && result.charAt(result.length - 1) !== '/' && part && part.charAt(0) !== '/' ? '/' : '') +
                    part;
            });
            return result;
        },


        normalize = function (url) {
            // Collapse .. and . in url paths
            var match = urlPattern.exec(url) || [url, '', url, ''],
                host = match[1], path = match[2], queryArgs = match[3];

            if (!path || path === '/') return url;

            var parts = [];
            arrays.forEach(path.split('/'), function (part, i, ary) {
                if (part === '.') {
                    if (i === ary.length - 1) {
                        parts.push('');
                    }
                    return;
                } else if (part == '..') {
                    if ((parts.length > 1 || parts.length == 1 && parts[0] !== '') && parts[parts.length - 1] !== '..') {
                        parts.pop();
                    } else {
                        parts.push(part);
                    }
                } else {
                    parts.push(part);
                }
            });
            var result = parts.join('/');

            return joinParts([host, result]) + queryArgs;
        },

        resolve = function (base, relative) {
            // Based on behavior of the Java URI.resolve() method.
            if (!base || !isRelative(relative)) {
                return normalize(relative);
            }
            if (!relative) {
                return normalize(base);
            }
            var match = urlPattern.exec(base) || [base, '', base, ''],
                host = match[1], path = match[2], queryArgs = match[3];

            // remove last path component from base before appending relative
            if (path.indexOf('/') !== -1 && path.charAt(path.length) !== '/') {
                // remove last path component
                path = path.split('/').slice(0, -1).join('/') + '/';
            }

            return normalize(joinParts([host, path, relative]));
        },

        addArgs = function (url, queryArgs) {
            // Mix in the query args specified by queryArgs to the URL
            if (queryArgs) {
                var queryObj = ioQuery.queryToObject(queryArgs),
                    mixedObj = lang.mixin(queryObj, ioQuery.queryToObject(url.split('?')[1] || ''));
                url = url.split('?').shift() + '?' + ioQuery.objectToQuery(mixedObj);
            }
            return url;
        },

        fixUrlsInCssFile = function (/*String*/filePath, /*String*/content, /*boolean*/lessImportsOnly) {
            var queryArgs = filePath.split('?')[1] || '';

            var rewriteUrl = function (url) {
                if (lessImportsOnly && url.charAt(0) === '@') {
                    return url;
                }
                // only fix relative URLs.
                if (isRelative(url)) {
                    // Support webpack style module name indicator
                    if (/^~[^/]/.test(url)) {
                        // leading tilde means url is a module name, not a relative url
                        url = require.toUrl(url.substring(1));
                    } else {
                        url = resolve(filePath, url);
                    }
                    if (lessImportsOnly && isLessUrl(url)) {
                        // LESS compiler fails to locate imports using relative urls when
                        // the document base has been modified (e.g. via a <base> tag),
                        // so make the url absolute.
                        var baseURI = dwindow.doc.baseURI;
                        if (!baseURI) {
                            // IE doesn't support document.baseURI.  See if there's a base tag
                            var baseTags = dwindow.doc.getElementsByTagName("base");
                            baseURI = baseTags.length && baseTags[0].href || dwindow.location && dwindow.location.href;
                        }
                        url = resolve(baseURI, url);
                    }
                }
                return addArgs(url, queryArgs);		// add cachebust arg from including file
            };

            if (lessImportsOnly) {
                // Only modify urls for less imports.  We need to do it this way because the LESS compiler
                // needs to be able to find the imports, but we don't want to do non-less imports because
                // that would result in those URLs being rewritten a second time when we process the compiled CSS.
                content = content.replace(/@import\s+(url\()?([^\s;]+)(\))?/gi, function (match, prefix, url) {
                    url = dequote(url);
                    return isLessUrl(url) ? '@import \'' + rewriteUrl(url) + '\'' : match;
                });
            } else {
                content = content.replace(/url\s*\(([^#\n\);]+)\)/gi, function (match, url) {
                    return 'url(\'' + rewriteUrl(dequote(url)) + '\')';
                });
                // handle @imports that don't use url(...)
                content = content.replace(/@import\s+(url\()?([^\s;]+)(\))?/gi, function (match, prefix, url) {
                    return (prefix == 'url(' ? match : ('@import \'' + rewriteUrl(dequote(url)) + '\''));
                });
            }
            return content;
        },

        postcssProcessor,

        postcssPromise,

		/*
		 * Initialize PostCSS and configured plugins.  Plugins are configured with the postcssPlugins
		 * property in dojoConfig or the global require object.
		 *
		 * Plugins are configured using an array of two element arrays as in the following example:
		 * <code><pre>
		 * postcss: {
		 *    plugins: [
		 *       [
		 *          'autoprefixer',  // Name of the plugin resource
		 *                           // Can be an AMD module id or an absolute URI to a server resource
		 *          function(autoprefixer) {
		 *             return autoprefixer({browsers: '> 1%'}).postcss; // the init function
		 *          }
		 *       ]
		 *    ]
		 * },
		 * </pre></code>
		 */
        postcssInitialize = function () {
            var deferred;
            if (!has('dojo-combo-api') && has('postcss') && postcss) {
                var postcssConfig = window.require.postcss || window.dojoConfig && window.dojoConfig.postcss;
                if (postcssConfig) {
                    var pluginsConfig = postcssConfig.plugins;
                    if (pluginsConfig) {
                        // Load each module using async require.  Each loaded module will get a Promise
                        // that will be resolved when that module loads and the plugin object has been
                        // initialized.
                        var promises = [], plugins = [];
                        arrays.forEach(pluginsConfig, function (pluginConfig) {
                            var deferred = new Deferred();
                            promises.push(deferred.promise);
                            require([pluginConfig[0]], function (p) {
                                try {
                                    plugins.push(pluginConfig[1](p));
                                    deferred.resolve();
                                } catch (e) {
                                    console.error(e);
                                    deferred.reject(e);
                                }
                            });
                        });
                        if (promises.length > 0) {
                            // Use dojo/promise/all so we know when all of the plugins have been
                            // loaded and initialized.
                            deferred = new Deferred();
                            postcssPromise = deferred.promise;
                            all(promises).then(function () {
                                try {
                                    postcssProcessor = postcss(plugins);
                                    deferred.resolve();
                                } catch (e) {
                                    console.error(e);
                                    deferred.reject(e);
                                }
                            }).otherwise(function (e) {
                                // one or more plugins failed to initialize
                                console.error(e);
                                deferred.reject(e);
                            });
                        }
                    }
                }
            }

            if (!postcssPromise) {
                // Not using PostCSS or there were no plugins configured.  Just create a
                // resolved promise.
                deferred = new Deferred();
                postcssPromise = deferred.promise;
                deferred.resolve();
            }
        };

    postcssInitialize();


    var test = require.toUrl("test");
    var idx = test.indexOf("?");
    var urlArgs = (idx === -1 ? "" : test.substring(idx));

    return {
        load: function (/*String*/id, /*Function*/parentRequire, /*Function*/load) {
            if (has('no-css')) {
                return load();
            }
            var url = parentRequire.toUrl(id).replace(/^\s+/g, ''); // Trim possible leading white-space

            // see if a stylesheet element has already been added for this module
            var styles = query("head>style[url='" + url.split('?').shift() + "']"), style;
            if (styles.length === 0) {
                // create a new style element for this module.  Add it to the DOM if the 'css-inject-api'
                // feature is not true.
                style = domConstruct.create('style', {}, has('css-inject-api') ? null : head);
                style.type = 'text/css';
                dhtml.setAttr(style, 'url', url.split('?').shift());
                dhtml.setAttr(style, 'loading', '');
            } else {
                style = styles[0];
                if (!dhtml.hasAttr(style, 'loading')) {
                    load(style);
                    return;
                }
            }

            parentRequire(['dojo/text!' + id], function (text) {
                // Check if we need to compile LESS client-side
                if (isLessUrl(id) && !has('dojo-combo-api')) {
                    processLess(text);
                } else {
                    processCss(text);
                }
            });

			/**
			 * Compiles LESS to CSS and passes off the result to the standard `processCss` method.
			 * @param  {String} lessText The LESS text to compile.
			 */
            function processLess(lessText) {
                var additionalData;
                var lessGlobals = window.require.lessGlobals || window.dojoConfig && window.dojoConfig.lessGlobals;
                if (lessGlobals) {
                    additionalData = { globalVars: lessGlobals };
                }
                var pathParts = url.split('?').shift().split('/');
                require(['lesspp'], function (lesspp) {
                    var parser = new lesspp.Parser({
                        filename: pathParts.pop(),
                        paths: [pathParts.join('/')]  // the compiler seems to ignore this
                    });
                    // Override the parser's fileLoader method so we can add urlArgs to less modules loaded by the parser
                    if (!lesspp.__originalFileLoader) {
                        lesspp.__originalFileLoader = lesspp.Parser.fileLoader;
                        lesspp.Parser.fileLoader = function () {
                            // add query args if needed
                            if (!/\?/.test(arguments[0])) {
                                arguments[0] += urlArgs;
                            }
                            return lesspp.__originalFileLoader.apply(this, arguments);
                        }
                    }
                    parser.parse(fixUrlsInCssFile(url, lessText, true), function (err, tree) {
                        if (err) {
                            console.error('LESS Parser Error!');
                            console.error(err);
                            return load.error(err);
                        }
                        processCss(tree.toCSS());
                    }, additionalData);
                });
            }

			/**
			 * Injects CSS text into the stylesheet element, fixing relative URLs.
			 * @param  {String} cssText The CSS to inject.
			 */
            function processCss(cssText) {
                postcssPromise.always(function () {
                    if (cssText && dojo.isString(cssText) && cssText.length) {
                        cssText = fixUrlsInCssFile(url, cssText);
                        if (postcssProcessor) {
                            cssText = postcssProcessor.process(cssText, { safe: true });
                        }
                        if (style.styleSheet) {
                            style.styleSheet.cssText = cssText;
                        } else {
                            while (style.firstChild) {
                                style.removeChild(style.firstChild);
                            }
                            style.appendChild(dwindow.doc.createTextNode(cssText));
                        }
                    }
                    dhtml.removeAttr(style, "loading");
                    load(style);
                });
            }
        },

		/*
		 * Iterates through the function arguments and for any style node arguments that
		 * are not already in the DOM, appends them to the HEAD element of the DOM.  Used
		 * when the 'css-injet-api' feature is true.
		 */
        inject: function () {
            if (has('css-inject-api')) {
                dojo.forEach(arguments, function (arg) {
                    if (arg && arg.nodeType === Node.ELEMENT_NODE && arg.tagName === 'STYLE' && !arg.parentNode) {
                        // Unparented style node.  Add it to the DOM
                        domConstruct.place(arg, head);
                    }
                });
            }
        },

		/*
		 * Installs the global require() and define() function intercepts.
		 *
		 * @return An object with a remove() function that can be called to cancel the intercepts
		 *         (useful for unit testing).
		 */
        installAutoInjectHooks: function () {
            if (has('css-inject-api')) {
                var self = this,

                    // The return value from this function replaces the callback passed
                    // to require() or define().  The callback replacement scans the arguments
                    // for context require instances that need to be intercepted, and invokes
                    // our inject() api before calling the original callback.
                    callbackIntercept = function (moduleIdList, callbackFn) {
                        return function () {
                            var i, args = arguments, newArgValues = [];
                            dojo.forEach(moduleIdList, function (mid, i) {
                                newArgValues.push(mid === "require" ? contextRequireIntercept(args[i]) : args[i]);
                            });
                            self.inject.apply(this, newArgValues);
                            return callbackFn.apply(this, newArgValues);
                        };
                    },

                    // The return value from this function replaces the context require passed
                    // in a require() or define() callback with a function that invokes our
                    // intercept.
                    contextRequireIntercept = function (contextRequire) {
                        return lang.mixin(function () {
                            return contextRequire.apply(this, reqDefIntercept.apply(this, arguments));
                        }, contextRequire);
                    },

                    // The require()/define() intercept.  This function scans the require()/define()
                    // arguments and replaces the callback function with our calback intercept
                    // function.
                    reqDefIntercept = function () {
                        // copy arguments to a new array that will contain our callback intercept
                        var callback, moduleList, newArgs = [];
                        dojo.forEach(arguments, function (arg) {
                            if (!callback && !moduleList && lang.isArray(arg)) {
                                moduleList = arg;
                            }
                            if (!callback && lang.isFunction(arg) && moduleList) {
                                callback = arg;
                                arg = callbackIntercept(moduleList, callback);
                            }
                            newArgs.push(arg);
                        });
                        return newArgs;
                    },

                    result;

                // install our intercepts for the global require() and define() functions
                (function () {
                    // Make sure we're looking at the global require/define
                    var signals = [], req = this.require, def = this.define;
                    signals.push(aspect.before(this, "define", reqDefIntercept));
                    signals.push(aspect.before(this, "require", reqDefIntercept));
                    if (!this.define.amd) {
                        // mixin properties defined in original functions
                        lang.mixin(this.define, def);
                        lang.mixin(this.require, req);
                    }
                    result = {
                        remove: function () {
                            dojo.forEach(signals, function (signal) {
                                signal.remove();
                            });
                        }
                    };
                })();
                return result;
            }
        },

        // export utility functions for unit tests
        __isLessUrl: isLessUrl,
        __isRelative: isRelative,
        __dequote: dequote,
        __normalize: normalize,
        __resolve: resolve,
        __fixUrlsInCssFile: fixUrlsInCssFile
    };
});