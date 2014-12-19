var dojoConfig = (function () {
    var initUrl = function () {
        var baseHost = (typeof debugConfig !== "undefined" ) ? "http://" + debugConfig.IP + ":" + debugConfig.Port : "";
        var hashNodes = location.hash.split("#");
        var searchNodes = location.search.split("?");
        var pathnodes = location.pathname.split("/");
        pathnodes.pop();

        return {
            hostname: location.hostname,
            port: location.port,
            pathname: location.pathname,
            hash: hashNodes.length >= 2 ? hashNodes[1] : "",
            params: searchNodes.length >= 2 ? searchNodes[1] : "",
            baseHost: baseHost,
            basePath: baseHost + "/esp/files",
            pluginsPath: baseHost + "/esp/files",
            resourcePath: baseHost + "/esp/files/eclwatch",
            scriptsPath: baseHost + "/esp/files/eclwatch",
            thisPath: pathnodes.join("/")
        };
    }

    var urlInfo = initUrl();

    return {
        async: true,
        parseOnLoad: false,
        urlInfo: urlInfo,
        isDebug: (typeof debugConfig !== "undefined"),
        selectorEngine: "lite",
        getURL: function (name) {
            return this.urlInfo.resourcePath + "/" + name;
        },
        getImageURL: function (name) {
            return this.getURL("img/" + name);
        },
        getImageHTML: function (name, tooltip) {
            return "<img src='" + this.getImageURL(name) + "'" + (tooltip ? " title='" + tooltip + "'" : "") + " class='iconAlign'/>";
        },
        isPluginInstalled: function () {
            try {
                var o = new ActiveXObject("HPCCSystems.HPCCSystemsGraphViewControl.1");
                o = null;
                return true;
            } catch (e) { 
            }
            if (navigator.plugins) {
                for (var i = 0, p = navigator.plugins, l = p.length; i < l; i++) {
                    if (p[i].name.indexOf("HPCCSystemsGraphViewControl") > -1) {
                        return true;
                    }
                }
            }
            return false;
        },
        paths: {
            //  Visualization Paths  ---
            "async": urlInfo.basePath + "/Visualization/widgets/lib/requirejs/plugins/async",
            "css": urlInfo.basePath + "/Visualization/widgets/lib/requirejs/plugins/css",
            "goog": urlInfo.basePath + "/Visualization/widgets/lib/requirejs/plugins/goog",
            "propertyParser": urlInfo.basePath + "/Visualization/widgets/lib/requirejs/plugins/propertyParser"
        },
        packages: [{
            name: "hpcc",
            location: urlInfo.scriptsPath
        }, {
            name: "templates",
            location: urlInfo.resourcePath + "/templates"
        }, {
            name: "ecl",
            location: urlInfo.resourcePath + "/ecl"
        }, {
            name: "plugins",
            location: urlInfo.pluginsPath
        }, {
            name: "src",
            location: urlInfo.basePath + "/Visualization/widgets/src"
        }, {
            name: "lib",
            location: urlInfo.basePath + "/Visualization/widgets/lib"
        }, {
            name: "d3",
            location: urlInfo.basePath + "/Visualization/widgets/lib/d3",
            main:"d3"
        }, {
            name: "topojson",
            location: urlInfo.basePath + "/Visualization/widgets/lib/topojson"
        }, {
            name: "this",
            location: urlInfo.thisPath
        }],
        debounce: function (func, threshold, execAsap) {
            var timeout;
            return function debounced() {
                var obj = this, args = arguments;
                function delayed() {
                    if (!execAsap)
                        func.apply(obj, args);
                    timeout = null;
                };
                if (timeout)
                    clearTimeout(timeout);
                else if (execAsap)
                    func.apply(obj, args);
                timeout = setTimeout(delayed, threshold || 100);
            }
        }
    };
})();
