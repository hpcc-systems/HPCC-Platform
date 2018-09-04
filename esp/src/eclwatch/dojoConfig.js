var dojoConfig;
var debugHPCC_JS = false; //  Should never be TRUE in a PR  ---

function getConfig(env) {
    // dojoRoot is defined if we're running in node (i.e. building)
    var dojoRoot = env.dojoRoot;
    var baseUrl = dojoRoot ? "." : "/esp/files";
    var hpccBaseUrl = baseUrl + "/node_modules/@hpcc-js";
    var hpccMin = debugHPCC_JS ? "" : ".min";

    return {
        baseUrl: baseUrl,
        deps: ["hpcc/stub"],
        async: true,

        parseOnLoad: false,
        isDebug: (typeof debugConfig !== "undefined"),
        vizDebug: false,
        selectorEngine: "lite",
        blankGif: "/esp/files/eclwatch/img/blank.gif",
        paths: {
            "hpcc": baseUrl + "/eclwatch",
            "src": baseUrl + "/lib/src",
            "templates": baseUrl + "/eclwatch/templates",
            "ecl": baseUrl + "/eclwatch/ecl",
            "css": baseUrl + "/loader/css",
            "d3-array": baseUrl + "/node_modules/d3-array/build/d3-array" + hpccMin,
            "d3-collection": baseUrl + "/node_modules/d3-collection/build/d3-collection" + hpccMin,
            "d3-color": baseUrl + "/node_modules/d3-color/build/d3-color" + hpccMin,
            "d3-format": baseUrl + "/node_modules/d3-format/build/d3-format" + hpccMin,
            "d3-interpolate": baseUrl + "/node_modules/d3-interpolate/build/d3-interpolate" + hpccMin,
            "d3-scale": baseUrl + "/node_modules/d3-scale/build/d3-scale" + hpccMin,
            "d3-selection": baseUrl + "/node_modules/d3-selection/build/d3-selection" + hpccMin,
            "d3-time": baseUrl + "/node_modules/d3-time/build/d3-time" + hpccMin,
            "d3-time-format": baseUrl + "/node_modules/d3-time-format/build/d3-time-format" + hpccMin,
            "@hpcc-js/api": baseUrl + "/node_modules/@hpcc-js/api/dist/index" + hpccMin,
            "@hpcc-js/chart": baseUrl + "/node_modules/@hpcc-js/chart/dist/index" + hpccMin,
            "@hpcc-js/common": baseUrl + "/node_modules/@hpcc-js/common/dist/index" + hpccMin,
            "@hpcc-js/comms": baseUrl + "/node_modules/@hpcc-js/comms/dist/index" + hpccMin,
            "@hpcc-js/composite": baseUrl + "/node_modules/@hpcc-js/composite/dist/index" + hpccMin,
            "@hpcc-js/dgrid": baseUrl + "/node_modules/@hpcc-js/dgrid/dist/index" + hpccMin,
            "@hpcc-js/dgrid-shim": baseUrl + "/node_modules/@hpcc-js/dgrid-shim/dist/index" + hpccMin,
            "@hpcc-js/eclwatch": baseUrl + "/node_modules/@hpcc-js/eclwatch/dist/index" + hpccMin,
            "@hpcc-js/form": baseUrl + "/node_modules/@hpcc-js/form/dist/index" + hpccMin,
            "@hpcc-js/graph": baseUrl + "/node_modules/@hpcc-js/graph/dist/index" + hpccMin,
            "@hpcc-js/layout": baseUrl + "/node_modules/@hpcc-js/layout/dist/index" + hpccMin,
            "@hpcc-js/map": baseUrl + "/node_modules/@hpcc-js/map/dist/index" + hpccMin,
            "@hpcc-js/other": baseUrl + "/node_modules/@hpcc-js/other/dist/index" + hpccMin,
            "@hpcc-js/timeline": baseUrl + "/node_modules/@hpcc-js/timeline/dist/index" + hpccMin,
            "@hpcc-js/tree": baseUrl + "/node_modules/@hpcc-js/tree/dist/index" + hpccMin,
            "@hpcc-js/util": baseUrl + "/node_modules/@hpcc-js/util/dist/index" + hpccMin,
            "@hpcc-js/TopoJSON": dojoRoot ? "/esp/files/dist/TopoJSON" : baseUrl + "/node_modules/@hpcc-js/map/TopoJSON",
            "clipboard": baseUrl + "/node_modules/clipboard/dist/clipboard",
            "codemirror": baseUrl + "/node_modules/codemirror",
            "crossfilter": baseUrl + "/node_modules/crossfilter2/crossfilter.min",
            "font-awesome": baseUrl + "/node_modules/@hpcc-js/common/font-awesome",
            "tslib": baseUrl + "/node_modules/tslib/tslib"
        },
        packages: [
            {
                name: 'dojo',
                location: baseUrl + '/node_modules/dojo',
                lib: '.'
            },
            {
                name: 'dijit',
                location: baseUrl + '/node_modules/dijit',
                lib: '.'
            },
            {
                name: 'dojox',
                location: baseUrl + '/node_modules/dojox',
                lib: '.'
            },
            {
                name: 'dojo-themes',
                location: baseUrl + '/node_modules/dojo-themes',
                lib: '.'
            },
            {
                name: 'dgrid',
                location: baseUrl + '/dgrid',
                lib: '.'
            },
            {
                name: 'xstyle',
                location: baseUrl + '/xstyle',
                lib: '.'
            },
            {
                name: 'put-selector',
                location: baseUrl + '/put-selector',
                lib: '.'
            }
        ]
    };
}

// For Webpack, export the config.  This is needed both at build time and on the client at runtime
// for the packed application.
if (typeof module !== 'undefined' && module) {
    module.exports = getConfig;
} else {
    dojoConfig = getConfig({});
}
