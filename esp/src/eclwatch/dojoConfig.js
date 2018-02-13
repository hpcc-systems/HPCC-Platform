var dojoConfig;

function getConfig(env) {
    // dojoRoot is defined if we're running in node (i.e. building)
    var dojoRoot = env.dojoRoot;
    var baseUrl = dojoRoot ? "." : "/esp/files";

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
            "src": baseUrl + "/lib",
            "templates": baseUrl + "/eclwatch/templates",
            "ecl": baseUrl + "/eclwatch/ecl",
            "css": baseUrl + "/loader/css",
            "d3-selection": baseUrl + "/node_modules/d3-selection/build/d3-selection",
            "@hpcc-js/api": baseUrl + "/node_modules/@hpcc-js/api/dist/index",
            "@hpcc-js/chart": baseUrl + "/node_modules/@hpcc-js/chart/dist/index",
            "@hpcc-js/common": baseUrl + "/node_modules/@hpcc-js/common/dist/index",
            "@hpcc-js/comms": baseUrl + "/node_modules/@hpcc-js/comms/dist/index",
            "@hpcc-js/composite": baseUrl + "/node_modules/@hpcc-js/composite/dist/index",
            "@hpcc-js/form": baseUrl + "/node_modules/@hpcc-js/form/dist/index",
            "@hpcc-js/graph": baseUrl + "/node_modules/@hpcc-js/graph/dist/index",
            "@hpcc-js/layout": baseUrl + "/node_modules/@hpcc-js/layout/dist/index",
            "@hpcc-js/map": baseUrl + "/node_modules/@hpcc-js/map/dist/index",
            "@hpcc-js/other": baseUrl + "/node_modules/@hpcc-js/other/dist/index",
            "@hpcc-js/tree": baseUrl + "/node_modules/@hpcc-js/tree/dist/index",
            "@hpcc-js/util": baseUrl + "/node_modules/@hpcc-js/util/dist/index",
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
