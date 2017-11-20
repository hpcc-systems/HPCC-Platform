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
            "@hpcc-js/api": baseUrl + "/node_modules/@hpcc-js/api/dist/api",
            "@hpcc-js/chart": baseUrl + "/node_modules/@hpcc-js/chart/dist/chart",
            "@hpcc-js/common": baseUrl + "/node_modules/@hpcc-js/common/dist/common",
            "@hpcc-js/composite": baseUrl + "/node_modules/@hpcc-js/composite/dist/composite",
            "@hpcc-js/form": baseUrl + "/node_modules/@hpcc-js/form/dist/form",
            "@hpcc-js/graph": baseUrl + "/node_modules/@hpcc-js/graph/dist/graph",
            "@hpcc-js/layout": baseUrl + "/node_modules/@hpcc-js/layout/dist/layout",
            "@hpcc-js/map": baseUrl + "/node_modules/@hpcc-js/map/dist/map",
            "@hpcc-js/other": baseUrl + "/node_modules/@hpcc-js/other/dist/other",
            "clipboard": baseUrl + "/node_modules/clipboard/dist/clipboard",
            "crossfilter": baseUrl + "/crossfilter/crossfilter.min"
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
