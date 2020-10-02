// eslint-disable-next-line @typescript-eslint/no-var-requires
var fs = require("fs");

function getConfig(env) {
    // dojoRoot is defined if we're running in node (i.e. building)
    var dojoRoot = env.dojoRoot;
    var baseUrl = dojoRoot ? "." : "/esp/files";
    var hpccBaseUrl = fs.existsSync(baseUrl + "/node_modules/@hpcc-js") ? baseUrl + "/node_modules/@hpcc-js" : baseUrl + "/../../../hpcc-js/packages";

    return {
        baseUrl: baseUrl,
        deps: ["hpcc/stub"],
        async: true,
        // locale: "zh",   // Testing only  ---

        parseOnLoad: false,
        isDebug: (typeof debugConfig !== "undefined"),
        vizDebug: false,
        selectorEngine: "lite",
        blankGif: "/esp/files/eclwatch/img/blank.gif",
        paths: {
            "hpcc": baseUrl + "/eclwatch",
            "src": baseUrl + "/lib/src",
            "ganglia": baseUrl + "/ganglia",
            "templates": baseUrl + "/eclwatch/templates",
            "ecl": baseUrl + "/eclwatch/ecl",
            "css": baseUrl + "/loader/css",
            "@hpcc-js/api": hpccBaseUrl + "/api/dist/index",
            "@hpcc-js/chart": hpccBaseUrl + "/chart/dist/index",
            "@hpcc-js/codemirror": hpccBaseUrl + "/codemirror/dist/index",
            "@hpcc-js/common": hpccBaseUrl + "/common/dist/index",
            "@hpcc-js/comms": hpccBaseUrl + "/comms/dist/index",
            "@hpcc-js/composite": hpccBaseUrl + "/composite/dist/index",
            "@hpcc-js/dgrid": hpccBaseUrl + "/dgrid/dist/index",
            "@hpcc-js/dgrid-shim": hpccBaseUrl + "/dgrid-shim/dist/index",
            "@hpcc-js/eclwatch": hpccBaseUrl + "/eclwatch/dist/index",
            "@hpcc-js/form": hpccBaseUrl + "/form/dist/index",
            "@hpcc-js/graph": hpccBaseUrl + "/graph/dist/index",
            "@hpcc-js/layout": hpccBaseUrl + "/layout/dist/index",
            "@hpcc-js/phosphor": hpccBaseUrl + "/phosphor/dist/index",
            "@hpcc-js/html": hpccBaseUrl + "/html/dist/index",
            "@hpcc-js/map": hpccBaseUrl + "/map/dist/index",
            "@hpcc-js/other": hpccBaseUrl + "/other/dist/index",
            "@hpcc-js/react": hpccBaseUrl + "/react/dist/index",
            "@hpcc-js/timeline": hpccBaseUrl + "/timeline/dist/index",
            "@hpcc-js/tree": hpccBaseUrl + "/tree/dist/index",
            "@hpcc-js/util": hpccBaseUrl + "/util/dist/index",
            "@hpcc-js/TopoJSON": dojoRoot ? "/esp/files/dist/TopoJSON" : hpccBaseUrl + "/map/TopoJSON",
            "clipboard": baseUrl + "/node_modules/clipboard/dist/clipboard",
            "codemirror": baseUrl + "/node_modules/codemirror",
            "crossfilter": baseUrl + "/node_modules/crossfilter2/crossfilter.min",
            "font-awesome": hpccBaseUrl + "/common/font-awesome",
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

// eslint-disable-next-line no-undef
module.exports = getConfig;
