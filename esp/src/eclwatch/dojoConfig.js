var dojoConfig = (function () {
    var initUrl = function () {
        var pathnodes = location.pathname.split("/");
        pathnodes.pop();

        var hashNodes = location.hash.split("#");

        var searchNodes = location.search.split("?");

        return {
            pathname: location.pathname,
            hash: hashNodes.length >= 2 ? hashNodes[1] : "",
            params: searchNodes.length >= 2 ? searchNodes[1] : "",
            basePath: "/esp/files",
            resourcePath: "/esp/files/eclwatch",
            scriptsPath: "/esp/files/eclwatch"
        };
    }

    var urlInfo = initUrl();

    return {
        async: true,
        parseOnLoad: false,
        urlInfo: urlInfo,
        getURL: function (name) {
            return this.urlInfo.resourcePath + "/" + name;
        },
        getImageURL: function (name) {
            return this.getURL("img/" + name);
        },
        getImageHTML: function (name) {
            return "<img src='" + this.getImageURL(name) + "'/>";
        },
        packages: [{
            name: "d3",
            location: urlInfo.basePath + "/d3"
        }, {
            name: "topojson",
            location: urlInfo.basePath + "/topojson"
        }, {
            name: "hpcc",
            location: urlInfo.scriptsPath
        }, {
            name: "viz",
            location: urlInfo.scriptsPath + "/viz"
        }, {
            name: "templates",
            location: urlInfo.resourcePath + "/templates"
        }, {
            name: "ecl",
            location: urlInfo.resourcePath + "/ecl"
        }]
    };
})();
