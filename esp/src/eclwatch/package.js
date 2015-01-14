var profile = (function(){
    copyOnly = function (filename, mid) {
        var list = {
            "hpcc/eclwatch.profile": true,
            "hpcc/eclwatch.json": true,
            "hpcc/dojoConfig": true,
            "hpcc/viz/DojoD3WordCloud": true,
            "hpcc/viz/d3-cloud/d3.layout.cloud": true,
            "hpcc/viz/map/us.json": true,
            "hpcc/viz/map/us_counties.json": true,
            "hpcc/viz/DojoD3": true,
            "hpcc/viz/DojoD3BarChart": true,
            "hpcc/viz/DojoD3Choropleth": true,
            "hpcc/viz/DojoD3Choropleth": true,
            "hpcc/viz/DojoD3CooccurrenceGraph": true,
            "hpcc/viz/DojoD3Histogram": true,
            "hpcc/viz/DojoD3PieChart": true,
            "hpcc/viz/DojoD3ScatterChart": true,
            "hpcc/viz/DojoD3DonutChart": true,
            "hpcc/viz/DojoD3ForceDirectedGraph": true,
            "hpcc/viz/DojoSlider": true
        };
        return (mid in list) ||
            (/^hpcc\/resources\//.test(mid) && !/\.css$/.test(filename)) ||
            /(png|jpg|jpeg|gif|tiff)$/.test(filename);
    };

    return {
        destLocation: "eclwatch",
        resourceTags: {
            test: function (filename, mid) {
                return false;
            },

            copyOnly: function (filename, mid) {
                return copyOnly(filename, mid);
            },

            amd: function (filename, mid) {
                return !copyOnly(filename, mid) && /\.js$/.test(filename);
            },

            miniExclude: function (filename, mid) {
                return mid in {
                    'hpcc/package': 1
                };
            }
        }
    };
})();