var profile = (function(){
    var copyOnly = function (filename, mid) {
        var list = {
            "hpcc/eclwatch.profile": true,
            "hpcc/eclwatch.json": true,
            "hpcc/dojoConfig": true,
            "hpcc/viz/DojoD3": true,
            "hpcc/viz/DojoD32DChart": true,
            "hpcc/viz/DojoD3NDChart": true,
            "hpcc/viz/DojoD3Choropleth": true
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