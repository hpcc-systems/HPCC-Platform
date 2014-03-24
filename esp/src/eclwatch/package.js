var profile = (function(){
    copyOnly = function (filename, mid) {
        var list = {
            "hpcc/eclwatch.profile": true,
            "hpcc/eclwatch.json": true,
            "hpcc/dojoConfig": true,
            "hpcc/viz/map/us.json": true,
            "hpcc/viz/map/us_counties.json": true
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