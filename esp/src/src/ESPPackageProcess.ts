import * as declare from "dojo/_base/declare";
import * as arrayUtil from "dojo/_base/array";
import * as lang from "dojo/_base/lang";
import * as Deferred from "dojo/_base/Deferred";
import * as ObjectStore from "dojo/data/ObjectStore";
import * as QueryResults from "dojo/store/util/QueryResults";
import * as Observable from "dojo/store/Observable";

import * as WsPackageMaps from "./WsPackageMaps";
import * as ESPUtil from "./ESPUtil";

var _packageMaps = {};
var Store = declare(null, {
    idProperty: "Id",

    _watched: {},

    constructor: function (options) {
        declare.safeMixin(this, options);
    },

    getIdentity: function (object) {
        return object[this.idProperty];
    },

    get: function (id) {
        if (!_packageMaps[id]) {
            _packageMaps[id] = new packageMap({
                Id: id
            });
        }
        return _packageMaps[id];
    },

    sortPackageMaps: function (packageMaps, sortIn) {
        packageMaps.sort(function (a, b) {
            var vA = a.Id;
            var vB = b.Id;
            if (sortIn.attribute === 'Target') {
                vA = a.Target;
                vB = b.Target;
            }
            else if (sortIn.attribute === 'Process') {
                vA = a.Process;
                vB = b.Process;
            }
            else if (sortIn.attribute === 'Description') {
                vA = a.Description;
                vB = b.Description;
            }
            else if (sortIn.attribute === 'Active') {
                vA = a.Active;
                vB = b.Active;
            }
            if (sortIn.descending) {
                if (vA < vB) //sort string ascending
                    return 1;
                if (vA > vB)
                    return -1;
            }
            else {
                if (vA < vB)
                    return -1;
                if (vA > vB)
                    return 1;
            }
            return 0 //default return value (no sorting)
        })
        return packageMaps;
    },

    query: function (query, options) {
        var request = {};
        lang.mixin(request, options.query);
        if (options.query.Target)
            request['Target'] = options.query.Target;
        if (options.query.Process)
            request['Process'] = options.query.Process;
        if (options.query.ProcessFilter)
            request['ProcessFilter'] = options.query.ProcessFilter;

        var results = WsPackageMaps.PackageMapQuery({
            request: request
        });

        var deferredResults = new Deferred();
        deferredResults.total = results.then(function (response) {
            if (lang.exists("ListPackagesResponse.NumPackages", response)) {
                return response.ListPackagesResponse.NumPackages;
            }
            return 0;
        });
        var context = this;
        Deferred.when(results, function (response) {
            var packageMaps = [];
            for (var key in context._watched) {
                context._watched[key].unwatch();
            }
            this._watched = {};
            if (lang.exists("ListPackagesResponse.PackageMapList.PackageListMapData", response)) {
                arrayUtil.forEach(response.ListPackagesResponse.PackageMapList.PackageListMapData, function (item, index) {
                    var packageMap = context.get(item.Id);
                    packageMap.updateData(item);
                    packageMaps.push(packageMap);
                    context._watched[packageMap.Id] = packageMap.watch("__hpcc_changedCount", function (name, oldValue, newValue) {
                        if (oldValue !== newValue) {
                            context.notify(packageMap, packageMap.Id);
                        }
                    });
                });
                if (options.sort) {
                    packageMaps = context.sortPackageMaps(packageMaps, options.sort[0]);
                }
            }
            deferredResults.resolve(packageMaps);
        });

        return QueryResults(deferredResults);
    }
});

var packageMap = declare([ESPUtil.Singleton], {
    constructor: ESPUtil.override(function (inherited, args) {
        inherited();
        declare.safeMixin(this, args);
        this.packageMap = this;
    })
});

export function CreatePackageMapQueryObjectStore(options) {
    var store = new Store(options);
    store = Observable(store);
    var objStore = new ObjectStore({ objectStore: store });
    return objStore;
}
