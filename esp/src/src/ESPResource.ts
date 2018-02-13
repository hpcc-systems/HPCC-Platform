import * as declare from "dojo/_base/declare";
import * as arrayUtil from "dojo/_base/array";
import * as Deferred from "dojo/Deferred";
import * as dom from "dojo/dom";
import * as domConstruct from "dojo/dom-construct";

import * as ESPWorkunit from "./ESPWorkunit";
import * as ESPQuery from "./ESPQuery";
import * as WsEcl from "./WsEcl";
import * as Utility from "./Utility";

declare const debugConfig;

var DebugBase = declare(null, {
    showDebugRow: function (node, key, value) {
        var domNode = dom.byId(node);
        domConstruct.create("li", {
            innerHTML: "<b>" + Utility.xmlEncode(key) + "</b>:  " + Utility.xmlEncode(value)
        }, domNode);
    },

    showDebugInfo: function (/*DOMNode|String*/ node, obj) {
        var domNode = dom.byId(node);
        if (!obj) {
            obj = this;
        }
        var ulNode = domConstruct.create("ul", {}, domNode);
        for (var key in obj) {
            if (obj.hasOwnProperty(key)) {
                this.showDebugRow(ulNode, key, obj[key]);
            }
        }
    }
});

var PageInfo = declare(DebugBase, {

    constructor: function () {
        var pathname = (typeof debugConfig !== "undefined") ? debugConfig.pathname : location.pathname;
        var pathnodes = pathname.split("/");
        pathnodes.pop();
        this.pathfolder = pathnodes.join("/");

        if (pathname.indexOf("/WsWorkunits/res/") === 0) {
            this.wuid = pathnodes[3];
        } else if (pathname.indexOf("/WsEcl/res/") === 0) {
            this.querySet = pathnodes[4];
            this.queryID = pathnodes[5];
            var queryIDParts = pathnodes[5].split(".");
            this.queryDefaultID = queryIDParts[0];
            if (queryIDParts.length > 1) {
                this.queryVersion = queryIDParts[1];
            }
            this.queryQualifiedDefaultID = this.querySet + "." + this.queryDefaultID;
            this.queryQualifiedID = this.querySet + "." + this.queryID;
        }
    },

    isWorkunit: function () {
        return (this.wuid);
    },

    isQuery: function () {
        return (this.querySet && this.queryID);
    }
});

export function getPageInfo() {
    return new PageInfo();
}

export function callExt(querySet, queryID, query) {
    var deferred = new Deferred();
    WsEcl.Call(querySet, queryID, query).then(function (response) {
        deferred.resolve(response);
        return response;
    });
    return deferred.promise;
}

export function call(query) {
    var deferred = new Deferred();
    var pageInfo = this.getPageInfo();
    if (pageInfo.isQuery()) {
        this.callExt(pageInfo.querySet, pageInfo.queryID, query).then(function (response) {
            deferred.resolve(response);
            return response;
        });
    } else if (pageInfo.isWorkunit()) {
        var wu = ESPWorkunit.Get(pageInfo.wuid);
        wu.fetchAllNamedResults(0, 10).then(function (response) {
            deferred.resolve(response);
            return response;
        });
    } else {
        deferred.resolve(null);
    }
    return deferred.promise;
}

export function arrayToMap(arr, keyField) {
    var retVal = {};
    arrayUtil.forEach(arr, function (item, idx) {
        retVal[item[keyField]] = item;
        delete retVal[item[keyField]][keyField];
    });
    return retVal;
}

export function fetchQuery() {
    var deferred = new Deferred();
    var pageInfo = this.getPageInfo();
    if (pageInfo.isQuery()) {
        var query = ESPQuery.Get(pageInfo.querySet, pageInfo.queryID);
        query.refresh().then(function (response) {
            deferred.resolve(query);
        });
    } else {
        deferred.resolve(null);
    }
    return deferred.promise;
}

export function fetchWorkunit() {
    var deferred = new Deferred();
    var pageInfo = this.getPageInfo();
    if (pageInfo.isWorkunit()) {
        var wu = ESPWorkunit.Get(pageInfo.wuid);
        wu.refresh(true).then(function (response) {
            deferred.resolve(wu);
        });
    } else {
        deferred.resolve(null);
    }
    return deferred.promise;
}
