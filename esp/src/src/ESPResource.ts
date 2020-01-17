import * as arrayUtil from "dojo/_base/array";
import * as declare from "dojo/_base/declare";
import * as Deferred from "dojo/Deferred";
import * as dom from "dojo/dom";
import * as domConstruct from "dojo/dom-construct";

import * as ESPQuery from "./ESPQuery";
import * as ESPWorkunit from "./ESPWorkunit";
import * as Utility from "./Utility";
import * as WsEcl from "./WsEcl";

declare const debugConfig;

const DebugBase = declare(null, {
    showDebugRow(node, key, value) {
        const domNode = dom.byId(node);
        domConstruct.create("li", {
            innerHTML: "<b>" + Utility.xmlEncode(key) + "</b>:  " + Utility.xmlEncode(value)
        }, domNode);
    },

    showDebugInfo(/*DOMNode|String*/ node, obj) {
        const domNode = dom.byId(node);
        if (!obj) {
            obj = this;
        }
        const ulNode = domConstruct.create("ul", {}, domNode);
        for (const key in obj) {
            if (obj.hasOwnProperty(key)) {
                this.showDebugRow(ulNode, key, obj[key]);
            }
        }
    }
});

const PageInfo = declare(DebugBase, {

    constructor() {
        const pathname = (typeof debugConfig !== "undefined") ? debugConfig.pathname : location.pathname;
        const pathnodes = pathname.split("/");
        pathnodes.pop();
        this.pathfolder = pathnodes.join("/");

        if (pathname.indexOf("/WsWorkunits/res/") === 0) {
            this.wuid = pathnodes[3];
        } else if (pathname.indexOf("/WsEcl/res/") === 0) {
            this.querySet = pathnodes[4];
            this.queryID = pathnodes[5];
            const queryIDParts = pathnodes[5].split(".");
            this.queryDefaultID = queryIDParts[0];
            if (queryIDParts.length > 1) {
                this.queryVersion = queryIDParts[1];
            }
            this.queryQualifiedDefaultID = this.querySet + "." + this.queryDefaultID;
            this.queryQualifiedID = this.querySet + "." + this.queryID;
        }
    },

    isWorkunit() {
        return (this.wuid);
    },

    isQuery() {
        return (this.querySet && this.queryID);
    }
});

export function getPageInfo() {
    return new PageInfo();
}

export function callExt(querySet, queryID, query) {
    const deferred = new Deferred();
    WsEcl.Call(querySet, queryID, query).then(function (response) {
        deferred.resolve(response);
        return response;
    });
    return deferred.promise;
}

export function call(query) {
    const deferred = new Deferred();
    const pageInfo = this.getPageInfo();
    if (pageInfo.isQuery()) {
        this.callExt(pageInfo.querySet, pageInfo.queryID, query).then(function (response) {
            deferred.resolve(response);
            return response;
        });
    } else if (pageInfo.isWorkunit()) {
        const wu = ESPWorkunit.Get(pageInfo.wuid);
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
    const retVal = {};
    arrayUtil.forEach(arr, function (item, idx) {
        retVal[item[keyField]] = item;
        delete retVal[item[keyField]][keyField];
    });
    return retVal;
}

export function fetchQuery() {
    const deferred = new Deferred();
    const pageInfo = this.getPageInfo();
    if (pageInfo.isQuery()) {
        const query = ESPQuery.Get(pageInfo.querySet, pageInfo.queryID);
        query.refresh().then(function (response) {
            deferred.resolve(query);
        });
    } else {
        deferred.resolve(null);
    }
    return deferred.promise;
}

export function fetchWorkunit() {
    const deferred = new Deferred();
    const pageInfo = this.getPageInfo();
    if (pageInfo.isWorkunit()) {
        const wu = ESPWorkunit.Get(pageInfo.wuid);
        wu.refresh(true).then(function (response) {
            deferred.resolve(wu);
        });
    } else {
        deferred.resolve(null);
    }
    return deferred.promise;
}
