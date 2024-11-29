import * as arrayUtil from "dojo/_base/array";
import * as Deferred from "dojo/_base/Deferred";
import * as lang from "dojo/_base/lang";
import * as request from "dojo/request";
import * as script from "dojo/request/script";
import * as xhr from "dojo/request/xhr";

import * as WsTopology from "./WsTopology";

declare const dojoConfig;

export function _flattenResults(results) {
    if (Object.prototype.toString.call(results) === "[object Array]") {
        for (let i = 0; i < results.length; ++i) {
            results[i] = this._flattenResults(results[i]);
        }
    } else if (Object.prototype.toString.call(results) === "[object Object]") {
        for (const key in results) {
            results[key] = this._flattenResults(results[key]);
            if (key === "Row") {
                return results.Row;
            }
        }
    }
    return results;
}
// http://192.168.1.201:8002/WsEcl/submit/query/roxie/countydeeds.1/json?year=2013&jsonp=XYZ
export function Call(target, method, query) {
    const deferred = new Deferred();
    const context = this;
    let request = null;
    if (dojoConfig.urlInfo.baseHost) {
        request = script.get(dojoConfig.urlInfo.baseHost + "/WsEcl/submit/query/" + target + "/" + method + "/json", {
            query,
            jsonp: "jsonp"
        });
    } else {
        request = xhr.get("/WsEcl/submit/query/" + target + "/" + method + "/json", {
            query,
            handleAs: "json"
        });
    }
    request.then(function (response) {
        let results = response[method + "Response"] && response[method + "Response"].Results ? response[method + "Response"].Results : {};
        results = context._flattenResults(results);
        deferred.resolve(results);
    });
    return deferred.promise;
}
export function CallURL(url, query) {
    //  http://X.X.X.X:8002/WsEcl/submit/query/roxie/method/json
    const urlParts = url.split("/");
    const method = urlParts[urlParts.length - 2];
    const context = this;
    return script.get(url, {
        query,
        jsonp: "jsonp"
    }).then(function (response) {
        const results = response[method + "Response"] && response[method + "Response"].Results ? response[method + "Response"].Results : {};
        return context._flattenResults(results);
    });
}
export function Submit(target, method, query) {
    const deferred = new Deferred();
    const context = this;
    WsTopology.GetWsEclURL("submit").then(function (response) {
        const url = response + target + "/" + method + "/json";
        script.get(url, {
            query,
            jsonp: "jsonp"
        }).then(function (response) {
            let results = response[method + "Response"] && response[method + "Response"].Results ? response[method + "Response"].Results : {};
            results = context._flattenResults(results);
            if (lang.exists("Exceptions.Exception", response)) {
                results.Exception = response.Exceptions.Exception;
            }
            deferred.resolve(results);
        });
    });
    return deferred.promise;
}
export function SubmitXML(target, domXml) {
    domXml = domXml.firstChild;
    let method = domXml.tagName;
    method = method.slice(0, -7); // "Request"
    const query = {};
    arrayUtil.forEach(domXml.childNodes, function (item, idx) {
        query[item.tagName] = item.textContent;
    });
    return this.Submit(target, method, query);
}
// http://192.168.1.201:8002/WsEcl/example/request/query/roxie/countydeeds.1
export function ExampleRequest(target, method) {
    const deferred = new Deferred();
    WsTopology.GetWsEclURL("example/request").then(function (response) {
        const url = response + target + "/" + method;
        //  HPCC-10488  ---
        //  script.get(url, {
        //    query: query,
        //    jsonp: "jsonp"
        request.get(url, {
            handleAs: "xml"
        }).then(function (response) {
            const fields = [];
            arrayUtil.forEach(response.getElementsByTagName(method + "Request"), function (item, idx) {
                arrayUtil.forEach(item.childNodes, function (child_item, idx) {
                    fields.push(child_item.tagName);
                });
            });
            deferred.resolve(fields);
        });
    });
    return deferred.promise;
}
