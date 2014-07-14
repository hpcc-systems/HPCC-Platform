/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/_base/Deferred",
    "dojo/request",
    "dojo/request/script",
    "dojo/request/xhr",

    "hpcc/WsTopology"
], function (declare, lang, arrayUtil, Deferred, request, script, xhr,
    WsTopology) {

    return {
        _flattenResults: function (results) {
            if (Object.prototype.toString.call(results) === '[object Array]') {
                for (var i = 0; i < results.length; ++i) {
                    results[i] = this._flattenResults(results[i]);
                }
            } else if (Object.prototype.toString.call(results) === '[object Object]') {
                var d = Object.prototype.toString.call(results);
                for (var key in results) {
                    results[key] = this._flattenResults(results[key]);
                    if (key === "Row") {
                        return results.Row;
                    }
                }
            }
            return results;
        },
        //http://192.168.1.201:8002/WsEcl/submit/query/roxie/countydeeds.1/json?year=2013&jsonp=XYZ
        Call: function (target, method, query) {
            var deferred = new Deferred();
            var context = this;
            var request = null;
            if (dojoConfig.urlInfo.baseHost) {
                request = script.get(dojoConfig.urlInfo.baseHost + "/WsEcl/submit/query/" + target + "/" + method + "/json", {
                    query: query,
                    jsonp: "jsonp"
                });
            } else {
                request = xhr.get("/WsEcl/submit/query/" + target + "/" + method + "/json", {
                    query: query,
                    handleAs: "json"
                });
            }
            request.then(function (response) {
                var results = response[method + "Response"] && response[method + "Response"].Results ? response[method + "Response"].Results : {};
                results = context._flattenResults(results);
                deferred.resolve(results);
            });
            return deferred.promise;
        },
        CallURL: function (url, query) {
            //  http://X.X.X.X:8002/WsEcl/submit/query/roxie/method/json
            var urlParts = url.split("/");
            var method = urlParts[urlParts.length - 2];
            var context = this;
            return script.get(url, {
                query: query,
                jsonp: "jsonp"
            }).then(function (response) {
                var results = response[method + "Response"] && response[method + "Response"].Results ? response[method + "Response"].Results : {};
                return context._flattenResults(results);
            });
        },
        Submit: function (target, method, query) {
            var deferred = new Deferred();
            var context = this;
            WsTopology.GetWsEclURL("submit").then(function (response) {
                var url = response + target + "/" + method + "/json";
                script.get(url, {
                    query: query,
                    jsonp: "jsonp"
                }).then(function (response) {
                    var results = response[method + "Response"] && response[method + "Response"].Results ? response[method + "Response"].Results : {};
                    results = context._flattenResults(results);
                    if (lang.exists("Exceptions.Exception", response)) {
                        results.Exception = response.Exceptions.Exception;
                    };
                    deferred.resolve(results);
                });
            });
            return deferred.promise;
        },
        SubmitXML: function (target, domXml) {
            domXml = domXml.firstChild;
            var method = domXml.tagName;
            method = method.slice(0, -7); //"Request"
            var query = {};
            arrayUtil.forEach(domXml.childNodes, function (item, idx) {
                query[item.tagName] = item.textContent;
            });
            return this.Submit(target, method, query);
        },
        //http://192.168.1.201:8002/WsEcl/example/request/query/roxie/countydeeds.1
        ExampleRequest: function(target, method) {
            var deferred = new Deferred();
            var context = this;
            WsTopology.GetWsEclURL("example/request").then(function (response) {
                var url = response + target + "/" + method;
                //  HPCC-10488  ---
                //  script.get(url, {
                //    query: query,
                //    jsonp: "jsonp"
                request.get(url, {
                    handleAs: "xml"
                }).then(function (response) {
                    var fields = [];
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
    };
});
