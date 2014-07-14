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
    "dojo/_base/array",
    "dojo/Deferred",
    "dojo/dom",
    "dojo/dom-construct",

    "hpcc/ESPWorkunit",
    "hpcc/ESPQuery",
    "hpcc/WsEcl"

], function (declare, arrayUtil, Deferred, dom, domConstruct,
    ESPWorkunit, ESPQuery, WsEcl) {

    var DebugBase = declare(null, {
        showDebugRow: function (node, key, value) {
            var domNode = dom.byId(node);
            var lineNode = domConstruct.create("li", {
                innerHTML: "<b>" + key + "</b>:  " + value
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

    return {
        getPageInfo: function () {
            return new PageInfo();
        },

        callExt: function (querySet, queryID, query) {
            var deferred = new Deferred();
            WsEcl.Call(querySet, queryID, query).then(function (response) {
                deferred.resolve(response);
                return response;
            });
            return deferred.promise;
        },

        call: function (query) {
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
        },

        arrayToMap: function (arr, keyField) {
            var retVal = {};
            arrayUtil.forEach(arr, function (item, idx) {
                retVal[item[keyField]] = item;
                delete retVal[item[keyField]][keyField];
            });
            return retVal;
        },

        fetchQuery: function() {
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
        },

        fetchWorkunit: function () {
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
    };
});
