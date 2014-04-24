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

    "hpcc/ESPRequest"
], function (declare, lang, arrayUtil, Deferred,
    ESPRequest) {
    return {
        TpServiceQuery: function (params) {
            lang.mixin(params.request, {
                Type: "ALLSERVICES"
            });
            return ESPRequest.send("WsTopology", "TpServiceQuery", params);
        },
        GetESPServiceBaseURL: function (type) {
            var deferred = new Deferred();
            var context = this;
            this.TpServiceQuery({}).then(function (response) {
                var retVal = "";
                if (lang.exists("TpServiceQueryResponse.ServiceList.TpEspServers.TpEspServer", response)) {
                    arrayUtil.forEach(response.TpServiceQueryResponse.ServiceList.TpEspServers.TpEspServer, function (item, idx) {
                        if (lang.exists("TpBindings.TpBinding", item)) {
                            arrayUtil.forEach(item.TpBindings.TpBinding, function (binding, idx) {
                                if (binding.Name === type) {
                                    retVal = ESPRequest.getURL({
                                        port: binding.Port,
                                        pathname: ""
                                    });
                                    return true;
                                }
                            });
                        }
                        if (retVal !== "")
                            return true;
                    });
                }
                deferred.resolve(retVal);
            });
            return deferred.promise;
        },
        WsEclURL: "",
        GetWsEclURL: function (type) {
            var deferred = new Deferred();
            if (this.WsEclURL === "") {
                var context = this;
                this.GetESPServiceBaseURL("ws_ecl").then(function (response) {
                    context.WsEclURL = response + "/WsEcl/";
                    deferred.resolve(context.WsEclURL + type + "/query/");
                });
            } else {
                deferred.resolve(this.WsEclURL + type + "/query/");
            }
            return deferred.promise;
        },
        WsEclIFrameURL: "",
        GetWsEclIFrameURL: function (type) {
            var deferred = new Deferred();
            if (this.WsEclIFrameURL === "") {
                var context = this;
                this.GetESPServiceBaseURL("ws_ecl").then(function (response) {
                    context.WsEclIFrameURL = response + dojoConfig.urlInfo.basePath + "/stub.htm?Widget=IFrameWidget&src=" + encodeURIComponent("/WsEcl/");
                    deferred.resolve(context.WsEclIFrameURL + encodeURIComponent(type + "/query/"));
                });
            } else {
                deferred.resolve(this.WsEclIFrameURL + encodeURIComponent(type + "/query/"));
            }
            return deferred.promise;
        },
        TpTargetClusterQuery: function (params) {
            return ESPRequest.send("WsTopology", "TpTargetClusterQuery", params);
        },
        TpGroupQuery: function (params) {
            return ESPRequest.send("WsTopology", "TpGroupQuery", params);
        },
        TpLogicalClusterQuery: function (params) {
            return ESPRequest.send("WsTopology", "TpLogicalClusterQuery", params).then(function (response) {
                var best = null;
                var hthor = null;
                if (lang.exists("TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster", response)) {
                    arrayUtil.forEach(response.TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster, function (item, idx) {
                        if (!best) {
                            best = item;
                        }
                        if (item.Name.indexOf("hthor") !== -1) {
                            hthor = item;
                            return false;
                        } else if (item.Name.indexOf("thor") !== -1) {
                            best = item;
                        }
                    });
                }
                if (hthor) {
                    response.TpLogicalClusterQueryResponse["default"] = hthor;
                } else if (best) {
                    response.TpLogicalClusterQueryResponse["default"] = best;
                } else {
                    response.TpLogicalClusterQueryResponse["default"] = null;
                }
                return response;
            });
        }
    };
});
