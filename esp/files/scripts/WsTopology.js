/*##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
