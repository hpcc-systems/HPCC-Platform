/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/_base/array",
    "dojo/_base/lang",
    "dojo/topic",

    "hpcc/ESPRequest"
], function (arrayUtil, lang, topic,
    ESPRequest) {

    return {
        WUGetXref: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefList", params);
        },
        DFUXRefBuild: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefBuild", params);
        },
        DFUXRefUnusedFiles: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefUnusedFiles", params);
        },
        DFUXRefFoundFiles: function (params) {
            var request = {
                Cluster: params
            }
            return ESPRequest.send("WsDFUXRef", "DFUXRefFoundFiles", {
                request: request
            }).then(function (response){
                var results = [];
                var newRows = [];
                if (lang.exists("DFUXRefFoundFilesQueryResponse.DFUXRefFoundFilesQueryResult.File", response)) {
                    results = response.DFUXRefFoundFilesQueryResponse.DFUXRefFoundFilesQueryResult.File;
                    if (results.length) {
                        arrayUtil.forEach(results, function (row, idx) {
                            newRows.push({
                                Name: row.Partmask,
                                Modified: row.Modified,
                                Parts: row.Numparts,
                                Size: row.Size
                            });
                        });
                    } else if (results.Partmask) {
                        newRows.push({
                            Name: results.Partmask,
                            Modified: results.Modified,
                            Parts: results.Numparts,
                            Size: results.Size
                        });
                    }
                }
                return newRows;
            });
        },
        DFUXRefOrphanFiles: function (params) {
            var request = {
                Cluster:params
            }
            return ESPRequest.send("WsDFUXRef", "DFUXRefOrphanFiles", {
                request: request
            }).then(function (response){
                var results = [];
                var newRows = [];
                if (lang.exists("DFUXRefOrphanFilesQueryResponse.DFUXRefOrphanFilesQueryResult.File", response)) {
                    results = response.DFUXRefOrphanFilesQueryResponse.DFUXRefOrphanFilesQueryResult.File
                    if (results.length) {
                        arrayUtil.forEach(results, function (row, idx) {
                           newRows.push({
                                Name: row.Partmask,
                                Modified: row.Modified,
                                PartsFound: row.Partsfound,
                                TotalParts: row.Numparts,
                                Size: row.Size
                            });
                        });
                    } else if (results.Partmask) {
                        newRows.push({
                            Name: results.Partmask,
                            Modified: results.Modified,
                            PartsFound: results.Partsfound,
                            TotalParts: results.Numparts,
                            Size: results.Size
                        });
                    }
                }
                return newRows;
            });
        },
        DFUXRefMessages: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefMessages", params);
        },
        DFUXRefCleanDirectories: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefCleanDirectories", params);
        },
        DFUXRefLostFiles: function (params) {
            var request = {
                Cluster: params
            }
            return ESPRequest.send("WsDFUXRef", "DFUXRefLostFiles", {
                request: request
            }).then(function (response){
                var results = [];
                var newRows = [];
                if (lang.exists("DFUXRefLostFilesQueryResponse.DFUXRefLostFilesQueryResult.File", response)) {
                    results = response.DFUXRefLostFilesQueryResponse.DFUXRefLostFilesQueryResult.File
                    if (results.length) {
                        arrayUtil.forEach(results, function (row, idx) {
                           newRows.push({
                                Name: row.Name,
                                Modified: row.Modified,
                                Numparts: row.Numparts,
                                Size: row.Size,
                                Partslost: row.Partslost,
                                Primarylost: row.Primarylost,
                                Replicatedlost: row.Replicatedlost
                            });
                        });
                    } else if (results.Name) {
                        newRows.push({
                            Name: results.Name,
                            Modified: results.Modified,
                            Numparts: results.Numparts,
                            Size: results.Size,
                            Partslost: results.Partslost,
                            Primarylost: results.Primarylost,
                            Replicatedlost: results.Replicatedlost
                        });
                    }
                }
                return newRows
            });

        },
        DFUXRefDirectories: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefDirectories", params);
        },
        DFUXRefBuildCancel: function (params) {
            return ESPRequest.send("WsDFUXRef", "DFUXRefBuildCancel", params);
        },
        DFUXRefArrayAction: function (xrefFiles, actionType, cluster, type) {
            arrayUtil.forEach(xrefFiles, function (item, idx) {
                item.qualifiedName = item.Name;
            });
            var request = {
                XRefFiles: xrefFiles,
                Action: actionType,
                Cluster:cluster,
                Type: type
            };
            ESPRequest.flattenArray(request, "XRefFiles", "qualifiedName");
            return ESPRequest.send("WsDFUXRef", "DFUXRefArrayAction", {
                request: request
            }).then(function (response) {
                if (lang.exists("DFUXRefArrayActionResponse.DFUXRefArrayActionResult", response)) {
                    if (response.DFUXRefArrayActionResponse.DFUXRefArrayActionResult.Value) {
                        dojo.publish("hpcc/brToaster", {
                            Severity: "Message",
                            Source: "WsDfu.DFUXRefArrayAction",
                            Exceptions: [{Message: response.DFUXRefArrayActionResponse.DFUXRefArrayActionResult.Value}]
                        });
                    }
                }
                return response;
            });
        }
    };
});