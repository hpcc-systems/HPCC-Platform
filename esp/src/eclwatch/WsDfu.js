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
    "dojo/_base/Deferred",
    "dojo/_base/array",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/store/util/QueryResults",
    "dojo/topic",

    "dojox/xml/parser",

    "hpcc/ESPBase",
    "hpcc/ESPRequest"
], function (declare, lang, Deferred, arrayUtil, Memory, Observable, QueryResults, topic,
    parser,
    ESPBase, ESPRequest) {

    var DiskUsageStore = declare([Memory], {

        constructor: function () {
            this.idProperty = "__hpcc_id";
        },

        query: function (query, options) {
            switch (query.CountBy) {
                case "Year":
                case "Quarter":
                case "Month":
                case "Day":
                    query.Interval = query.CountBy;
                    query.CountBy = "Date";
                    break;
            }
            var results = self.DFUSpace({
                request: query
            }).then(lang.hitch(this, function (response) {
                var data = [];
                if (lang.exists("DFUSpaceResponse.DFUSpaceItems.DFUSpaceItem", response)) {
                    arrayUtil.forEach(response.DFUSpaceResponse.DFUSpaceItems.DFUSpaceItem, function (item, idx) {
                        data.push(lang.mixin(item, {
                            __hpcc_id: item.Name
                        }));
                    }, this);
                }
                if (options.sort && options.sort.length) {
                    data.sort(function (_l, _r) {
                        var l = _l[options.sort[0].attribute];
                        var r = _r[options.sort[0].attribute];
                        if (l === r) {
                            return 0;
                        }
                        switch (options.sort[0].attribute) {
                            case "TotalSize":
                            case "LargestSize":
                            case "SmallestSize":
                            case "NumOfFiles":
                            case "NumOfFilesUnknown":
                                l = parseInt(l.split(",").join(""));
                                r = parseInt(r.split(",").join(""));
                        }
                        if (options.sort[0].descending) {
                            return r < l ? -1 : 1;
                        }
                        return l < r ? -1 : 1;
                    })
                }
                this.setData(data);
                return this.data;
            }));
            return QueryResults(results);
        }
    });

    var self = {
        CreateDiskUsageStore: function() {
            var store = new DiskUsageStore();
            return Observable(store);
        },

        DFUArrayAction: function (logicalFiles, actionType) {
            arrayUtil.forEach(logicalFiles, function (item, idx) {
                item.qualifiedName = item.Name + "@" + item.NodeGroup;
            });
            var request = {
                LogicalFiles: logicalFiles,
                Type: actionType
            };
            ESPRequest.flattenArray(request, "LogicalFiles", "qualifiedName");

            return ESPRequest.send("WsDfu", "DFUArrayAction", {
                request: request
            }).then(function (response) {
                if (lang.exists("DFUArrayActionResponse.ActionResults.DFUActionInfo", response)) {
                    var exceptions = [];
                    arrayUtil.forEach(response.DFUArrayActionResponse.ActionResults.DFUActionInfo, function (item, idx) {
                        if (item.Failed) {
                            exceptions.push({
                                Source: item.FileName,
                                Message: item.ActionResult
                            });
                        }
                    });
                    if (exceptions.length) {
                        topic.publish("hpcc/brToaster", {
                            Severity: "Error",
                            Source: "WsDfu.DFUArrayAction",
                            Exceptions: exceptions
                        });
                    }
                }
                return response;
            });
        },

        SuperfileAction: function (action, superfile, subfiles, removeSuperfile) {
            var request = {
                action: action,
                superfile: superfile,
                subfiles: subfiles,
                removeSuperfile: removeSuperfile
            };
            ESPRequest.flattenArray(request, "subfiles", "Name");

            return ESPRequest.send("WsDfu", "SuperfileAction", {
                request: request
            });
        },

        AddtoSuperfile: function (logicalFiles, superfile, existingFile) {
            var request = {
                names: logicalFiles,
                Superfile: superfile,
                ExistingFile: existingFile ? 1 : 0
            };
            ESPRequest.flattenArray(request, "names", "Name");

            return ESPRequest.send("WsDfu", "AddtoSuperfile", {
                request: request
            });
        },

        DFUQuery: function (params) {
            return ESPRequest.send("WsDfu", "DFUQuery", params);
        },

        DFUFileView: function (params) {
            return ESPRequest.send("WsDfu", "DFUFileView", params);
        },

        DFUSpace: function (params) {
            return ESPRequest.send("WsDfu", "DFUSpace", params);
        },

        DFUInfo: function (params) {
            return ESPRequest.send("WsDfu", "DFUInfo", params).then(function (response) {
                if (lang.exists("Exceptions.Exception", response)) {
                    arrayUtil.forEach(response.Exceptions.Exception, function (item, idx) {
                        if (item.Code === 20038) {
                            lang.mixin(response, {
                                DFUInfoResponse: {
                                    FileDetail: {
                                        Name: params.request.Name,
                                        StateID: 999,
                                        State: "deleted"
                                    }
                                }
                            });
                        }
                    });
                }
                return response;
            });
        },

        DFUDefFile: function (params) {
            lang.mixin(params, {
                handleAs: "text"
            });
            return ESPRequest.send("WsDfu", "DFUDefFile", params).then(function (response) {
                try {
                    var domXml = parser.parse(response);
                    var espBase = new ESPBase();
                    var exceptions = espBase.getValues(domXml, "Exception", ["Exception"]);
                    if (exceptions.length) {
                        response = "";
                        arrayUtil.forEach(exceptions, function (item, idx) {
                            response += item.Message + "\n";
                        });
                    }
                } catch (e) {
                    //  No errors  ---
                }
                return response;
            });
        }
    };

    return self;
});

