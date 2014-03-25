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

        DFUArrayAction: function (logicalFiles, actionType, callback) {
            arrayUtil.forEach(logicalFiles, function (item, idx) {
                item.qualifiedName = item.Name + "@" + item.ClusterName;
            });
            var request = {
                LogicalFiles: logicalFiles,
                Type: actionType
            };
            ESPRequest.flattenArray(request, "LogicalFiles", "qualifiedName");

            return ESPRequest.send("WsDfu", "DFUArrayAction", {
                request: request,
                load: function (response) {
                    if (lang.exists("DFUArrayActionResponse.DFUArrayActionResult", response)) {
                        topic.publish("hpcc/brToaster", {
                            Severity: "Error",
                            Source: "WsDfu.DFUArrayAction",
                            Exceptions: [{ Message: response.DFUArrayActionResponse.DFUArrayActionResult }]
                        });
                    }

                    if (callback && callback.load) {
                        callback.load(response);
                    }
                },
                error: function (err) {
                    if (callback && callback.error) {
                        callback.error(err);
                    }
                }
            });
        },

        SuperfileAction: function (action, superfile, subfiles, removeSuperfile, callback) {
            var request = {
                action: action,
                superfile: superfile,
                subfiles: subfiles,
                removeSuperfile: removeSuperfile
            };
            ESPRequest.flattenArray(request, "subfiles", "Name");

            return ESPRequest.send("WsDfu", "SuperfileAction", {
                request: request,
                load: function (response) {
                    if (lang.exists("SuperfileActionResponse", response) && response.SuperfileActionResponse.retcode) {
                        topic.publish("hpcc/brToaster", {
                            Severity: "Error",
                            Source: "WsDfu.SuperfileAction",
                            Exceptions: [{ Message: dojo.toJson(response.SuperfileActionResponse) }]
                        });
                    }

                    if (callback && callback.load) {
                        callback.load(response);
                    }
                },
                error: function (err) {
                    if (callback && callback.error) {
                        callback.error(err);
                    }
                }
            });
        },

        AddtoSuperfile: function (logicalFiles, superfile, existingFile, callback) {
            var request = {
                names: logicalFiles,
                Superfile: superfile,
                ExistingFile: existingFile ? 1 : 0
            };
            ESPRequest.flattenArray(request, "names", "Name");

            return ESPRequest.send("WsDfu", "AddtoSuperfile", {
                request: request,
                load: function (response) {
                    if (lang.exists("AddtoSuperfileResponse.Subfiles", response)) {
                        topic.publish("hpcc/brToaster", {
                            Severity: "Error",
                            Source: "WsDfu.AddtoSuperfile",
                            Exceptions: [{ Message: response.AddtoSuperfileResponse.Subfiles }]
                        });
                    }

                    if (callback && callback.load) {
                        callback.load(response);
                    }
                },
                error: function (err) {
                    if (callback && callback.error) {
                        callback.error(err);
                    }
                }
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
            return ESPRequest.send("WsDfu", "DFUInfo", params);
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

