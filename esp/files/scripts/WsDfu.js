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
    "dojo/store/util/QueryResults",

    "hpcc/ESPRequest"
], function (declare, lang, Deferred, arrayUtil, QueryResults,
    ESPRequest) {

    return {
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
                        dojo.publish("hpcc/brToaster", {
                            message: response.DFUArrayActionResponse.DFUArrayActionResult,
                            type: "error",
                            duration: -1
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
                    if (lang.exists("SuperfileActionResponse", response)) {
                        dojo.publish("hpcc/brToaster", {
                            message: response.AddtoSuperfileResponse.Subfiles,
                            type: "error",
                            duration: -1
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
                        dojo.publish("hpcc/brToaster", {
                            message: response.AddtoSuperfileResponse.Subfiles,
                            type: "error",
                            duration: -1
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

        DFUInfo: function (params) {
            return ESPRequest.send("WsDfu", "DFUInfo", params);
        },

        DFUDefFile: function (params) {
            lang.mixin(params, {
                handleAs: "text"
            });
            return ESPRequest.send("WsDfu", "DFUDefFile", params);
        }
    };
});

