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
    "dojo/_base/xhr",
    "dojo/_base/Deferred",
    "dojo/store/util/QueryResults",
    "dojo/store/JsonRest", 
    "dojo/store/Memory", 
    "dojo/store/Cache", 
    "dojo/store/Observable",
    
    "dojox/xml/parser",    

    "hpcc/ESPBase"
], function (declare, lang, xhr, Deferred, QueryResults, JsonRest, Memory, Cache, Observable,
    parser,
    ESPBase) {
    var GetDFUWorkunits = declare(ESPBase, {
        idProperty: "ID",

        constructor: function (options) {
            declare.safeMixin(this, options);
        },

        getIdentity: function (object) {
            return object[this.idProperty];
        },

        query: function (query, options) {
            var request = {};
            lang.mixin(request, options.query);
            if (options.start)
                request['PageStartFrom'] = options.start;
            if (options.count)
                request['PageSize'] = options.count;
            if (options.sort) {
                request['Sortby'] = options.sort[0].attribute;
                request['Descending'] = options.sort[0].descending;
            }
            request['rawxml_'] = "1";

            var results = xhr.get({
                url: this.getBaseURL("FileSpray") + "/GetDFUWorkunits.json",
                handleAs: "json",
                content: request
            });

            var deferredResults = new Deferred();
            deferredResults.total = results.then(function (response) {
                if (lang.exists("GetDFUWorkunitsResponse.NumWUs", response)) {
                    return response.GetDFUWorkunitsResponse.NumWUs;
                }
                return 0;
            });
            Deferred.when(results, function (response) {
                var workunits = [];
                if (lang.exists("GetDFUWorkunitsResponse.results.DFUWorkunit", response)) {
                    workunits = response.GetDFUWorkunitsResponse.results.DFUWorkunit;
                }
                deferredResults.resolve(workunits);
            });

            return QueryResults(deferredResults);
        }
    });

    var DropZoneFiles = declare(ESPBase, {
        idProperty: "NetAddress",

        constructor: function (options) {
            declare.safeMixin(this, options);
        },

        getIdentity: function (object) {
            return object[this.idProperty];
        },

        query: function (query, options) {
            var request = {};
            lang.mixin(request, options.query);
            if (options.start) {
                //  Not supported  ---
            }
            if (options.count) {
                //  Not supported  ---
            }
            if (options.sort) {
                //  Not supported  ---
            }
            request['rawxml_'] = "1";

            var results = xhr.get({
                url: this.getBaseURL("FileSpray") + "/DropZoneFiles.json",
                handleAs: "json",
                content: request
            });

            var deferredResults = new Deferred();
            deferredResults.total = results.then(function (response) {
                if (lang.exists("DropZoneFilesResponse.Files.PhysicalFileStruct.length", response)) {
                    return response.DropZoneFilesResponse.Files.PhysicalFileStruct.length;
                }
                return 0;
            });
            Deferred.when(results, function (response) {
                var files = [];
                if (lang.exists("DropZoneFilesResponse.Files.PhysicalFileStruct", response)) {
                    files = response.DropZoneFilesResponse.Files.PhysicalFileStruct;
                }
                deferredResults.resolve(files);
            });

            return QueryResults(deferredResults);
        }
    });

    return {
        GetDFUWorkunits: GetDFUWorkunits,
        DropZoneFiles: DropZoneFiles,

        GetDropZones: function (callback) {
            var request = {
                DirectoryOnly: true
            };

            var espBase = new ESPBase();
            xhr.post({
                url: espBase.getBaseURL("FileSpray") + "/DropZoneFiles.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                    if (callback && callback.load) {
                        callback.load(response.DropZoneFilesResponse.DropZones.DropZone);
                    }
                },
                error: function (e) {
                    if (callback && callback.error) {
                        callback.error(e);
                    }
                }
            });
        },

        WUAction: function (items, actionType, callback) {
            var request = {
                Type: actionType
            };

            for (var i = 0; i < items.length; ++i) {
                request["wuids_i" + i] = items[i].ID;
            }

            var espBase = new ESPBase();
            xhr.post({
                url: espBase.getBaseURL("FileSpray") + "/DFUWorkunitsAction.json",
                handleAs: "json",
                content: request,
                load: function (response) {
                    if (callback && callback.load) {
                        callback.load(response);
                    }
                },
                error: function () {
                    if (callback && callback.error) {
                        callback.error(e);
                    }
                }
            });
        }
    };
});

