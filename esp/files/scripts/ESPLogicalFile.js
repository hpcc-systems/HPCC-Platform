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

    "hpcc/WsDfu",
    "hpcc/ESPResult"
], function (declare, lang,
        WsDfu, ESPResult) {
    return declare(null, {
        cluster: "",
        logicalName: "",
        result: [],

        constructor: function (args) {
            declare.safeMixin(this, args);
            this.DFUInfoResponse = {
                Name: this.logicalName,
                Cluster: this.cluster
            };
        },
        save: function (description, args) {
            //WsDfu/DFUInfo?FileName=progguide%3A%3Aexampledata%3A%3Akeys%3A%3Apeople.lastname.firstname&UpdateDescription=true&FileDesc=%C2%A0123&Save+Description=Save+Description
            var context = this;
            WsDfu.DFUInfo({
                request: {
                    FileName: this.logicalName,
                    Cluster: this.cluster,
                    UpdateDescription: true,
                    FileDesc: description
                },
                load: function (response) {
                    if (response.DFUInfoResponse) {
                        context.processDFUInfoResponse(response.DFUInfoResponse, args);
                    }
                },
                error: function (e) {
                }
            });
        },
        getInfo: function (args) {
            //WsDfu/DFUInfo?Name=progguide::exampledata::keys::people.state.city.zip.lastname.firstname.payload&Cluster=hthor__myeclagent HTTP/1.1
            var context = this;
            WsDfu.DFUInfo({
                request:{
                    Name: this.logicalName,
                    Cluster: this.cluster
                },
                load: function (response) {
                    if (response.DFUInfoResponse) {
                        context.processDFUInfoResponse(response.DFUInfoResponse, args);
                    }
                }
            });
        },
        processDFUInfoResponse: function(dfuInfoResponse, args) {
            var fileDetail = dfuInfoResponse.FileDetail;
            this.DFUInfoResponse = fileDetail;
            this.result = new ESPResult(fileDetail);

            if (args.onGetAll) {
                args.onGetAll(fileDetail);
            }
        },
        fetchStructure: function (format, onFetchStructure) {
            var context = this;
            WsDfu.DFUDefFile({
                request: {
                    Name: this.logicalName,
                    Format: format
                },
                load: function (response) {
                    onFetchStructure(response);
                }
            });
        },
        fetchDEF: function (onFetchXML) {
            this.fetchStructure("def", onFetchXML);
        },
        fetchXML: function (onFetchXML) {
            this.fetchStructure("xml", onFetchXML);
        }
    });
});
