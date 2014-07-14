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

    "hpcc/WsTopology",
    "hpcc/ESPUtil"
], function (declare, lang,
    WsTopology, ESPUtil) {

    var ThorCache = {
    };
    var Thor = declare([ESPUtil.Singleton], {
        constructor: function (args) {
            if (args) {
                declare.safeMixin(this, args);
            }
        },

        refresh: function () {
            var context = this;
            return WsTopology.TpThorStatus({
                request: {
                    Name: this.Name
                }
            }).then(function (response) {
                if (lang.exists("TpThorStatusResponse", response)) {
                    context.updateData(response.TpThorStatusResponse);
                    if (response.TpThorStatusResponse.Graph && response.TpThorStatusResponse.SubGraph) {
                        context.updateData({
                            GraphSummary:  response.TpThorStatusResponse.Graph + "-" + response.TpThorStatusResponse.SubGraph
                        });
                    }
                }
                return response;
            })
        }
    });


    return {
        GetThor: function (thorName) {
            if (!ThorCache[thorName]) {
                ThorCache[thorName] = new Thor({
                    Name: thorName
                });
            }
            return ThorCache[thorName];
        }

    };
});
