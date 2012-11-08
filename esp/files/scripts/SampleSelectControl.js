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
    "dojo/_base/xhr",
    "dojo/data/ItemFileReadStore",
    "dijit/form/Select"
], function (declare, xhr, ItemFileReadStore, Select) {
    return declare(null, {
        id: null,
        samplesURL: null,
        selectControl: null,

        onNewSelection: function (eclText) {
        },

        constructor: function (args) {
            declare.safeMixin(this, args);
            var sampleStore = new dojo.data.ItemFileReadStore({
                url: this.samplesURL
            });

            var context = this;
            this.selectControl = new dijit.form.Select({
                name: this.id,
                store: sampleStore,
                value: "default.ecl",
                maxHeight: 480,
                style: {
                    padding: 0
                },
                onChange: function () {
                    var filename = dijit.byId(this.id).get("value");
                    xhr.get({
                        url: "ecl/" + filename,
                        handleAs: "text",
                        load: function (eclText) {
                            context.onNewSelection(eclText);
                        },
                        error: function () {
                        }
                    });
                }
            }, this.id);
            try {
                this.selectControl.startup();
            } catch (e) {
            }
        }

    });
});
