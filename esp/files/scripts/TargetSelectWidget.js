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
    "dojo/i18n",
    "dojo/i18n!./nls/common",
    "dojo/i18n!./nls/TargetSelectWidget",
    "dojo/_base/array",
    "dojo/_base/xhr",
    "dojo/data/ItemFileReadStore",
    "dojo/on",

    "dijit/form/Select",
    "dijit/registry",

    "hpcc/WsTopology",
    "hpcc/WsWorkunits",
    "hpcc/FileSpray"
], function (declare, lang, i18n, nlsCommon, nlsSpecific, arrayUtil, xhr, ItemFileReadStore, on,
    Select, registry,
    WsTopology, WsWorkunits, FileSpray) {
    return declare("TargetSelectWidget", [Select], {
        i18n: lang.mixin(nlsCommon, nlsSpecific),

        loading: false,
        defaultValue: "",

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.loading = true;
            this.options = [];

            if (params.Target) {
                this.defaultValue = params.Target;
                this.set("value", params.Target);
            }

            if (params.includeBlank) {
                this.includeBlank = params.includeBlank;
                this.options.push({
                    label: "&nbsp;",
                    value: ""
                });
            }
            if (params.Groups === true) {
                this.loadGroups();
            } else if (params.DropZones === true) {
                this.loadDropZones();
            } else if (params.WUState === true) {
                this.loadWUState();
            } else if (params.DFUState === true) {
                this.loadDFUState();
            } else if (params.ECLSamples === true) {
                this.loadECLSamples();
            } else {
                this.loadTargets();
            }
            if (params.callback) {
                this.callback = params.callback;
            }
            if (params.includeBlank) {
            }
        },

        _setValueAttr: function (target) {
            if (target === null)
                target = "";
            this.inherited(arguments);
            if (this.callback) {
                this.callback(this.value, this._getRowAttr());
            }
        },

        _getValueAttr: function () {
            if (this.loading)
                return this.defaultValue;

            return this.value;
        },

        _getRowAttr: function () {
            var context = this;
            var retVal = null;
            arrayUtil.forEach(this.options, function (item, idx) {
                if (context.value === item.value) {
                    retVal = item;
                    return false;
                }
            });
            return retVal;
        },

        _postLoad: function () {
            if (this.defaultValue == "") {
                this.defaultValue = this.options[0].value;
            }
            this.set("value", this.defaultValue);
            this.loading = false;
        },

        loadDropZones: function () {
            var context = this;
            WsTopology.TpServiceQuery({
                load: function (response) {
                    if (lang.exists("TpServiceQueryResponse.ServiceList.TpDropZones.TpDropZone", response)) {
                        var targetData = response.TpServiceQueryResponse.ServiceList.TpDropZones.TpDropZone;
                        for (var i = 0; i < targetData.length; ++i) {
                            context.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name,
                                machine: targetData[i].TpMachines.TpMachine[0]
                            });
                        }
                        context._postLoad();
                    }
                }
            });
        },

        loadGroups: function () {
            var context = this;
            WsTopology.TpGroupQuery({
                load: function (response) {
                    if (lang.exists("TpGroupQueryResponse.TpGroups.TpGroup", response)) {
                        var targetData = response.TpGroupQueryResponse.TpGroups.TpGroup;
                        for (var i = 0; i < targetData.length; ++i) {
                            context.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name
                            });
                        }
                        context._postLoad();
                    }
                }
            });
        },

        loadWUState: function() {
            for (var key in WsWorkunits.States) {
                this.options.push({
                    label: WsWorkunits.States[key],
                    value: WsWorkunits.States[key]
                });
            }
            this._postLoad();
        },

        loadDFUState: function () {
            for (var key in FileSpray.States) {
                this.options.push({
                    label: FileSpray.States[key],
                    value: FileSpray.States[key]
                });
            }
            this._postLoad();
        },

        LogicalFileSearchType: function() {
            this.options.push({
                label: "Created",
                value: "Created"
            });
            this.options.push({
                label: "Used",
                value: "Referenced"
            });
            this._postLoad();
        },

        loadTargets: function () {
            var context = this;
            WsTopology.TpLogicalClusterQuery({
            }).then(function (response) {
                if (lang.exists("TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster", response)) {
                    var targetData = response.TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster;
                    for (var i = 0; i < targetData.length; ++i) {
                        context.options.push({
                            label: targetData[i].Name,
                            value: targetData[i].Name
                        });
                    }

                    if (!context.includeBlank && context._value == "") {
                        if (response.TpLogicalClusterQueryResponse["default"]) {
                            context._value = response.TpLogicalClusterQueryResponse["default"].Name;
                        } else {
                            context._value = context.options[0].value;
                        }
                    }
                }
                context._postLoad();
            });
        },

        loadECLSamples: function () {
            var sampleStore = new ItemFileReadStore({
                url: "ecl/ECLPlaygroundSamples.json"
            });
            this.setStore(sampleStore);
            var context = this;
            this.on("change", function (evt) {
                var filename = this.get("value");
                xhr.get({
                    url: "ecl/" + filename,
                    handleAs: "text",
                    load: function (eclText) {
                        context.onNewSelection(eclText);
                    },
                    error: function () {
                    }
                });
            });
            context._postLoad();
        }
    });
});
