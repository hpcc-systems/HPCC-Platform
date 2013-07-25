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
require([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",
    "dojo/dom",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/form/Select",
    "dijit/registry",

    "hpcc/WsTopology",
    "hpcc/WsWorkunits",
    "hpcc/FileSpray",

    "dojo/text!./templates/TargetSelectWidget.html"
], function (declare, lang, arrayUtil, dom,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, Select, registry,
    WsTopology, WsWorkunits, FileSpray,
    template) {
    return declare("TargetSelectWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "TargetSelectWidget",

        targetSelectControl: null,
        name: "",
        _value: "",

        postCreate: function (args) {
            this.inherited(arguments);
            this._initControls();
        },

        resize: function (args) {
            this.inherited(arguments);
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        _initControls: function () {
            var context = this;
            this.targetSelectControl = registry.byId(this.id + "TargetSelect");
            this.targetSelectControl.onChange = function () {
                context.onChange(this.get("value"));
            };
        },

        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.targetSelectControl.options = [];

            if (params.Target) {
                this._value = params.Target;
            }
            if (params.includeBlank) {
                this.includeBlank = params.includeBlank;
                this.targetSelectControl.options.push({
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
            } else {
                this.loadTargets();
            }
            if (params.callback) {
                this.callback = params.callback;
            }
            if (params.includeBlank) {
            }
        },

        onChange: function (target) {
            this._value = target;
            this._valueItem = null;
            var context = this;
            var idx = arrayUtil.forEach(this.targetSelectControl.options, function(item, idx) {
                if (item.value === context._value) {
                    context._valueItem = item;
                }
            });
            if (this.callback) {
                this.callback(this._value, this._valueItem);
            }
        },

        setValue: function (target) {
            if (target !== null && this._value != target) {
                this._value = target;
                this.targetSelectControl.set("value", target);
            }
        },

        _setValueAttr: function (target) {
            if (target === null) {
                target = "";
            }
            if (target !== null && this._value != target) {
                this._value = target;
                this.targetSelectControl.set("value", target);
            }
        },

        getValue: function () {
            return this._value;
        },

        _getValueAttr: function () {
            return this._value;
        },

        resetDefaultSelection: function () {
            if (this._value == "") {
                this._value = this.targetSelectControl.options[0].value;
            }
            this.targetSelectControl.set("value", this._value);
        },

        loadDropZones: function () {
            var context = this;
            WsTopology.TpServiceQuery({
                load: function (response) {
                    if (lang.exists("TpServiceQueryResponse.ServiceList.TpDropZones.TpDropZone", response)) {
                        var targetData = response.TpServiceQueryResponse.ServiceList.TpDropZones.TpDropZone;
                        for (var i = 0; i < targetData.length; ++i) {
                            context.targetSelectControl.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name,
                                machine: targetData[i].TpMachines.TpMachine[0]
                            });
                        }
                        context.resetDefaultSelection();
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
                            context.targetSelectControl.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name
                            });
                        }
                        context.resetDefaultSelection();
                    }
                }
            });
        },

        loadWUState: function() {
            for (var key in WsWorkunits.States) {
                this.targetSelectControl.options.push({
                    label: WsWorkunits.States[key],
                    value: WsWorkunits.States[key]
                });
            }
            this.resetDefaultSelection();
        },

        loadDFUState: function () {
            for (var key in FileSpray.States) {
                this.targetSelectControl.options.push({
                    label: FileSpray.States[key],
                    value: FileSpray.States[key]
                });
            }
            this.resetDefaultSelection();
        },

        LogicalFileSearchType: function() {
            this.targetSelectControl.options.push({
                label: "Created",
                value: "Created"
            });
            this.targetSelectControl.options.push({
                label: "Used",
                value: "Referenced"
            });
            this.resetDefaultSelection();
        },

        loadTargets: function () {
            var context = this;
            WsTopology.TpLogicalClusterQuery({
            }).then(function (response) {
                if (lang.exists("TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster", response)) {
                    var targetData = response.TpLogicalClusterQueryResponse.TpLogicalClusters.TpLogicalCluster;
                    for (var i = 0; i < targetData.length; ++i) {
                        context.targetSelectControl.options.push({
                            label: targetData[i].Name,
                            value: targetData[i].Name
                        });
                    }

                    if (!context.includeBlank && context._value == "") {
                        if (response.TpLogicalClusterQueryResponse.default) {
                            context._value = response.TpLogicalClusterQueryResponse.default.Name;
                        } else {
                            context._value = context.targetSelectControl.options[0].value;
                        }
                    }
                    context.resetDefaultSelection();
                }
            });
        }
    });
});
