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

    "dojo/text!./templates/TargetSelectWidget.html"
], function (declare, lang, arrayUtil, dom,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, Select, registry,
    WsTopology,
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

            if (params.Target) {
                this._value = params.Target;
            }
            if (params.includeBlank) {
                this.includeBlank = params.includeBlank;
            }
            if (params.Groups === true) {
                this.loadGroups();
            } else if (params.DropZones === true) {
                this.loadDropZones();
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

        loadDropZones: function () {
            var context = this;
            WsTopology.TpServiceQuery({
                load: function (response) {
                    if (lang.exists("TpServiceQueryResponse.ServiceList.TpDropZones.TpDropZone", response)) {
                        var targetData = response.TpServiceQueryResponse.ServiceList.TpDropZones.TpDropZone;
                        context.targetSelectControl.options = [];
                        if (context.includeBlank) {
                            context.targetSelectControl.options.push({
                                label: "",
                                value: ""
                            });
                        }
                        for (var i = 0; i < targetData.length; ++i) {
                            context.targetSelectControl.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name,
                                machine: targetData[i].TpMachines.TpMachine[0]
                            });
                        }

                        if (context._value == "") {
                            context._value = context.targetSelectControl.options[0].value;
                        }
                        context.targetSelectControl.set("value", context._value);
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
                        context.targetSelectControl.options = [];
                        if (context.includeBlank) {
                            context.targetSelectControl.options.push({
                                label: "",
                                value: ""
                            });
                        }
                        for (var i = 0; i < targetData.length; ++i) {
                            context.targetSelectControl.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name
                            });
                        }

                        if (context._value == "") {
                            context._value = context.targetSelectControl.options[0].value;
                        }
                        context.targetSelectControl.set("value", context._value);
                    }
                }
            });
        },

        loadTargets: function () {
            var context = this;
            WsTopology.TpTargetClusterQuery({
                load: function (response) {
                    if (lang.exists("TpTargetClusterQueryResponse.TpTargetClusters.TpTargetCluster", response)) {
                        var targetData = response.TpTargetClusterQueryResponse.TpTargetClusters.TpTargetCluster;
                        context.targetSelectControl.options = [];
                        if (context.includeBlank) {
                            context.targetSelectControl.options.push({
                                label: "",
                                value: ""
                            });
                        }
                        var has_hthor = false;
                        for (var i = 0; i < targetData.length; ++i) {
                            context.targetSelectControl.options.push({
                                label: targetData[i].Name,
                                value: targetData[i].Name
                            });
                            if (targetData[i].Name == "hthor") {
                                has_hthor = true;
                            }
                        }

                        if (!context.includeBlank && context._value == "") {
                            if (has_hthor) {
                                context.setValue("hthor");
                            } else {
                                context._value = context.targetSelectControl.options[0].value;
                            }
                        } else {
                            context.targetSelectControl.set("value", context._value);
                        }
                    }
                }
            });
        }
    });
});
