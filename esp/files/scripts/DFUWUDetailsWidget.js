/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "exports",
    "dojo/_base/declare",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/dom-style",
    "dojo/query",

    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Textarea",
    "dijit/TitlePane",
    "dijit/registry",
    "dijit/ProgressBar",

    "hpcc/_TabContainerWidget",
    "hpcc/FileSpray",
    "hpcc/ESPDFUWorkunit",
    "hpcc/ECLSourceWidget",
    "hpcc/LFDetailsWidget",

    "dojo/text!../templates/DFUWUDetailsWidget.html",

    "dojox/layout/TableContainer"

], function (exports, declare, arrayUtil, dom, domAttr, domClass, domStyle, query,
                _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, TabContainer, ContentPane, Toolbar, Textarea, TitlePane, registry, ProgressBar,
                _TabContainerWidget, FileSpray, ESPDFUWorkunit, ECLSourceWidget, LFDetailsWidget,
                template) {
    exports.fixCircularDependency = declare("DFUWUDetailsWidget", [_TabContainerWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "DFUWUDetailsWidget",
        summaryWidget: null,
        xmlWidget: null,

        wu: null,
        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.xmlWidget = registry.byId(this.id + "_XML");
            var stateOptions = [];
            for (var key in FileSpray.States) {
                stateOptions.push({
                    label: FileSpray.States[key],
                    value: FileSpray.States[key]
                });
            }
            var stateSelect = registry.byId(this.id + "StateMessage");
            stateSelect.addOption(stateOptions);
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.wu.refresh(true);
        },
        _onSave: function (event) {
            var protectedCheckbox = registry.byId(this.id + "isProtected");
            var context = this;
            this.wu.update({
                JobName: dom.byId(context.id + "JobName").value,
                isProtected: protectedCheckbox.get("value")
            }, null);
        },
        _onDelete: function (event) {
            this.wu.doDelete();
        },
        _onAbort: function (event) {
            this.wu.abort();
        },
        _onResubmit: function (event) {
            var context = this;
            this.wu.resubmit();
        },
        _onModify: function (event) {
            //TODO
        },

        //  Implementation  ---
        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;

            //dom.byId("showWuid").innerHTML = params.Wuid;
            if (params.Wuid) {
                this.summaryWidget.set("title", params.Wuid);

                dom.byId(this.id + "Wuid").innerHTML = params.Wuid;

                this.clearInput();
                this.wu = ESPDFUWorkunit.Get(params.Wuid);
                var data = this.wu.getData();
                for (key in data) {
                    this.updateInput(key, null, data[key]);
                }
                var context = this;
                this.wu.watch(function (name, oldValue, newValue) {
                    context.updateInput(name, oldValue, newValue);
                });
                this.wu.refresh();
            }
            this.selectChild(this.summaryWidget, true);
        },

        initTab: function () {
            if (!this.wu) {
                return
            }

            var currSel = this.getSelectedChild();
            if (!currSel.initalized) {
                if (currSel.id == this.summaryWidget.id) {
                } else if (currSel.id == this.xmlWidget.id) {
                    var context = this;
                    this.wu.fetchXML(function (response) {
                        context.xmlWidget.init({
                            ECL: response
                        });
                    });
                } else {
                    currSel.init(currSel._hpccParams);
                }
            }
        },

        objectToText: function (obj) {
            var text = ""
            for (var key in obj) {
                text += "<tr><td>" + key + ":</td>";
                if (typeof obj[key] == "object") {
                    text += "[<br/>";
                    for (var i = 0; i < obj[key].length; ++i) {
                        text += this.objectToText(obj[key][i]);
                    }
                    text += "<br/>]<br/>";
                } else {
                    text += "<td>" + obj[key] + "</td></tr>";

                }
            }
            return text;
        },

        resetPage: function () {
        },

        getAncestor: function (node, type) {
            if (node) {
                if (node.tagName === type) {
                    return node;
                }
                return this.getAncestor(node.parentNode, type);
            }
            return null;
        },

        setInnerHTML: function (id, value) {
            var domNode = dom.byId(this.id + id);
            var pNode = this.getAncestor(domNode, "LI");
            if (typeof value != 'undefined') {
                if (pNode) {
                    domClass.remove(pNode, "hidden");
                }
                domNode.innerHTML = value;
            } else {
                if (pNode) {
                    domClass.add(pNode, "hidden");
                }
            }
        },

        setValue: function (id, value) {
            var domNode = dom.byId(this.id + id);
            var pNode = this.getAncestor(domNode, "LI");
            if (typeof value != 'undefined') {
                if (pNode) {
                    domClass.remove(pNode, "hidden");
                }
                var registryNode = registry.byId(this.id + id);
                if (registryNode) {
                    registryNode.set("value", value);
                } else {
                    domNode.value = value;
                }
            } else {
                if (pNode) {
                    domClass.add(pNode, "hidden");
                }
            }
        },

        clearInput: function () {
            var list = query("div#" + this.id + "_Summary form > ul > li");
            arrayUtil.forEach(list, function (item, idx) {
                domClass.add(item, "hidden");
            });
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                this.setValue(name, newValue);
            } else {
                var domNode = dom.byId(this.id + name);
                if (domNode) {
                    switch (domNode.tagName) {
                        case "SPAN":
                        case "DIV":
                            this.setInnerHTML(name, newValue);
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            this.setValue(name, newValue);
                            break;
                        default:
                            alert(domNode.tagName + ":" + name);
                    }
                }
            }
            switch (name) {
                case "CommandMessage":
                    this.setInnerHTML("CommandMessage2", newValue);
                    break;
                case "isProtected":
                    dom.byId(this.id + "ProtectedImage").src = this.wu.getProtectedImage();
                    break;
                case "State":
                case "hasCompleted":
                    this.refreshActionState();
                    break;
                case "SourceLogicalName":
                    this.ensurePane(this.id + "_SourceLogicalName", "Source", {
                        Name: newValue
                    });
                    break;
                case "DestLogicalName":
                    this.ensurePane(this.id + "_DestLogicalName", "Target", {
                        Name: newValue
                    });
                    break;
            }
        },

        refreshActionState: function () {
            registry.byId(this.id + "Save").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Delete").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Abort").set("disabled", this.wu.isComplete());
            registry.byId(this.id + "Resubmit").set("disabled", !this.wu.isComplete());
            registry.byId(this.id + "Modify").set("disabled", true);  //TODO
            registry.byId(this.id + "JobName").set("readOnly", !this.wu.isComplete());
            registry.byId(this.id + "isProtected").set("readOnly", !this.wu.isComplete());

            this.summaryWidget.set("iconClass", this.wu.getStateIconClass());
            dom.byId(this.id + "StateIdImage").src = this.wu.getStateImage();
        },

        checkIfComplete: function() {
        },

        monitorWorkunit: function (response) {
        },

        ensurePane: function (id, title, params) {
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new LFDetailsWidget.fixCircularDependency({
                    id: id,
                    title: title,
                    closable: false,
                    _hpccParams: params
                });
                this.addChild(retVal);
            }
            return retVal;
        }
    });
});