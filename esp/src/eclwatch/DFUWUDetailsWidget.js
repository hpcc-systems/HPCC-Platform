define([
    "exports",
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-class",
    "dojo/query",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "src/Clippy",
    "src/FileSpray",
    "src/ESPDFUWorkunit",
    "hpcc/DelayLoadWidget",

    "dojo/text!../templates/DFUWUDetailsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ProgressBar",
    "dijit/TitlePane",
    "dijit/form/Textarea",
    "dijit/form/Select",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/CheckBox",
    "dijit/ToolbarSeparator"

], function (exports, declare, nlsHPCCMod, arrayUtil, dom, domClass, query,
    registry,
    _TabContainerWidget, Clippy, FileSpray, ESPDFUWorkunit, DelayLoadWidget,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    exports.fixCircularDependency = declare("DFUWUDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "DFUWUDetailsWidget",
        i18n: nlsHPCC,

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

            Clippy.attach(this.id + "ClippyButton");
        },

        getTitle: function () {
            return this.i18n.title_DFUWUDetails;
        },

        //  Hitched actions  ---
        _onAutoRefresh: function (event) {
            this.wu.disableMonitor(!this.widget.AutoRefresh.get("checked"));
        },
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
            if (confirm(this.i18n.YouAreAboutToDeleteThisWorkunit)) {
                this.wu.doDelete();
            }
        },
        _onAbort: function (event) {
            this.wu.abort();
        },
        _onResubmit: function (event) {
            //TODO once HPCC-15504
        },
        _onModify: function (event) {
            //TODO once HPCC-15504
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            //dom.byId("showWuid").innerHTML = params.Wuid;
            if (params.Wuid) {
                this.summaryWidget.set("title", params.Wuid);

                dom.byId(this.id + "Wuid").textContent = params.Wuid;

                this.clearInput();
                this.wu = ESPDFUWorkunit.Get(params.Wuid);
                var data = this.wu.getData();
                for (var key in data) {
                    this.updateInput(key, null, data[key]);
                }
                var context = this;
                this.wu.watch(function (name, oldValue, newValue) {
                    context.updateInput(name, oldValue, newValue);
                });
                this.wu.refresh();
            }
        },

        initTab: function () {
            if (!this.wu) {
                return;
            }

            var currSel = this.getSelectedChild();
            if (!currSel.initalized) {
                if (currSel.id === this.summaryWidget.id) {
                } else if (currSel.id === this.xmlWidget.id) {
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
            var text = "";
            for (var key in obj) {
                text += "<tr><td>" + key + ":</td>";
                if (typeof obj[key] === "object") {
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

        setTextContent: function (id, value) {
            var domNode = dom.byId(this.id + id);
            var pNode = this.getAncestor(domNode, "LI");
            if (typeof value !== "undefined") {
                if (pNode) {
                    domClass.remove(pNode, "hidden");
                }
                domNode.textContent = value;
            } else {
                if (pNode) {
                    domClass.add(pNode, "hidden");
                }
            }
        },

        setValue: function (id, value) {
            var domNode = dom.byId(this.id + id);
            var pNode = this.getAncestor(domNode, "LI");
            if (typeof value !== "undefined") {
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
                            this.setTextContent(name, newValue);
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
                    this.setTextContent("CommandMessage2", newValue);
                    break;
                case "isProtected":
                    dom.byId(this.id + "ProtectedImage").src = this.wu.getProtectedImage();
                    break;
                case "State":
                case "hasCompleted":
                    this.refreshActionState();
                    break;
                case "__hpcc_changedCount":
                    if (this.wu.SourceLogicalName) {
                        this.ensurePane("SourceLogicalName", this.i18n.Source, {
                            NodeGroup: this.wu.SourceGroupName,
                            Name: this.wu.SourceLogicalName
                        });
                    }
                    if (this.wu.DestLogicalName) {
                        this.ensurePane("DestLogicalName", this.i18n.Target, {
                            NodeGroup: this.wu.DestGroupName,
                            Name: this.wu.DestLogicalName
                        });
                    }
                    if (this.wu.SourceFormatMessage === "csv") {
                        dom.byId(this.id + "SourceType").innerText = "(" + nlsHPCC.CSV + ")";
                    } else if (this.wu.SourceFormatMessage === "fixed") {
                        dom.byId(this.id + "SourceType").innerText = "(" + nlsHPCC.Fixed + ")";
                    } else if (!!this.wu.RowTag) {
                        dom.byId(this.id + "SourceType").innerText = "(" + nlsHPCC.XML + "/" + nlsHPCC.JSON + ")";
                    }
                    if (this.wu.DestFormatMessage === "csv") {
                        dom.byId(this.id + "TargetType").innerText = "(" + nlsHPCC.CSV + ")";
                    } else if (this.wu.DestFormatMessage === "fixed") {
                        dom.byId(this.id + "TargetType").innerText = "(" + nlsHPCC.Fixed + ")";
                    }
                    break;
            }
        },

        refreshActionState: function () {
            this.setDisabled(this.id + "AutoRefresh", this.wu.isComplete(), "iconAutoRefresh", "iconAutoRefreshDisabled");
            registry.byId(this.id + "Save").set("disabled", false);
            registry.byId(this.id + "Delete").set("disabled", !this.wu.isComplete() || this.wu.isDeleted());
            registry.byId(this.id + "Abort").set("disabled", this.wu.isComplete() || this.wu.isDeleted());
            //registry.byId(this.id + "Resubmit").set("disabled", !this.wu.isComplete() || this.wu.isDeleted()); //TODO
            //registry.byId(this.id + "Modify").set("disabled", true);  //TODO
            registry.byId(this.id + "JobName").set("readOnly", false);
            registry.byId(this.id + "isProtected").set("readOnly", !this.wu.isComplete() || this.wu.isDeleted());

            this.summaryWidget.set("iconClass", this.wu.getStateIconClass());
            dom.byId(this.id + "StateIdImage").src = this.wu.getStateImage();
        },

        checkIfComplete: function () {
        },

        monitorWorkunit: function (response) {
        },

        ensurePane: function (id, title, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                retVal = new DelayLoadWidget({
                    id: id,
                    title: title,
                    closable: false,
                    delayWidget: "LFDetailsWidget",
                    _hpccParams: params
                });
                this.addChild(retVal);
            }
            return retVal;
        }
    });
});
