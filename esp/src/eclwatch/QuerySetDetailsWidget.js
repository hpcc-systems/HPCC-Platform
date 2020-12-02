define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/promise/all",
    "dojo/_base/array",

    "dijit/registry",

    "src/Clippy",
    "src/ESPQuery",
    "hpcc/_TabContainerWidget",
    "src/Utility",

    "dojo/text!../templates/QuerySetDetailsWidget.html",

    "hpcc/DelayLoadWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/CheckBox",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane"

], function (declare, lang, nlsHPCCMod, dom, domAttr, all, arrayUtil,
    registry,
    Clippy, ESPQuery, _TabContainerWidget, Utility,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("QuerySetDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "QuerySetDetailsWidget",
        i18n: nlsHPCC,

        query: null,

        initalized: false,
        summaryTab: null,
        summaryTabLoaded: false,
        errorsTab: null,
        errorsTabLoaded: false,
        graphsTab: null,
        graphsTabLoaded: false,
        logicalFilesTab: null,
        logicalFilesTabLoaded: false,
        superFilesTab: null,
        superFilesTabLoaded: false,
        workunitsTab: null,
        workunitsTabLoaded: false,
        testPagesTab: null,
        testPagesLoaded: false,
        loaded: false,

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryTab = registry.byId(this.id + "_Summary");
            this.errorsTab = registry.byId(this.id + "_Errors");
            this.graphsTab = registry.byId(this.id + "_Graphs");
            this.logicalFilesTab = registry.byId(this.id + "_QuerySetLogicalFiles");
            this.superFilesTab = registry.byId(this.id + "_QuerySetSuperFiles");
            this.librariesUsedTab = registry.byId(this.id + "_LibrariesUsed");
            this.workunitsTab = registry.byId(this.id + "_Workunit");
            this.testPagesTab = registry.byId(this.id + "_TestPages");
            this.suspended = registry.byId(this.id + "Suspended");

            Clippy.attach(this.id + "ClippyButton");
        },

        //  Hitched actions  ---
        _onSave: function (event) {
            var suspended = registry.byId(this.id + "Suspended").get("value");
            var activated = registry.byId(this.id + "Activated").get("value");
            var context = this;
            all({
                suspend: this.query.setSuspended(suspended),
                activate: this.query.setActivated(activated)
            });
        },
        _onReset: function () {
            if (confirm(this.i18n.ResetThisQuery)) {
                this.query.doReset();
            }
        },
        _onDelete: function (event) {
            if (confirm(this.i18n.YouAreAboutToDeleteThisQueryset)) {
                this.query.doDelete();
            }
        },
        _onRefresh: function () {
            this.query.refresh();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.query = ESPQuery.Get(params.QuerySetId, params.Id);

            var context = this;
            var data = this.query.getData();
            for (var key in data) {
                this.updateInput(key, null, data[key]);
            }
            this.query.watch(function (name, oldValue, newValue) {
                context.updateInput(name, oldValue, newValue);
            });
            this.query.refresh();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel.id === this.summaryTab.id && !this.summaryTabLoaded) {
                this.summaryTabLoaded = true;
            } else if (currSel.id === this.workunitsTab.id && !this.workunitsTabLoaded) {
                this.workunitsTabLoaded = true;
                this.workunitsTab.init({
                    Wuid: this.query.Wuid
                });
            } else if (currSel.id === this.errorsTab.id && !this.errorsTabLoaded) {
                this.errorsTabLoaded = true;
                this.errorsTab.init({
                    QuerySetId: this.params.QuerySetId,
                    Id: this.params.Id
                });
            } else if (currSel.id === this.graphsTab.id && !this.graphsTabLoaded) {
                this.graphsTabLoaded = true;
                this.graphsTab.init({
                    QuerySetId: this.params.QuerySetId,
                    Id: this.params.Id
                });
            } else if (currSel.id === this.logicalFilesTab.id && !this.logicalFilesTabLoaded) {
                this.logicalFilesTabLoaded = true;
                this.logicalFilesTab.init({
                    QuerySetId: this.params.QuerySetId,
                    Id: this.params.Id
                });
            } else if (currSel.id === this.superFilesTab.id && !this.superFilesTabLoaded) {
                this.superFilesTabLoaded = true;
                this.superFilesTab.init({
                    QuerySetId: this.params.QuerySetId,
                    Id: this.params.Id
                });
            } else if (currSel.id === this.librariesUsedTab.id && !this.librariesUsedTabLoaded) {
                this.librariesUsedTabLoaded = true;
                this.librariesUsedTab.init({
                    QuerySetId: this.params.QuerySetId,
                    Id: this.params.Id
                });
            } else if (currSel.id === this.widget._Resources.id && !this.resourcesWidgetLoaded) {
                this.resourcesWidgetLoaded = true;
                this.widget._Resources.init({
                    Wuid: this.query.Wuid,
                    QuerySetId: this.params.QuerySetId,
                    Id: this.params.Id
                });
            } else if (currSel.id === this.testPagesTab.id && !this.testPagesTabLoaded) {
                this.testPagesTabLoaded = true;
                this.testPagesTab.init({
                    QuerySetId: this.params.QuerySetId,
                    Id: this.params.Id
                });
            }
        },

        updateInput: function (name, oldValue, newValue) {
            var context = this;
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            } else {
                var domElem = dom.byId(this.id + name);
                if (domElem) {
                    switch (domElem.tagName) {
                        case "SPAN":
                        case "DIV":
                            dom.byId(this.id + name).textContent = newValue;
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            alert(domElem.tagName);
                            break;
                    }
                }
            }
            if (name === "Wuid") {
                this.workunitsTab.set("title", newValue);
            } else if (name === "Suspended") {
                dom.byId(this.id + "SuspendImg").src = newValue ? Utility.getImageURL("suspended.png") : "";
            } else if (name === "Activated") {
                dom.byId(this.id + "ActiveImg").src = newValue ? Utility.getImageURL("active.png") : "";
            } else if (name === "SuspendedReason" && newValue === "cluster") {
                dom.byId(this.id + "SuspendCluster").src = Utility.getImageURL("error-icon.png");
            } else if (name === "CountGraphs" && newValue) {
                this.graphsTab.set("title", this.i18n.Graphs + " (" + newValue + ")");
            } else if (name === "graphs") {
                this.graphsTab.set("title", this.i18n.Graphs + " (" + newValue.length + ")");
                var tooltip = "";
                for (var i = 0; i < newValue.length; ++i) {
                    if (tooltip !== "")
                        tooltip += "\n";
                    tooltip += newValue[i].Name;
                    if (newValue[i].Time)
                        tooltip += " " + newValue[i].Time;
                }
                this.graphsTab.set("tooltip", tooltip);
            } else if (name === "ResourceURLCount" && newValue - 1) {
                this.widget._Resources.set("title", this.i18n.Resources + " (" + (newValue - 1) + ")");
            } else if (name === "SuperFiles") {
                if (lang.exists("SuperFile.length", newValue)) {
                    this.superFilesTab.set("title", this.i18n.SuperFiles + " (" + newValue.SuperFile.length + ")");
                    var superFileToolTip = "";
                    for (var i = 0; i < newValue.SuperFile.length; ++i) {
                        if (superFileToolTip !== "")
                            superFileToolTip += "\n";
                        superFileToolTip += newValue.SuperFile[i].Name;
                    }
                    this.superFilesTab.set("tooltip", superFileToolTip);
                }
                var count = 0;
                arrayUtil.forEach(context.query.SuperFiles.SuperFile, function (item, idx) {
                    arrayUtil.forEach(item.SubFiles.File, function (item, idx) {
                        count++;
                    });
                });
            } else if (name === "LogicalFiles") {
                if (lang.exists("Item.length", newValue)) {
                    this.logicalFilesTab.set("title", this.i18n.LogicalFiles + " (" + context.query.LogicalFiles.Item.length + ")");
                    var logicalFileToolTip = "";
                    for (var i = 0; i < newValue.Item.length; ++i) {
                        if (logicalFileToolTip !== "")
                            logicalFileToolTip += "\n";
                        logicalFileToolTip += newValue.Item[i];
                    }
                    this.logicalFilesTab.set("tooltip", logicalFileToolTip);
                }
            } else if (name === "LibrariesUsed") {
                if (lang.exists("Item.length", newValue)) {
                    this.librariesUsedTab.set("title", this.i18n.LibrariesUsed + " (" + newValue.Item.length + ")");
                    var tooltip = "";
                    for (var i = 0; i < newValue.Item.length; ++i) {
                        if (tooltip !== "")
                            tooltip += "\n";
                        tooltip += newValue.Item[i];
                    }
                    this.librariesUsedTab.set("tooltip", tooltip);
                }
            } else if (name === "Clusters") {
                if (lang.exists("ClusterQueryState.length", newValue)) {
                    var checkIfSuspended = false;
                    if (newValue.ClusterQueryState[0].MixedNodeStates === true) {
                        dom.byId(this.id + "SuspendCluster").src = Utility.getImageURL("mixwarn.png");
                        checkIfSuspended = true;
                    } else if (newValue.ClusterQueryState[0].State === "Suspended") {
                        dom.byId(this.id + "SuspendCluster").src = Utility.getImageURL("errwarn.png");
                        checkIfSuspended = true;
                    }
                    this.suspended.set("checked", checkIfSuspended);
                    this.suspended.set("readOnly", checkIfSuspended);
                    this.errorsTab.set("title", this.i18n.ErrorsStatus + " (" + newValue.ClusterQueryState.length + ")");
                    var tooltip = "";
                    for (var i = 0; i < newValue.ClusterQueryState.length; ++i) {
                        if (tooltip !== "")
                            tooltip += "\n";
                        tooltip += newValue.ClusterQueryState[i].Cluster + " (" + newValue.ClusterQueryState[i].State + ")";
                    }
                    this.errorsTab.set("tooltip", tooltip);
                }
            }
        }
    });
});
