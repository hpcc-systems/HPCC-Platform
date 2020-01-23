define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom-construct",

    "dijit/registry",

    "hpcc/_TabContainerWidget",

    "dojo/text!../templates/TopologyDetailsWidget.html",

    "hpcc/ECLSourceWidget",
    "hpcc/LogWidget",
    "hpcc/RequestInformationWidget",
    "hpcc/GetNumberOfFilesToCopyWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/ValidationTextBox",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/form/TextBox",
    "dijit/Dialog",
    "dijit/form/SimpleTextarea",

    "hpcc/TableContainer"
], function (declare, i18n, nlsHPCC, domConstruct,
    registry,
    _TabContainerWidget,
    template) {
    return declare("TopologyDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "TopologyDetailsWidget",
        i18n: nlsHPCC,

        summaryWidget: null,
        configurationWidget: null,
        configurationWidgetLoaded: false,
        logsWidget: null,
        logsWidgetLoaded: false,
        getNumberOfFilesToCopyWidget: null,
        getNumberOfFilesToCopyWidgetLoaded: false,

        postCreate: function (args) {
            this.inherited(arguments);
            this.details = registry.byId(this.id + "_Details");
            this.configurationWidget = registry.byId(this.id + "_Configuration");
            this.logsWidget = registry.byId(this.id + "_Logs");
            this.requestInformationWidget = registry.byId(this.id + "_RequestInformation");
            this.preflightWidget = registry.byId(this.id + "_Preflight");
            this.getNumberOfFilesToCopyWidget = registry.byId(this.id + "_GetNumberOfFilesToCopy");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.title_TopologyDetails;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.params.__hpcc_id === params.__hpcc_id)
                return;

            this.initalized = false;
            this.widget._Summary.__hpcc_initalized = false;
            this.widget._Configuration.__hpcc_initalized = false;
            this.widget._Logs.__hpcc_initalized = false;
            this.widget._RequestInformation.__hpcc_initalized = false;
            this.widget._GetNumberOfFilesToCopy.__hpcc_initalized = false;
            this.widget._RequestInformation.set("disabled", true);
            this.widget._GetNumberOfFilesToCopy.set("disabled", true);

            this.inherited(arguments);

            if (this.params.hasConfig()) {
                this.widget._Configuration.set("disabled", false);
            } else {
                this.widget._Configuration.set("disabled", true);
                if (this.getSelectedChild().id === this.widget._Configuration.id) {
                    this.selectChild(this.widget._Summary.id);
                }
            }
            if (this.params.hasLogs()) {
                this.widget._Logs.set("disabled", false);
            } else {
                this.widget._Logs.set("disabled", true);
                if (this.getSelectedChild().id === this.widget._Logs.id) {
                    this.selectChild(this.widget._Summary.id);
                }
            }
            if (this.params.__hpcc_treeItem.Type === "RoxieCluster" && this.params.__hpcc_treeItem.OS) {
                this.widget._GetNumberOfFilesToCopy.set("disabled", false);
            } else {
                this.widget._GetNumberOfFilesToCopy.set("disabled", true);
                if (this.getSelectedChild().id === this.widget._GetNumberOfFilesToCopy.id) {
                    this.selectChild(this.widget._Summary.id);
                }
            }
            this.initTab();
        },

        initTab: function () {
            var context = this;
            var currSel = this.getSelectedChild();
            if (currSel.id === this.widget._Summary.id && !this.widget._Summary.__hpcc_initalized) {
                this.widget._Summary.__hpcc_initalized = true;
                var table = domConstruct.create("table", {});
                for (var key in this.params.__hpcc_treeItem) {
                    if (this.params.__hpcc_treeItem.hasOwnProperty(key) && !(this.params.__hpcc_treeItem[key] instanceof Object)) {
                        if (key.indexOf("__") !== 0) {
                            switch (key) {
                                case "Port":
                                case "Path":
                                case "ProcessNumber":
                                    break;
                                default:
                                    var tr = domConstruct.create("tr", {}, table);
                                    domConstruct.create("td", {
                                        innerHTML: "<b>" + key + ":&nbsp;&nbsp;</b>"
                                    }, tr);
                                    domConstruct.create("td", {
                                        innerHTML: this.params.__hpcc_treeItem[key]
                                    }, tr);
                            }
                        }
                    }
                }
                var tpMachine = null;
                if (this.params.__hpcc_treeItem.__hpcc_type === "TpMachine") {
                    tpMachine = this.params.__hpcc_treeItem;
                } else if (this.params.__hpcc_parentNode && this.params.__hpcc_parentNode.__hpcc_treeItem.__hpcc_type === "TpMachine") {
                    tpMachine = this.params.__hpcc_parentNode.__hpcc_treeItem;
                }
                var tpBinding = null;
                if (this.params.__hpcc_treeItem.__hpcc_type === "TpBinding") {
                    tpBinding = this.params.__hpcc_treeItem;
                } else if (this.params.__hpcc_parentNode && this.params.__hpcc_parentNode.__hpcc_treeItem.__hpcc_type === "TpBinding") {
                    tpBinding = this.params.__hpcc_parentNode.__hpcc_treeItem;
                }
                if (tpBinding && tpMachine) {
                    var tr = domConstruct.create("tr", {}, table);
                    domConstruct.create("td", {
                        innerHTML: "<b>URL:&nbsp;&nbsp;</b>"
                    }, tr);
                    var td = domConstruct.create("td", {
                    }, tr);
                    var url = tpBinding.Protocol + "://" + tpMachine.Netaddress + ":" + tpBinding.Port + "/";
                    domConstruct.create("a", {
                        href: url,
                        innerHTML: url
                    }, td);
                }
                this.details.setContent(table);
            } else if (currSel.id === this.widget._Configuration.id && !this.widget._Configuration.__hpcc_initalized) {
                this.widget._Configuration.__hpcc_initalized = true;
                this.params.getConfig().then(function (response) {
                    var xml = context.formatXml(response);
                    context.widget._Configuration.init({
                        sourceMode: "xml"
                    });
                    context.widget._Configuration.setText(xml);
                });
            } else if (currSel.id === this.widget._Logs.id && !this.widget._Logs.__hpcc_initalized) {
                this.widget._Logs.__hpcc_initalized = true;
                this.widget._Logs.init(this.params);
            } else if (currSel.id === this.widget._GetNumberOfFilesToCopy.id && !this.widget._GetNumberOfFilesToCopy.__hpcc_initalized) {
                this.widget._GetNumberOfFilesToCopy.__hpcc_initalized = true;
                this.widget._GetNumberOfFilesToCopy.init(this.params);
            }
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            }
        },

        refreshActionState: function () {
        }
    });
});
