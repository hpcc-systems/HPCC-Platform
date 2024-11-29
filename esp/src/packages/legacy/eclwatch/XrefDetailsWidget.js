define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/dom",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "src/WsDFUXref",

    "dojo/text!../templates/XrefDetailsWidget.html",

    "hpcc/DelayLoadWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Form",
    "dijit/form/Textarea",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/form/NumberTextBox",
    "dijit/form/ValidationTextBox",
    "dijit/form/Select",
    "dijit/form/ToggleButton",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/form/TextBox",
    "dijit/Dialog",
    "dijit/form/SimpleTextarea",

    "hpcc/TableContainer"
], function (declare, nlsHPCCMod, dom,
    registry,
    _TabContainerWidget, WsDFUXref,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("XrefDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "XrefDetailsWidget",
        i18n: nlsHPCC,

        initalized: false,
        loaded: false,
        summaryWidget: null,
        foundFilesWidget: null,
        foundFilesWidgetLoaded: null,
        orphanFilesWidget: null,
        orphanFilesWidgetLoaded: null,
        lostFilesWidget: null,
        lostFilesWidgetLoaded: null,
        directoriesWidget: null,
        directoriesWidgetLoaded: null,
        errorsWidget: null,
        errorsWidgetLoaded: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.foundFilesWidget = registry.byId(this.id + "_FoundFiles");
            this.orphanFilesWidget = registry.byId(this.id + "_OrphanFiles");
            this.lostFilesWidget = registry.byId(this.id + "_LostFiles");
            this.directoriesWidget = registry.byId(this.id + "_Directories");
            this.errorsWidget = registry.byId(this.id + "_Errors");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Name) {
                dom.byId(this.id + "Cluster").textContent = params.Name;
                dom.byId(this.id + "LastRun").textContent = params.Modified;
                dom.byId(this.id + "LastMessage").textContent = params.Status;
            }

            if (params.Status.indexOf("Generated") !== -1) {
                this.setDisabled(this.widget._FoundFiles.id, false);
                this.setDisabled(this.widget._OrphanFiles.id, false);
                this.setDisabled(this.widget._LostFiles.id, false);
                this.setDisabled(this.widget._Directories.id, false);
                this.setDisabled(this.widget._Errors.id, false);
            } else {
                this.setDisabled(this.widget._FoundFiles.id, true);
                this.setDisabled(this.widget._OrphanFiles.id, true);
                this.setDisabled(this.widget._LostFiles.id, true);
                this.setDisabled(this.widget._Directories.id, true);
                this.setDisabled(this.widget._Errors.id, true);
            }
        },

        _onGenerate: function (arg) {
            WsDFUXref.DFUXRefBuild({
                request: {
                    Cluster: this.params.Name
                }
            });
        },

        _onCancel: function (arg) {
            WsDFUXref.DFUXRefBuildCancel({
                request: {}
            });
            alert(this.i18n.AllQueuedItemsCleared);
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel.id === this.widget._FoundFiles.id && !this.widget._FoundFiles.__hpcc_initalized) {
                this.widget._FoundFiles.init({
                    Name: this.params.Name
                });
            } else if (currSel.id === this.widget._OrphanFiles.id && !this.widget._OrphanFiles.__hpcc_initalized) {
                this.widget._OrphanFiles.init({
                    Name: this.params.Name
                });
            } else if (currSel.id === this.widget._LostFiles.id && !this.widget._LostFiles.__hpcc_initalized) {
                this.widget._LostFiles.init({
                    Name: this.params.Name
                });
            } else if (currSel.id === this.widget._Directories.id && !this.widget._Directories.__hpcc_initalized) {
                this.widget._Directories.init({
                    Name: this.params.Name
                });
            } else if (currSel.id === this.widget._Errors.id && !this.widget._Errors.__hpcc_initalized) {
                this.widget._Errors.init({
                    Name: this.params.Name
                });
            }
        },

    });
});
