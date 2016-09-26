/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/dom-attr",
    "dojo/request/iframe",
    "dojo/dom-class",
    "dojo/query",
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_TabContainerWidget",
    "hpcc/ESPWorkunit",
    "hpcc/ESPRequest",
    "hpcc/TargetSelectWidget",
    "hpcc/DelayLoadWidget",
    "hpcc/InfoGridWidget",
    "hpcc/WsWorkunits",
    "hpcc/GridDetailsWidget",
    "hpcc/WsDFUXref",

    "dojo/text!../templates/XrefDetailsWidget.html",

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
], function (declare, lang, i18n, nlsHPCC, dom, domForm, domAttr, iframe, domClass, query, Memory, Observable,
                registry,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                _TabContainerWidget, ESPWorkunit, ESPRequest, TargetSelectWidget, DelayLoadWidget, InfoGridWidget, WsWorkunits, GridDetailsWidget, WsDFUXref,
                template) {
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
            this.refreshActionState();
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

        refreshActionState: function () {
            var disabled = false;
            if (this.params.Name === "SuperFiles") {
                disabled = true;
            }

            this.foundFilesWidget = registry.byId(this.id + "_FoundFiles").set("disabled", disabled);
            this.orphanFilesWidget = registry.byId(this.id + "_OrphanFiles").set("disabled", disabled);
            this.lostFilesWidget = registry.byId(this.id + "_LostFiles").set("disabled", disabled);
            this.directoriesWidget = registry.byId(this.id + "_Directories").set("disabled", disabled);;
        }
    });
});