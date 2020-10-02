define([
    "dojo/_base/declare",
    "src/nlsHPCC",

    "dijit/registry",
    "dijit/form/Button",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/WsDFUXref",
    "src/ESPUtil"

], function (declare, nlsHPCCMod,
    registry, Button,
    selector,
    GridDetailsWidget, WsDFUXref, ESPUtil) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("XrefFoundFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_FoundFilesFor,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this._refreshActionState();
            this.refreshGrid();

            this.gridTab.set("title", this.i18n.title_FoundFilesFor + ":" + this.params.Name);
        },

        _onRefresh: function (event) {
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            this._delete = new Button({
                id: this.id + "Delete",
                disabled: false,
                onClick: function (val) {
                    context._onDeleteFiles();
                },
                label: this.i18n.Delete
            }).placeAt(this.openButton.domNode, "after");
            this._attach = new Button({
                id: this.id + "Attach",
                disabled: false,
                onClick: function (val) {
                    context._onAttachFiles();
                },
                label: this.i18n.Attach
            }).placeAt(this.openButton.domNode, "after");
            dojo.destroy(this.id + "Open");

            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                        label: ""
                    }),
                    Name: { label: this.i18n.Name, width: 100, sortable: false },
                    Modified: { label: this.i18n.Modified, width: 30, sortable: true },
                    Parts: { label: this.i18n.Parts, width: 30, sortable: true, className: 'justify-right' },
                    Size: { label: this.i18n.Size, width: 30, sortable: true, className: 'justify-right' }
                }
            }, domID);

            return retVal;
        },

        refreshActionState: function (event) {
            var selection = this.grid.getSelected();
            var hasSelection = selection.length;

            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
            registry.byId(this.id + "Attach").set("disabled", !hasSelection);
        },

        _onDeleteFiles: function (event) {
            var context = this;
            var selections = this.grid.getSelected();
            var list = this.arrayToList(selections, "Name");
            if (confirm(this.i18n.DeleteSelectedFiles + "\n" + list)) {
                WsDFUXref.DFUXRefArrayAction(selections, this.i18n.Delete, context.params.Name, "Found").then(function (response) {
                    context.refreshGrid();
                });
            }
        },

        _onAttachFiles: function (event) {
            var context = this;
            var selections = this.grid.getSelected();
            var list = this.arrayToList(selections, "Name");
            if (confirm(this.i18n.AddTheseFilesToDali + "\n" + list)) {
                WsDFUXref.DFUXRefArrayAction(selections, "Attach", context.params.Name, "Found").then(function (response) {
                    context.refreshGrid();
                });
            }
        },

        refreshGrid: function () {
            var context = this;
            WsDFUXref.DFUXRefFoundFiles(this.params.Name).then(function (response) {
                context.store.setData(response);
                context.grid.set("query", {});
            });
        }
    });
});
