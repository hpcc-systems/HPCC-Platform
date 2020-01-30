define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "dijit/registry",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/WsDFUXref",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil"
], function (declare, lang, i18n, nlsHPCC, arrayUtil,
    registry, ToolbarSeparator, Button,
    selector,
    GridDetailsWidget, WsDFUXref, DelayLoadWidget, ESPUtil) {
    return declare("XrefQueryWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        gridTitle: nlsHPCC.XRef,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this._refreshActionState();
            this.refreshGrid();
            this.initTab();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.init) {
                    currSel.init({
                        Name: currSel.params.Name,
                        Status: currSel.params.Status,
                        Modified: currSel.params.Modified
                    });
                }
            }
        },

        _onRefresh: function (event) {
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;

            this.openButton = registry.byId(this.id + "Open");

            this.generate = new Button({
                id: this.id + "Generate",
                disabled: false,
                onClick: function (val) {
                    var selections = context.grid.getSelected();
                    if (confirm(context.i18n.RunningServerStrain)) {
                        for (var i = selections.length - 1; i >= 0; --i) {
                            WsDFUXref.DFUXRefBuild({
                                request: {
                                    Cluster: selections[i].Name
                                }
                            })
                        }
                    }
                    context.refreshGrid();
                },
                label: this.i18n.Generate
            }).placeAt(this.openButton.domNode, "after");

            this.cancel = new Button({
                id: this.id + "Cancel",
                disabled: false,
                onClick: function (val) {
                    if (confirm(context.i18n.CancelAllMessage)) {
                        WsDFUXref.DFUXRefBuildCancel({
                            request: {}
                        })
                    }
                    context.refreshGrid();
                },
                label: this.i18n.CancelAll
            }).placeAt(this.openButton.domNode, "after");

            new ToolbarSeparator().placeAt(this.openButton.domNode, "after");
            new ToolbarSeparator().placeAt(this.cancel.domNode, "after");

            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    col1: selector({
                        width: 10,
                        selectorType: 'checkbox',
                        label: ""
                    }),
                    Name: {
                        label: this.i18n.Name, width: 100, sortable: false,
                        formatter: function (Name, idx) {
                            return "<a href='#' class='dgrid-row-url'>" + Name + "</a>";
                        }
                    },
                    Modified: { label: this.i18n.LastRun, width: 30, sortable: false },
                    Status: { label: this.i18n.LastMessage, width: 30, sortable: false }
                }
            }, domID);

            retVal.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var item = retVal.row(evt).data;
                    context._onRowDblClick(item);
                }
            });
            retVal.on(".dgrid-row:dblclick", function (evt) {
                if (context._onRowDblClick) {
                    var item = retVal.row(evt).data;
                    context._onRowDblClick(item);
                }
            });
            return retVal;
        },

        _onOpen: function (event) {
            var selections = this.grid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(selections[i].Name, {
                    Name: selections[i].Name,
                    Modified: selections[i].Modified,
                    Status: selections[i].Status

                });
                if (i === 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },

        _onRowDblClick: function (item) {
            var nameTab = this.ensurePane(item.Name, {
                Name: item.Name,
                Modified: item.Modified,
                Status: item.Status
            });
            this.selectChild(nameTab);
        },

        refreshGrid: function () {
            var context = this;

            WsDFUXref.WUGetXref({
                request: {}
            }).then(function (response) {
                var results = [];
                var newRows = [];
                if (lang.exists("DFUXRefListResponse.DFUXRefListResult.XRefNode", response)) {
                    results = response.DFUXRefListResponse.DFUXRefListResult.XRefNode;
                }

                if (results.length) {
                    arrayUtil.forEach(results, function (row, idx) {
                        newRows.push({
                            Name: row.Name,
                            Modified: row.Modified,
                            Status: row.Status
                        });
                    });
                } else {
                    newRows.push({
                        Name: results.Name,
                        Modified: results.Modified,
                        Status: results.Status
                    });
                }

                context.store.setData(newRows);
                context.grid.set("query", {});
            });
        },

        ensurePane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new DelayLoadWidget({
                    id: id,
                    title: params.Name,
                    closable: true,
                    delayWidget: "XrefDetailsWidget",
                    params: params
                });
                this.addChild(retVal, 1);
            }
            return retVal;
        }
    });
});
