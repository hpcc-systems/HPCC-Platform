define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "dijit/form/Button",

    "hpcc/GridDetailsWidget",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",
    "src/Utility"
], function (declare, i18n, nlsHPCC, arrayUtil,
    Button,
    GridDetailsWidget, DelayLoadWidget, ESPUtil, Utility) {
    return declare("GraphsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Graphs,
        idProperty: "Name",

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.alphanumSort["Name"] = true;
        },

        getStateImageName: function (row) {
            if (row.Complete) {
                return "workunit_completed.png";
            } else if (row.Running) {
                return "workunit_running.png";
            } else if (row.Failed) {
                return "workunit_failed.png";
            }
            return "workunit.png";
        },

        getStateImageHTML: function (row) {
            return Utility.getImageHTML(this.getStateImageName(row));
        },

        createGridColumns: function () {
            //  Abstract  ---
        },

        createGrid: function (domID) {
            var context = this;
            this.openLegacyMode = new Button({
                label: this.i18n.OpenLegacyMode,
                onClick: function (event) {
                    context._onOpen(event, {
                        legacyMode: true
                    });
                }
            }).placeAt(this.widget.Open.domNode, "after");
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: this.createGridColumns()
            }, domID);

            retVal.on(".dgrid-row:click", function (evt) {
                context.syncSelectionFrom(context.grid);
            });

            retVal.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        getDetailID: function (row, params) {
            var retVal = "Detail" + row[this.idProperty];
            if (params && params.SubGraphId) {
                retVal += params.SubGraphId;
            }
            if (params && params.legacyMode) {
                retVal += "Legacy";
            }
            return retVal;
        },

        openGraph: function (graphName, subgraphID) {
            this._onRowDblClick({ Name: graphName }, { SubGraphId: subgraphID });
        },

        createDetail: function (_id, row, params) {
            params = params || {};
            var localParams = this.localParams(_id, row, params);
            var title = row.Name;
            var delayWidget = "GraphTree7Widget";
            var delayProps = {
                _hostPage: this,
                forceJS: true
            };
            if (params && params.SubGraphId) {
                title = params.SubGraphId + " - " + title;
            }
            if (params && params.legacyMode) {
                delayWidget = "GraphTreeWidget";
                title += " (L)";
                delayProps = {};
            }
            return new DelayLoadWidget({
                id: _id,
                title: title,
                closable: true,
                delayWidget: delayWidget,
                delayProps: delayProps,
                hpcc: {
                    type: "graph",
                    params: localParams
                }
            });
        },

        refreshGrid: function (args) {
            //  Abstract  ---
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);

            this.openLegacyMode.set("disabled", !selection.length);
        },

        syncSelectionFrom: function (sourceControl) {
            var graphItems = [];
            var timingItems = [];

            //  Get Selected Items  ---
            if (sourceControl === this.grid) {
                arrayUtil.forEach(sourceControl.getSelected(), function (item, idx) {
                    timingItems.push(item);
                });
            }

            //  Set Selected Items  ---
            if (sourceControl !== this.grid) {
                this.grid.setSelected(graphItems);
            }
        }
    });
});
