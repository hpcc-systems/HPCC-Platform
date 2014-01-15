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
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/common",
    "dojo/i18n!./nls/ActivityWidget",
    "dojo/_base/array",
    "dojo/on",

    "dijit/form/Button",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/tree",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPActivity",
    "hpcc/WUDetailsWidget",
    "hpcc/DFUWUDetailsWidget",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsCommon, nlsSpecific, arrayUtil, on,
                Button,
                OnDemandGrid, Keyboard, Selection, selector, tree, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPActivity, WUDetailsWidget, DFUWUDetailsWidget, ESPUtil) {
    return declare("ActivityWidget", [GridDetailsWidget], {

        i18n: lang.mixin(nlsCommon, nlsSpecific),
        gridTitle: nlsSpecific.title,
        idProperty: "Wuid",

        doSearch: function (searchText) {
            this.searchText = searchText;
            this.selectChild(this.gridTab);
            this.refreshGrid();
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            this.activity.monitor(function (activity) {
                context.grid.set("query", {});
            });

            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;
            this.noDataMessage = this.i18n.loadingMessage;
            this.activity = ESPActivity.Get();
            var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: this.activity.getStore(),
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                        disabled: function (item) {
                            if (item.__hpcc_type) {
                                switch (item.__hpcc_type) {
                                    case "TargetCluster":
                                        return true;
                                }
                            }
                            return false;
                        },
                        sortable: false
                    }),
                    DisplayName: tree({
                        label: this.i18n.Target,
                        width: 225,
                        sortable: true,
                        shouldExpand: function(row, level, previouslyExpanded) {
                            return true;
                        },
                        formatter: function (_name, row) {
                            var img;
                            var name = "";
                            if (row.__hpcc_type === "TargetCluster") {
                                img = "/esp/files/img/server.png";
                                name = row.__hpcc_id;
                            } else {
                                img = row.getStateImage();
                                name = "<a href='#' class='" + context.id + "WuidClick'>" + row.Wuid + "</a>";
                            }
                            return "<img src='" + img + "'/>&nbsp;" + name;
                        }
                    }),
                    State: {
                        label: this.i18n.State,
                        sortable: true,
                        formatter: function (state, row) {
                            if (row.__hpcc_type === "TargetCluster") {
                                return "";
                            }
                            if (row.Duration) {
                                return state + " (" + row.Duration + ")";
                            } else if (row.Instance && state.indexOf(row.Instance) === -1) {
                                return state + " [" + row.Instance + "]";
                            }
                            return state;
                        }
                    },
                    Owner: { label: this.i18n.Owner, width: 90, sortable: true },
                    Jobname: { label: this.i18n.JobName, sortable: true }
                }
            }, domID);

            on(document, "." + this.id + "WuidClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        createDetail: function (id, row, params) {
            if (row.Server === "DFUserver") {
                return new DFUWUDetailsWidget.fixCircularDependency({
                    id: id,
                    title: row.ID,
                    closable: true,
                    hpcc: {
                        params: {
                            Wuid: row.ID
                        }
                    }
                });
            } 
            return new WUDetailsWidget({
                id: id,
                title: row.Wuid,
                closable: true,
                hpcc: {
                    params: {
                        Wuid: row.Wuid
                    }
                }
            });
        },

        loadRunning: function (response) {
            var items = lang.getObject("ActivityResponse.Running", false, response)
            if (items) {
                var context = this;
                arrayUtil.forEach(items, function (item, idx) {
                    context.store.add({
                        id: "ActivityRunning" + idx,
                        ClusterName: item.ClusterName,
                        Wuid: item.Wuid,
                        Owner: item.Owner,
                        Jobname: item.Owner,
                        Summary: item.Name + " (" + prefix + ")",
                        _type: "LogicalFile",
                        _name: item.Name
                    });
                });
                return items.length;
            }
            return 0;
        },

        refreshGrid: function (args) {
            this.activity.refresh();
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);
        }
    });
});
