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
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",

    "dijit/registry",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/tree",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPActivity",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on,
                registry, Button, ToolbarSeparator,
                OnDemandGrid, Keyboard, Selection, selector, tree, ColumnResizer, DijitRegistry,
                GridDetailsWidget, ESPActivity, DelayLoadWidget, ESPUtil) {
    return declare("ActivityWidget", [GridDetailsWidget], {

        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_Activity,
        idProperty: "Wuid",

        _onPause: function (event, params) {
            arrayUtil.forEach(this.grid.getSelected(), function (item, idx) {
                if (this.activity.isInstanceOfQueue(item)) {
                    var context = this;
                    item.pause().then(function(response) {
                        context._refreshActionState();
                    });
                }
            }, this);
        },

        _onResume: function (event, params) {
            arrayUtil.forEach(this.grid.getSelected(), function (item, idx) {
                if (this.activity.isInstanceOfQueue(item)) {
                    var context = this;
                    item.resume().then(function (response) {
                        context._refreshActionState();
                    });
                }
            }, this);
        },

        _onClear: function (event, params) {
            arrayUtil.forEach(this.grid.getSelected(), function (item, idx) {
                if (this.activity.isInstanceOfQueue(item)) {
                    item.clear();
                }
            }, this);
            this._onRefresh();
        },

        _onWUAbort: function (event, params) {
            arrayUtil.forEach(this.grid.getSelected(), function (item, idx) {
                if (this.activity.isInstanceOfWorkunit(item)) {
                    item.abort();
                }
            }, this);
            this._onRefresh();
        },

        _onWUPriority: function (event, priority) {
            arrayUtil.forEach(this.grid.getSelected(), function (item, idx) {
                if (this.activity.isInstanceOfWorkunit(item)) {
                    var queue = item.get("ESPQueue");
                    if (queue) {
                        queue.setPriority(item.Wuid, priority);
                    }
                }
            }, this);
            this._onRefresh();
        },

        _onWUTop: function (event, params) {
            var selected = this.grid.getSelected();
            for (var i = selected.length - 1; i >= 0; --i) {
                var item = selected[i];
                if (this.activity.isInstanceOfWorkunit(item)) {
                    var queue = item.get("ESPQueue");
                    if (queue) {
                        queue.moveTop(item.Wuid);
                    }
                }
            }
            this._onRefresh();
        },

        _onWUUp: function (event, params) {
            arrayUtil.forEach(this.grid.getSelected(), function (item, idx) {
                if (this.activity.isInstanceOfWorkunit(item)) {
                    var queue = item.get("ESPQueue");
                    if (queue) {
                        queue.moveUp(item.Wuid);
                    }
                }
            }, this);
            this._onRefresh();
        },

        _onWUDown: function (event, params) {
            var selected = this.grid.getSelected();
            for (var i = selected.length - 1; i >= 0; --i) {
                var item = selected[i];
                if (this.activity.isInstanceOfWorkunit(item)) {
                    var queue = item.get("ESPQueue");
                    if (queue) {
                        queue.moveDown(item.Wuid);
                    }
                }
            }
            this._onRefresh();
        },

        _onWUBottom: function (event, params) {
            arrayUtil.forEach(this.grid.getSelected(), function (item, idx) {
                if (this.activity.isInstanceOfWorkunit(item)) {
                    var queue = item.get("ESPQueue");
                    if (queue) {
                        queue.moveBottom(item.Wuid);
                    }
                }
            }, this);
            this._onRefresh();
        },

        doSearch: function (searchText) {
            this.searchText = searchText;
            this.selectChild(this.gridTab);
            this.refreshGrid();
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            this.activity.watch("changedCount", function (item, oldValue, newValue) {
                context.grid.set("query", {});
                context._refreshActionState();
            });

            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;

            this.openButton = registry.byId(this.id + "Open");
            this.clusterPauseButton = new Button({
                id: this.id + "PauseButton",
                label: "Pause",
                onClick: function (event) {
                    context._onPause(event);
                }
            }).placeAt(this.openButton.domNode, "before");
            this.clusterResumeButton = new Button({
                id: this.id + "ResumeButton",
                label: "Resume",
                onClick: function (event) {
                    context._onResume(event);
                }
            }).placeAt(this.openButton.domNode, "before");
            this.clusterClearButton = new Button({
                id: this.id + "ClearButton",
                label: "Clear",
                onClick: function (event) {
                    context._onClear(event);
                }
            }).placeAt(this.openButton.domNode, "before");
            var tmpSplitter = new ToolbarSeparator().placeAt(this.openButton.domNode, "before");

            this.wuMoveBottomButton = new Button({
                id: this.id + "MoveBottomButton",
                label: "Bottom",
                onClick: function (event) {
                    context._onWUBottom(event);
                }
            }).placeAt(this.openButton.domNode, "after");
            this.wuMoveDownButton = new Button({
                id: this.id + "MoveDownButton",
                label: "Down",
                onClick: function (event) {
                    context._onWUDown(event);
                }
            }).placeAt(this.openButton.domNode, "after");
            this.wuMoveUpButton = new Button({
                id: this.id + "MoveUpButton",
                label: "Up",
                onClick: function (event) {
                    context._onWUUp(event);
                }
            }).placeAt(this.openButton.domNode, "after");
            this.wuMoveTopButton = new Button({
                id: this.id + "MoveTopButton",
                label: "Top",
                onClick: function (event) {
                    context._onWUTop(event);
                }
            }).placeAt(this.openButton.domNode, "after");
            tmpSplitter = new ToolbarSeparator().placeAt(this.openButton.domNode, "after");
            this.wuLowPriorityButton = new Button({
                id: this.id + "LowPriorityButton",
                label: "Low",
                onClick: function (event) {
                    context._onWUPriority(event, "low");
                }
            }).placeAt(this.openButton.domNode, "after");
            this.wuNormalPriorityButton = new Button({
                id: this.id + "NormalPriorityButton",
                label: "Normal",
                onClick: function (event) {
                    context._onWUPriority(event, "normal");
                }
            }).placeAt(this.openButton.domNode, "after");
            this.wuHighPriorityButton = new Button({
                id: this.id + "HighPriorityButton",
                label: "High",
                onClick: function (event) {
                    context._onWUPriority(event, "high");
                }
            }).placeAt(this.openButton.domNode, "after");
            tmpSplitter = new ToolbarSeparator().placeAt(this.openButton.domNode, "after");
            this.wuAbortButton = new Button({
                id: this.id + "AbortButton",
                label: "Abort",
                onClick: function (event) {
                    context._onWUAbort(event);
                }
            }).placeAt(this.openButton.domNode, "after");

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
                        sortable: false
                    }),
                    Priority: {
                        renderHeaderCell: function (node) {
                            node.innerHTML = dojoConfig.getImageHTML("priority.png");
                        },
                        width: 25,
                        sortable: false,
                        formatter: function (Priority) {
                            switch (Priority) {
                                case "high":
                                    return dojoConfig.getImageHTML("priority_high.png");
                                case "low":
                                    return dojoConfig.getImageHTML("priority_low.png");
                            }
                            return "";
                        }
                    },
                    DisplayName: tree({
                        label: this.i18n.TargetWuid,
                        width: 270,
                        sortable: true,
                        shouldExpand: function (row, level, previouslyExpanded) {
                            return true;
                        },
                        formatter: function (_name, row) {
                            var img = "";
                            var name = "";
                            if (context.activity.isInstanceOfQueue(row)) {
                                if (row.isPaused()) {
                                    img = dojoConfig.getImageURL("server_paused.png");
                                } else {
                                    img = dojoConfig.getImageURL("server.png");
                                }
                                name = _name;
                            } else {
                                img = row.getStateImage();
                                name = "<a href='#' class='" + context.id + "WuidClick'>" + row.Wuid + "</a>";
                            }
                            return "<img src='" + img + "'/>&nbsp;" + name;
                        }
                    }),
                    DisplaySize: { label: this.i18n.Size, width: 59, sortable: true },
                    State: {
                        label: this.i18n.State,
                        sortable: false,
                        formatter: function (state, row) {
                            if (context.activity.isInstanceOfQueue(row)) {
                                if (row.isPaused()) {
                                    return row.StatusDetails;
                                }
                                return "";
                            }
                            if (row.Duration) {
                                return state + " (" + row.Duration + ")";
                            } else if (row.Instance && !(state.indexOf && state.indexOf(row.Instance) !== -1)) {
                                return state + " [" + row.Instance + "]";
                            }
                            return state;
                        }
                    },
                    Owner: { label: this.i18n.Owner, width: 90, sortable: false },
                    Jobname: { label: this.i18n.JobName, sortable: false }
                },
                getSelected: function () {
                    var retVal = [];
                    for (var id in this.selection) {
                        var item = context.activity.resolve(id)
                        if (item) {
                            retVal.push(item);
                        }
                    }
                    return retVal;
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
            if (this.activity.isInstanceOfQueue(row)) {
            } else if (this.activity.isInstanceOfWorkunit(row)) {
                if (row.Server === "DFUserver") {
                    return new DelayLoadWidget({
                        id: id,
                        title: row.ID,
                        closable: true,
                        delayWidget: "DFUWUDetailsWidget",
                        hpcc: {
                            params: {
                                Wuid: row.ID
                            }
                        }
                    });
                }
                return new DelayLoadWidget({
                    id: id,
                    title: row.Wuid,
                    closable: true,
                    delayWidget: "WUDetailsWidget",
                    hpcc: {
                        params: {
                            Wuid: row.Wuid
                        }
                    }
                });
            }
            return null;
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
            var clusterSelected = false;
            var wuSelected = false;
            var clusterPausedSelected = false;
            var clusterNotPausedSelected = false;
            var clusterHasItems = false;
            var wuCanHigh = false;
            var wuCanNormal = false;
            var wuCanLow = false;
            var wuCanUp = false;
            var wuCanDown = false;
            var context = this;
            arrayUtil.forEach(selection, function (item, idx) {
                if (context.activity.isInstanceOfQueue(item)) {
                    clusterSelected = true;
                    if (item.isPaused()) {
                        clusterPausedSelected = true;
                    } else {
                        clusterNotPausedSelected = true;
                    }
                    if (item.getChildCount()) {
                        clusterHasItems = true;
                    }
                } else if (context.activity.isInstanceOfWorkunit(item)) {
                    wuSelected = true;
                    var queue = item.get("ESPQueue");
                    if (queue) {
                        if (queue.canChildMoveUp(item.__hpcc_id)) {
                            wuCanUp = true;
                        }
                        if (queue.canChildMoveDown(item.__hpcc_id)) {
                            wuCanDown = true;
                        }
                    }
                    if (item.get("Priority") !== "high") {
                        wuCanHigh = true;
                    }
                    if (item.get("Priority") !== "normal") {
                        wuCanNormal = true;
                    }
                    if (item.get("Priority") !== "low") {
                        wuCanLow = true;
                    }
                }
            });

            this.clusterPauseButton.set("disabled", !clusterNotPausedSelected);
            this.clusterResumeButton.set("disabled", !clusterPausedSelected);
            this.clusterClearButton.set("disabled", !clusterHasItems);
            this.openButton.set("disabled", !wuSelected);
            this.wuAbortButton.set("disabled", !wuSelected);
            this.wuHighPriorityButton.set("disabled", !wuCanHigh);
            this.wuNormalPriorityButton.set("disabled", !wuCanNormal);
            this.wuLowPriorityButton.set("disabled", !wuCanLow);
            this.wuMoveTopButton.set("disabled", !wuCanUp);
            this.wuMoveUpButton.set("disabled", !wuCanUp);
            this.wuMoveDownButton.set("disabled", !wuCanDown);
            this.wuMoveBottomButton.set("disabled", !wuCanDown);
        }
    });
});
