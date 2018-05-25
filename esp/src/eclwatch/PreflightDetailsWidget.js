define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "dijit/registry",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/Dialog",
    "dijit/form/TextBox",

    "hpcc/GridDetailsWidget",
    "src/ESPUtil",

    "dgrid/selector"

], function (declare, lang, i18n, nlsHPCC, arrayUtil,
    registry, Button, ToolbarSeparator, Dialog, TextBox,
    GridDetailsWidget, ESPUtil,
    selector) {
        return declare("PreflightDetailsWidget", [GridDetailsWidget], {
            i18n: nlsHPCC,

            preflightTab: null,
            preflightWidgetLoaded: false,
            gridTitle: nlsHPCC.title_PreflightResults,
            idProperty: "__hpcc_id",

            init: function (params, route) {
                if (this.inherited(arguments))
                    return;
                this.initalized = false;
                this.params = params;
                this.setColumns(params);
                this.refresh(params, route);
            },

            refresh: function (params, route) {
                route === "machines" ? this.refreshMachinesGrid(params) : this.refreshClusterGrid(params);
            },

            createGrid: function (domID) {
                var context = this;

                this.openButton = registry.byId(this.id + "Open");
                this.openButton.set("disabled", true);

                var retVal = new declare([ESPUtil.Grid(true, true)])({
                    store: this.store,
                    columns: context.setColumns()
                }, domID);
                return retVal;
            },

            setColumns: function (params) {
                var context = this;
                var dynamicColumns = {
                    Location: { label: this.i18n.Location },
                    Component: { label: this.i18n.Component },
                    Condition: { label: this.i18n.Condition },
                    State: { label: this.i18n.State }
                }

                if (params) {
                    var finalColumns = params.Columns.Item;
                    for (var index in finalColumns) {
                        var clean = finalColumns[index].replace(/([~!@#$%^&*()_+=`{}\[\]\|\\:;'<>,.\/? ])+/g, '').replace(/^(-)+|(-)+$/g, '');
                        if (clean === "Condition") {
                            lang.mixin(dynamicColumns, {
                                "Condition": {
                                    "label": finalColumns[index],
                                    formatter: function (Name, row) {
                                        return context.formatState(row.Condition);
                                    }
                                }
                            });
                        }
                        if (clean === 'State') {
                            lang.mixin(dynamicColumns, {
                                "State": {
                                    "label": finalColumns[index],
                                    formatter: function (Name, row) {
                                        return context.formatCondition(row.State);
                                    }
                                }
                            });
                        } else {
                            var tmpObj = {};
                            tmpObj[clean] = {
                                "label": finalColumns[index]
                            };
                            lang.mixin(dynamicColumns, tmpObj);
                        }
                    }
                    context.grid.set("columns", dynamicColumns);
                }
            },

            formatState: function (row) {
                switch (row) {
                    case 1:
                        return this.i18n.Normal;
                    case 2:
                        return this.i18n.Warning;
                    case 3:
                        return this.i18n.Minor;
                    case 4:
                        return this.i18n.Major;
                    case 5:
                        return this.i18n.Critical;
                    case 6:
                        return this.i18n.Fatal;
                    default:
                        return this.i18n.Unknown;
                }
            },

            formatCondition: function (row) {
                switch (row) {
                    case 0:
                        return this.i18n.Unknown;
                    case 1:
                        return this.i18n.Starting;
                    case 2:
                        return this.i18n.Stopping;
                    case 3:
                        return this.i18n.Suspended;
                    case 4:
                        return this.i18n.Recycling;
                    case 5:
                        return this.i18n.Ready;
                    case 6:
                        return this.i18n.Busy;
                    default:
                        return this.i18n.Unknown;
                }
            },

            refreshMachinesGrid: function (params) {
                var context = this;
                var params = this.params;
                var results = [];

                if (params) {
                    arrayUtil.forEach(params.Machines.MachineInfoEx, function (row, idx) {
                        var dynamicRowsObj = {};
                        row.RoxieState ? lang.mixin(dynamicRowsObj, { RoxieState: row.RoxieState }) : "";
                        lang.mixin(dynamicRowsObj, {
                            Location: row.Address + " " + row.ComponentPath,
                            Component: row.DisplayType + "[" + row.ComponentName + "]",
                            Condition: row.ComponentInfo.Condition,
                            State: row.ComponentInfo.State,
                            UpTime: row.UpTime
                        });
                        if (row.Processors) {
                            arrayUtil.forEach(row.Processors.ProcessorInfo, function (processor, idx) {
                                lang.mixin(dynamicRowsObj, {
                                    CPULoad: processor.Load + "%"
                                });
                            });
                        }
                        if (row.Storage) {
                            arrayUtil.forEach(row.Storage.StorageInfo, function (storage, idx) {
                                var cleanColumn = storage.Description.replace(/([~!@#$%^&*()_+=`{}\[\]\|\\:;'<>,.\/? ])+/g, '').replace(/^(-)+|(-)+$/g, '');
                                var tmpObj = {};
                                tmpObj[cleanColumn] = storage.PercentAvail + "%";
                                lang.mixin(dynamicRowsObj, tmpObj);
                            });
                        }
                        results.push(dynamicRowsObj);
                    });
                }

                context.store.setData(results);
                context.grid.set("query", {});
            },

            refreshClusterGrid: function (params) {
                var context = this;
                var params = this.params;
                var results = [];

                if (params) {
                    arrayUtil.forEach(params.TargetClusterInfoList.TargetClusterInfo, function (row, idx) {
                        arrayUtil.forEach(row.Processes.MachineInfoEx, function (setRows, idx) {
                            var dynamicRowsObj = {};
                            lang.mixin(dynamicRowsObj, {
                                Location: setRows.Address + " " + setRows.ComponentPath,
                                Component: setRows.DisplayType + "[" + setRows.ComponentName + "]",
                                Condition: setRows.ComponentInfo.Condition,
                                State: setRows.ComponentInfo.State,
                                UpTime: setRows.UpTime
                            });
                            if (setRows.Processors) {
                                arrayUtil.forEach(setRows.Processors.ProcessorInfo, function (processor, idx) {
                                    lang.mixin(dynamicRowsObj, {
                                        CPULoad: processor.Load + "%"
                                    });
                                });
                            }
                            if (setRows.Storage) {
                                arrayUtil.forEach(setRows.Storage.StorageInfo, function (storage, idx) {
                                    var cleanColumn = storage.Description.replace(/([~!@#$%^&*()_+=`{}\[\]\|\\:;'<>,.\/? ])+/g, '').replace(/^(-)+|(-)+$/g, '');
                                    var tmpObj = {};
                                    tmpObj[cleanColumn] = storage.PercentAvail + "%";
                                    lang.mixin(dynamicRowsObj, tmpObj);
                                });
                            }
                            results.push(dynamicRowsObj);
                        });
                    });
                }

                context.store.setData(results);
                context.grid.set("query", {});
            }
        });
    });