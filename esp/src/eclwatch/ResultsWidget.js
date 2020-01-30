define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",

    "dijit/layout/ContentPane",
    "dijit/form/Button",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/ESPRequest",
    "src/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil"

], function (declare, i18n, nlsHPCC, arrayUtil,
    ContentPane, Button,
    selector,
    GridDetailsWidget, ESPRequest, ESPWorkunit, DelayLoadWidget, ESPUtil) {
    return declare("ResultsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Results,
        idProperty: "Sequence",

        wu: null,

        _onRowDblClickFile: function (row) {
            var tab = this.ensurePane(row, {
                logicalFile: true
            });
            this.selectChild(tab);
        },

        _onRowDblClickView: function (row, viewName) {
            var tab = this.ensurePane(row, {
                resultView: true,
                viewName: viewName
            });
            this.selectChild(tab);
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.alphanumSort["Name"] = true;
            this.alphanumSort["Value"] = true;

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                        context.refreshGrid();
                    }
                });
            }
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;
            this.openViz = new Button({
                label: this.i18n.Visualize,
                onClick: function (event) {
                    context._onOpen(event, {
                        vizMode: true
                    });
                }
            }).placeAt(this.widget.Open.domNode, "after");

            this.openLegacy = new Button({
                label: this.i18n.OpenLegacyMode,
                onClick: function (event) {
                    context._onOpen(event, {
                        legacyMode: true
                    });
                }
            }).placeAt(this.widget.Open.domNode, "after");

            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
                    }),
                    Name: {
                        label: this.i18n.Name, width: 180, sortable: true,
                        formatter: function (Name, idx) {
                            return "<a href='#' class='dgrid-row-url'>" + Name + "</a>";
                        }
                    },
                    FileName: {
                        label: this.i18n.FileName, sortable: true,
                        formatter: function (FileName, idx) {
                            return "<a href='#' class='dgrid-row-url2'>" + FileName + "</a>";
                        }
                    },
                    Value: {
                        label: this.i18n.Value,
                        width: 360,
                        sortable: true
                    },
                    ResultViews: {
                        label: this.i18n.Views, sortable: true,
                        formatter: function (ResultViews, idx) {
                            var retVal = "";
                            arrayUtil.forEach(ResultViews, function (item, idx) {
                                retVal += "<a href='#' viewName=" + encodeURIComponent(item) + " class='dgrid-row-url3'>" + item + "</a>&nbsp;";
                            });
                            return retVal;
                        }
                    }
                }
            }, domID);

            retVal.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            retVal.on(".dgrid-row-url2:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClickFile(row);
                }
            });
            retVal.on(".dgrid-row-url3:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClickView(row, evt.srcElement.getAttribute("viewName"));
                }
            });
            return retVal;
        },

        getDetailID: function (row, params) {
            var retVal = "Detail" + row[this.idProperty];
            if (params && params.vizMode) {
                retVal += "Viz";
            } else if (params && params.legacyMode) {
                retVal += "Legacy";
            } else if (row.FileName && params && params.logicalFile) {
                retVal += "LogicalFile";
            } else if (params && params.resultView && params.viewName) {
                retVal += "View";
            }
            return retVal;
        },

        createDetail: function (id, row, params) {
            if (params && params.vizMode) {
                return new DelayLoadWidget({
                    id: id,
                    title: "[V] " + row.Name,
                    closable: true,
                    delayWidget: "VizWidget",
                    hpcc: {
                        type: "VizWidget",
                        params: {
                            Wuid: row.Wuid,
                            Sequence: row.Sequence
                        }
                    }
                });
            } else if (params && params.legacyMode) {
                return new DelayLoadWidget({
                    id: id,
                    title: "[L] " + row.Name,
                    closable: true,
                    delayWidget: "IFrameWidget",
                    hpcc: {
                        type: "IFrameWidget",
                        params: {
                            src: "/WsWorkunits/WUResult?Wuid=" + row.Wuid + "&Sequence=" + row.Sequence
                        }
                    }
                });
            } else if (row.FileName && params && params.logicalFile) {
                return new DelayLoadWidget({
                    id: id,
                    title: "[F] " + row.Name,
                    closable: true,
                    delayWidget: "LFDetailsWidget",
                    hpcc: {
                        type: "LFDetailsWidget",
                        params: {
                            Name: row.FileName
                        }
                    }
                });
            } else if (params && params.resultView && params.viewName) {
                return new ContentPane({
                    id: id,
                    title: row.Name + " [" + decodeURIComponent(params.viewName) + "]",
                    closable: true,
                    content: dojo.create("iframe", {
                        src: ESPRequest.getBaseURL("WsWorkunits") + "/WUResultView?Wuid=" + row.Wuid + "&ResultName=" + row.Name + "&ViewName=" + params.viewName,
                        style: "border: 0; width: 100%; height: 100%"
                    }),
                    hpcc: {
                        type: "ContentPane",
                        params: {
                            Name: row.Name,
                            viewName: params.viewName
                        }
                    },
                    noRefresh: true
                });
            } else {
                return new DelayLoadWidget({
                    id: id,
                    title: row.Name,
                    closable: true,
                    style: "padding: 0px; overflow: hidden",
                    delayWidget: "ResultWidget",
                    hpcc: {
                        type: "ResultWidget",
                        params: {
                            Wuid: row.Wuid,
                            Sequence: row.Sequence
                        }
                    }
                });
            }
        },

        refreshGrid: function (args) {
            var context = this;
            this.wu.getInfo({
                onGetResults: function (results) {
                    context.store.setData(results);
                    context.grid.refresh();
                }
            });
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);

            this.openLegacy.set("disabled", !this.wu || !selection.length);
            this.openViz.set("disabled", !this.wu || !selection.length);
        }

    });
});
