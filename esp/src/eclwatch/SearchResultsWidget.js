define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "src/Memory",
    "dojo/store/Observable",
    "dojo/on",
    "dojo/promise/all",

    "dojox/widget/Standby",
    "dojox/validate",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/WsWorkunits",
    "src/ESPWorkunit",
    "src/ESPDFUWorkunit",
    "src/ESPLogicalFile",
    "src/ESPQuery",
    "src/FileSpray",
    "src/WsDfu",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",
    "src/ESPSearch",
    "src/Utility"

], function (declare, lang, nlsHPCCMod, arrayUtil, MemoryMod, Observable, on, all,
    Standby, validate,
    selector,
    GridDetailsWidget, WsWorkunits, ESPWorkunit, ESPDFUWorkunit, ESPLogicalFile, ESPQuery, FileSpray, WsDfu, DelayLoadWidget, ESPUtil, ESPSearch, Utility) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("SearchResultsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_SearchResults,
        idProperty: "storeID",
        _rowID: 0,

        doSearch: function (searchText) {
            lang.mixin(this.params, {
                searchText: searchText
            });
            this.searchText = searchText.toUpperCase();
            this.refreshGrid();
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.searchText) {
                this.doSearch(params.searchText);
            }
            this.refreshGrid();
        },

        getTitle: function () {
            return "Results Widget";
        },

        refreshTab: function (tab) {
            var title = "";
            var store = null;
            switch (tab) {
                case this.eclTab:
                    title = this.i18n.title_WUQuery;
                    store = this.eclStore;
                    break;
                case this.dfuTab:
                    title = this.i18n.title_GetDFUWorkunits;
                    store = this.dfuStore;
                    break;
                case this.fileTab:
                    title = this.i18n.title_DFUQuery;
                    store = this.fileStore;
                    break;
                case this.queryTab:
                    title = this.i18n.title_QuerySetQuery;
                    store = this.queryStore;
                    break;
            }
            if (title && store) {
                tab.set("title", title + " (" + store.data.length + ")");
                tab.set("disabled", store.data.length === 0);
            }
        },

        getDetailID: function (row, params) {
            return "Detail" + row.id;
        },

        createGrid: function (domID) {
            this.eclStore = new Observable(new MemoryMod.Memory("Wuid"));
            this.dfuStore = new Observable(new MemoryMod.Memory("ID"));
            this.fileStore = new Observable(new MemoryMod.Memory("__hpcc_id"));
            this.queryStore = new Observable(new MemoryMod.Memory("__hpcc_id"));
            this.eclTab = this.ensurePane({ id: this.i18n.ECLWorkunit }, { type: this.i18n.ECLWorkunit });
            this.dfuTab = this.ensurePane({ id: this.i18n.DFUWorkunit }, { type: this.i18n.DFUWorkunit });
            this.fileTab = this.ensurePane({ id: this.i18n.LogicalFile }, { type: this.i18n.LogicalFile });
            this.queryTab = this.ensurePane({ id: this.i18n.Query }, { type: this.i18n.Query });

            var context = this;
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    col1: selector({ width: 27, selectorType: "checkbox" }),
                    Type: {
                        label: this.i18n.What, width: 108, sortable: true,
                        formatter: function (type, idx) {
                            return "<a href='#' onClick='return false;' rowIndex=" + idx + " class='" + context.id + "SearchTypeClick'>" + type + "</a>";
                        }
                    },
                    Reason: { label: this.i18n.Where, width: 108, sortable: true },
                    Summary: {
                        label: this.i18n.Who, sortable: true,
                        formatter: function (summary, idx) {
                            return "<a href='#' onClick='return false;' class='dgrid-row-url'>" + summary + "</a>";
                        }
                    }
                }
            }, domID);

            var context = this;
            retVal.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            on(document, "." + this.id + "SearchTypeClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick({ id: row.Type }, { type: row.Type });
                }
            });

            this.standby = new Standby({
                target: this.domNode,
                image: Utility.getImageURL("loadingBar.gif"),
                color: null
            });
            document.body.appendChild(this.standby.domNode);
            this.standby.startup();

            return retVal;
        },

        createDetail: function (id, row, params) {
            if (lang.exists("type", params)) {
                switch (params.type) {
                    case this.i18n.ECLWorkunit:
                        return new DelayLoadWidget({
                            id: id,
                            title: this.i18n.title_WUQuery + " (0)",
                            disabled: true,
                            delayWidget: "WUQueryWidget",
                            hpcc: {
                                params: {
                                    searchResults: this.eclStore
                                }
                            }
                        });
                    case this.i18n.DFUWorkunit:
                        return new DelayLoadWidget({
                            id: id,
                            title: this.i18n.title_GetDFUWorkunits + " (0)",
                            disabled: true,
                            delayWidget: "GetDFUWorkunitsWidget",
                            hpcc: {
                                params: {
                                    searchResults: this.dfuStore
                                }
                            }
                        });
                    case this.i18n.LogicalFile:
                    case this.i18n.SuperFile:
                        return new DelayLoadWidget({
                            id: id,
                            title: this.i18n.title_DFUQuery + " (0)",
                            disabled: true,
                            delayWidget: "DFUQueryWidget",
                            hpcc: {
                                params: {
                                    searchResults: this.fileStore
                                }
                            }
                        });
                    case this.i18n.Query:
                        return new DelayLoadWidget({
                            id: id,
                            title: this.i18n.title_QuerySetQuery + " (0)",
                            disabled: true,
                            delayWidget: "QuerySetQueryWidget",
                            hpcc: {
                                params: {
                                    searchResults: this.queryStore
                                }
                            }
                        });
                }
            } else {
                switch (row._type) {
                    case "Wuid":
                        return new DelayLoadWidget({
                            id: id,
                            title: row.Summary,
                            closable: true,
                            delayWidget: "WUDetailsWidget",
                            hpcc: {
                                params: {
                                    Wuid: row._wuid
                                }
                            }
                        });
                    case "DFUWuid":
                        return new DelayLoadWidget({
                            id: id,
                            title: row.Summary,
                            closable: true,
                            delayWidget: "DFUWUDetailsWidget",
                            hpcc: {
                                params: {
                                    Wuid: row._wuid
                                }
                            }
                        });
                    case "SuperFile":
                        return new DelayLoadWidget({
                            id: id,
                            title: row.Summary,
                            closable: true,
                            delayWidget: "SFDetailsWidget",
                            hpcc: {
                                params: {
                                    Name: row._name
                                }
                            }
                        });
                    case "LogicalFile":
                        return new DelayLoadWidget({
                            id: id,
                            title: row.Summary,
                            closable: true,
                            delayWidget: "LFDetailsWidget",
                            hpcc: {
                                params: {
                                    NodeGroup: row._nodeGroup,
                                    Name: row._name
                                }
                            }
                        });
                    case "Query":
                        return new DelayLoadWidget({
                            id: id,
                            title: row.Summary,
                            closable: true,
                            delayWidget: "QuerySetDetailsWidget",
                            hpcc: {
                                type: "QuerySetDetailsWidget",
                                params: {
                                    QuerySetId: row._querySetId,
                                    Id: row._id
                                }
                            }
                        });
                }
            }
            return null;
        },

        loadWUQueryResponse: function (prefix, response) {
            var workunits = lang.getObject("WUQueryResponse.Workunits.ECLWorkunit", false, response);
            if (workunits) {
                var idPrefix = prefix.split(" ").join("_");
                var context = this;
                arrayUtil.forEach(workunits, function (item, idx) {
                    context.store.add({
                        storeID: ++context._rowID,
                        id: context.id + item.Wuid,
                        Type: context.i18n.ECLWorkunit,
                        Reason: prefix,
                        Summary: item.Wuid,
                        _type: "Wuid",
                        _wuid: item.Wuid
                    });
                    context.eclStore.add(ESPWorkunit.Get(item.Wuid, item), { overwrite: true });
                });
                this.refreshTab(this.eclTab);
                return workunits.length;
            }
            return 0;
        },

        loadGetDFUWorkunitsResponse: function (prefix, response) {
            var workunits = lang.getObject("GetDFUWorkunitsResponse.results.DFUWorkunit", false, response);
            if (workunits) {
                var idPrefix = prefix.split(" ").join("_");
                var context = this;
                arrayUtil.forEach(workunits, function (item, idx) {
                    context.store.add({
                        storeID: ++context._rowID,
                        id: context.id + item.ID,
                        Type: context.i18n.DFUWorkunit,
                        Reason: prefix,
                        Summary: item.ID,
                        _type: "DFUWuid",
                        _wuid: item.ID
                    });
                    context.dfuStore.add(ESPDFUWorkunit.Get(item.ID, item), { overwrite: true });
                });
                this.refreshTab(this.dfuTab);
                return workunits.length;
            }
            return 0;
        },

        loadGetDFUWorkunitResponse: function (prefix, response) {
            var context = this;
            var workunit = lang.getObject("GetDFUWorkunitResponse.result", false, response);
            if (workunit && workunit.State !== 999) {
                var idPrefix = prefix.split(" ").join("_");
                this.store.add({
                    storeID: ++context._rowID,
                    id: context.id + workunit.ID,
                    Type: context.i18n.DFUWorkunit,
                    Reason: prefix,
                    Summary: workunit.ID,
                    _type: "DFUWuid",
                    _wuid: workunit.ID
                });
                this.dfuStore.add(ESPDFUWorkunit.Get(workunit.ID, workunit), { overwrite: true });
                this.refreshTab(this.dfuTab);
                return 1;
            }
            return 0;
        },

        loadDFUQueryResponse: function (prefix, response) {
            var items = lang.getObject("DFUQueryResponse.DFULogicalFiles.DFULogicalFile", false, response);
            if (items) {
                var idPrefix = prefix.split(" ").join("_");
                var context = this;
                arrayUtil.forEach(items, function (item, idx) {
                    if (item.isSuperfile) {
                        context.store.add({
                            storeID: ++context._rowID,
                            id: context.id + item.Name,
                            Type: context.i18n.SuperFile,
                            Reason: prefix,
                            Summary: item.Name,
                            _type: "SuperFile",
                            _name: item.Name
                        });
                    } else {
                        context.store.add({
                            storeID: ++context._rowID,
                            id: context.id + item.Name,
                            Type: context.i18n.LogicalFile,
                            Reason: prefix,
                            Summary: item.Name + " (" + item.NodeGroup + ")",
                            _type: "LogicalFile",
                            _nodeGroup: item.NodeGroup,
                            _name: item.Name
                        });
                    }
                    context.fileStore.add(ESPLogicalFile.Get(item.NodeGroup, item.Name, item), { overwrite: true });
                });
                this.refreshTab(this.fileTab);
                return items.length;
            }
            return 0;
        },

        loadWUListQueriesResponse: function (prefix, response) {
            var items = lang.getObject("WUListQueriesResponse.QuerysetQueries.QuerySetQuery", false, response);
            if (items) {
                var idPrefix = prefix.split(" ").join("_");
                var context = this;
                arrayUtil.forEach(items, function (item, idx) {
                    context.store.add({
                        storeID: ++context._rowID,
                        id: context.id + item.QuerySetId + "::" + item.Id,
                        Type: context.i18n.Query,
                        Reason: prefix,
                        Summary: item.Name + " (" + item.QuerySetId + " - " + item.Id + ")",
                        _type: "Query",
                        _querySetId: item.QuerySetId,
                        _id: item.Id
                    });
                    context.queryStore.add(ESPQuery.Get(item.QuerySetId, item.Id, item), { overwrite: true });
                });
                this.refreshTab(this.queryTab);
                return items.length;
            }
            return 0;
        },

        searchAll: function () {
            this.standby.show();

            if (validate.isNumberFormat(this.searchText, { format: ["W########-######", "W########-######-#???"] })) {
                var tab = this.ensurePane({
                    id: this.searchText,
                    Type: "ECL Workunit",
                    Summary: this.searchText,
                    _type: "Wuid",
                    _wuid: this.searchText
                }, {});
                this.selectChild(tab);

            } else if (validate.isNumberFormat(this.searchText, { format: ["D########-######", "D########-######-#???"] })) {
                var tab = this.ensurePane({
                    id: this.searchText,
                    Type: "DFU Workunit",
                    Summary: this.searchText,
                    _type: "DFUWuid",
                    _wuid: this.searchText
                }, {});
                this.selectChild(tab);
            }

            const specificSearch = ESPSearch.searchAll(this.searchText,
                (what, response) => { this.loadWUQueryResponse(what, response); },
                (what, response) => { this.loadGetDFUWorkunitResponse(what, response); },
                (what, response) => { this.loadGetDFUWorkunitsResponse(what, response); },
                (what, response) => { this.loadDFUQueryResponse(what, response); },
                (what, response) => { this.loadWUListQueriesResponse(what, response); },
                (searchCount) => { },
                (success) => { this.standby.hide(); });

            switch (specificSearch) {
                case "ecl":
                    this.selectChild(this.eclTab);
                    break;
                case "dfu":
                    this.selectChild(this.dfuTab);
                    break;
                case "file":
                    this.selectChild(this.fileTab);
                    break;
                case "query":
                    this.selectChild(this.queryTab);
                    break;
                default:
                    this.selectChild(this.gridTab);
            }
        },

        refreshGrid: function (args) {
            this.store.setData([]);
            this.eclStore.setData([]);
            this.refreshTab(this.eclTab);
            this.dfuStore.setData([]);
            this.refreshTab(this.dfuTab);
            this.fileStore.setData([]);
            this.refreshTab(this.fileTab);
            this.queryStore.setData([]);
            this.refreshTab(this.queryTab);
            this.grid.refresh();
            if (this.searchText) {
                this.searchAll();
            }
            this._refreshActionState();
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);
        }
    });
});
