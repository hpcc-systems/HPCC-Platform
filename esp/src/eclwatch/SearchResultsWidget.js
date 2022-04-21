define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "src/store/Memory",
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

        parseWuQueryResponse: function (prefix, response) {
            return response?.WUQueryResponse?.Workunits?.ECLWorkunit.map(item => {
                return {
                    storeID: ++this._rowID,
                    id: item.Wuid,
                    Type: nlsHPCC.ECLWorkunit,
                    Reason: prefix,
                    Summary: item.Wuid,
                    _type: "Wuid",
                    _wuid: item.Wuid,
                    ...item
                };
            });
        },

        parseGetDFUWorkunitResponse: function (prefix, response) {
            const item = response?.GetDFUWorkunitResponse?.result;
            return (item && item.State !== 999) ? {
                storeID: ++this._rowID,
                id: item.ID,
                Type: nlsHPCC.DFUWorkunit,
                Reason: prefix,
                Summary: item.ID,
                _type: "DFUWuid",
                _wuid: item.ID,
                ...item
            } : undefined;
        },

        parseGetDFUWorkunitsResponse: function (prefix, response) {
            return response?.GetDFUWorkunitsResponse?.results?.DFUWorkunit.map(item => {
                return {
                    storeID: ++this._rowID,
                    id: item.ID,
                    Type: nlsHPCC.DFUWorkunit,
                    Reason: prefix,
                    Summary: item.ID,
                    _type: "DFUWuid",
                    _wuid: item.ID,
                    ...item
                };
            });
        },

        parseDFUQueryResponse: function (prefix, response) {
            return response?.DFUQueryResponse?.DFULogicalFiles?.DFULogicalFile.map(item => {
                if (item.isSuperfile) {
                    return {
                        storeID: ++this._rowID,
                        id: item.Name,
                        Type: nlsHPCC.SuperFile,
                        Reason: prefix,
                        Summary: item.Name,
                        _type: "SuperFile",
                        _nodeGroup: item.NodeGroup,
                        _name: item.Name,
                        ...item
                    };
                }
                return {
                    storeID: ++this._rowID,
                    id: item.NodeGroup + "::" + item.Name,
                    Type: nlsHPCC.LogicalFile,
                    Reason: prefix,
                    Summary: item.Name + " (" + item.NodeGroup + ")",
                    _type: "LogicalFile",
                    _nodeGroup: item.NodeGroup,
                    _name: item.Name,
                    ...item
                };
            });
        },

        parseWUListQueriesResponse: function (prefix, response) {
            return response?.WUListQueriesResponse?.QuerysetQueries?.QuerySetQuery.map(item => {
                return {
                    storeID: ++this._rowID,
                    id: item.QuerySetId + "::" + item.Id,
                    Type: nlsHPCC.Query,
                    Reason: prefix,
                    Summary: item.Name + " (" + item.QuerySetId + " - " + item.Id + ")",
                    _type: "Query",
                    _querySetId: item.QuerySetId,
                    _id: item.Id,
                    ...item
                };
            });
        },

        loadWUQueryResponse: function (prefix, response) {
            const results = this.parseWuQueryResponse(prefix, response);
            if (results) {
                results.forEach((item, idx) => {
                    this.store.put(item, { overwrite: true });
                    this.eclStore.put(ESPWorkunit.Get(item._wuid, item), { overwrite: true });
                });
                this.refreshTab(this.eclTab);
                return results.length;
            }
            return 0;
        },

        loadGetDFUWorkunitResponse: function (prefix, response) {
            const workunit = this.parseGetDFUWorkunitResponse(prefix, response);
            if (workunit) {
                this.store.put(workunit, { overwrite: true });
                this.dfuStore.put(ESPDFUWorkunit.Get(workunit._wuid, workunit), { overwrite: true });
                this.refreshTab(this.dfuTab);
                return 1;
            }
            return 0;
        },

        loadGetDFUWorkunitsResponse: function (prefix, response) {
            const results = this.parseGetDFUWorkunitsResponse(prefix, response);
            if (results) {
                results.forEach((item, idx) => {
                    this.store.put(item, { overwrite: true });
                    this.dfuStore.put(ESPDFUWorkunit.Get(item._wuid, item), { overwrite: true });
                });
                this.refreshTab(this.dfuTab);
                return results.length;
            }
            return 0;
        },

        loadDFUQueryResponse: function (prefix, response) {
            const items = this.parseDFUQueryResponse(prefix, response);
            if (items) {
                items.forEach((item, idx) => {
                    this.store.put(item, { overwrite: true });
                    this.fileStore.put(ESPLogicalFile.Get(item._nodeGroup, item._name, item), { overwrite: true });
                });
                this.refreshTab(this.fileTab);
                return items.length;
            }
            return 0;
        },

        loadWUListQueriesResponse: function (prefix, response) {
            const items = this.parseWUListQueriesResponse(prefix, response);
            if (items) {
                items.forEach((item, idx) => {
                    this.store.put(item, { overwrite: true });
                    this.queryStore.put(ESPQuery.Get(item._querySetId, item._id, item), { overwrite: true });
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

            const specificSearch = ESPSearch.searchAll(
                this.searchText,
                (prefix, response) => { this.loadWUQueryResponse(prefix, response); },
                (prefix, response) => { this.loadGetDFUWorkunitResponse(prefix, response); },
                (prefix, response) => { this.loadGetDFUWorkunitsResponse(prefix, response); },
                (prefix, response) => { this.loadDFUQueryResponse(prefix, response); },
                (prefix, response) => { this.loadWUListQueriesResponse(prefix, response); },
                (searchCount) => { },
                (success) => { this.standby.hide(); }
            );

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
