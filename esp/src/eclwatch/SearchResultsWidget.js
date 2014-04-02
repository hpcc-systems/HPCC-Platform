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
    "dojo/promise/all",

    "dijit/form/Button",

    "dojox/widget/Standby",
    "dojox/validate",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/WsWorkunits",
    "hpcc/FileSpray",
    "hpcc/WsDfu",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, all,
                Button,
                Standby, validate,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, WsWorkunits, FileSpray, WsDfu, DelayLoadWidget, ESPUtil) {
    return declare("SearchResultsWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_SearchResults,
        idProperty: "id",

        doSearch: function (searchText) {
            lang.mixin(this.params, {
                searchText: searchText
            });
            this.searchText = searchText;
            this.selectChild(this.gridTab);
            this.refreshGrid();
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.searchText) {
                this.doSearch(params.searchText);
            }
            this._refreshActionState();
        },

        getTitle: function () {
            return "Results Widget";
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: this.store,
                columns: {
                    col1: selector({ width: 27, selectorType: 'checkbox' }),
                    Type: { label: this.i18n.What, width: 108, sortable: true },
                    Reason: { label: this.i18n.Where, width: 108, sortable: true },
                    Summary: {
                        label: this.i18n.Who, sortable: true,
                        formatter: function (summary, idx) {
                            return "<a href='#' rowIndex=" + idx + " class='" + context.id + "SearchResultClick'>" + summary + "</a>";
                        }
                    }
                }
            }, domID);

            var context = this;
            on(document, "." + this.id + "SearchResultClick:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });

            this.standby = new Standby({
                target: domID,
                image: dojoConfig.getImageURL("loadingBar.gif"),
                color: null
            });
            document.body.appendChild(this.standby.domNode);
            this.standby.startup();

            return retVal;
        },

        createDetail: function (id, row, params) {
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
                    break;
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
                    break;
                case "LogicalFile":
                    if (row.isSuperfile) {
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
                    } else {
                        return new DelayLoadWidget({
                            id: id,
                            title: row.Summary,
                            closable: true,
                            delayWidget: "LFDetailsWidget",
                            hpcc: {
                                params: {
                                    Name: row._name
                                }
                            }
                        });
                    }
                    break;
                default:
                    break;
            }
            return null;
        },

        loadWUQueryResponse: function(prefix, response) {
            var workunits = lang.getObject("WUQueryResponse.Workunits.ECLWorkunit", false, response)
            if (workunits) {
                var idPrefix = prefix.split(" ").join("_");
                var context = this;
                arrayUtil.forEach(workunits, function (item, idx) {
                    context.store.add({
                        id: "WsWorkunitsWUQuery" + idPrefix + idx,
                        Type: "ECL Workunit",
                        Reason: prefix,
                        Summary: item.Wuid,
                        _type: "Wuid",
                        _wuid: item.Wuid
                    });
                });
                return workunits.length;
            }
            return 0;
        },

        loadGetDFUWorkunitsResponse: function (prefix, response) {
            var workunits = lang.getObject("GetDFUWorkunitsResponse.results.DFUWorkunit", false, response)
            if (workunits) {
                var idPrefix = prefix.split(" ").join("_");
                var context = this;
                arrayUtil.forEach(workunits, function (item, idx) {
                    context.store.add({
                        id: "FileSprayGetDFUWorkunits" + idPrefix + idx,
                        Type: "DFU Workunit",
                        Reason: prefix,
                        Summary: item.ID,
                        _type: "DFUWuid",
                        _wuid: item.ID
                    });
                });
                return workunits.length;
            }
            return 0;
        },

        loadGetDFUWorkunitResponse: function (prefix, response) {
            var workunit = lang.getObject("GetDFUWorkunitResponse.result", false, response)
            if (workunit && workunit.State !== 999) {
                var idPrefix = prefix.split(" ").join("_");
                this.store.add({
                    id: "FileSprayGetDFUWorkunits" + idPrefix + workunit.ID,
                    Type: "DFU Workunit",
                    Reason: prefix,
                    Summary: workunit.ID,
                    _type: "DFUWuid",
                    _wuid: workunit.ID
                });
                return 1;
            }
            return 0;
        },

        loadDFUQueryResponse: function (prefix, response) {
            var items = lang.getObject("DFUQueryResponse.DFULogicalFiles.DFULogicalFile", false, response)
            if (items) {
                var idPrefix = prefix.split(" ").join("_");
                var context = this;
                arrayUtil.forEach(items, function (item, idx) {
                    context.store.add({
                        id: "WsDfuDFUQuery" + idPrefix + idx,
                        Type: "Logical File",
                        Reason: prefix,
                        Summary: item.Name,
                        _type: "LogicalFile",
                        _name: item.Name
                    });
                });
                return items.length;
            }
            return 0;
        },

        searchAll: function() {
            var context = this;
            this.standby.show();
            all([
                WsWorkunits.WUQuery({ request: { Wuid: "*" + this.searchText + "*" }, suppressExceptionToaster: true }).then(function (response) {
                    context.loadWUQueryResponse(context.i18n.WUID, response);
                }),
                WsWorkunits.WUQuery({ request: { Jobname: "*" + this.searchText + "*" } }).then(function (response) {
                    context.loadWUQueryResponse(context.i18n.JobName, response);
                }),
                WsWorkunits.WUQuery({ request: { Owner: this.searchText } }).then(function (response) {
                    context.loadWUQueryResponse(context.i18n.Owner, response);
                }),
                WsWorkunits.WUQuery({ request: { ECL: this.searchText } }).then(function (response) {
                    context.loadWUQueryResponse(context.i18n.ECL, response);
                }),
                //  DFU WU  ---
                FileSpray.GetDFUWorkunit({ request: { wuid: "*" + this.searchText  + "*"}, suppressExceptionToaster: true }).then(function (response) {
                    context.loadGetDFUWorkunitResponse(context.i18n.WUID, response);
                }),
                FileSpray.GetDFUWorkunits({ request: { Jobname: "*" + this.searchText + "*" } }).then(function (response) {
                    context.loadGetDFUWorkunitsResponse(context.i18n.JobName, response);
                }),
                FileSpray.GetDFUWorkunits({ request: { Owner: this.searchText } }).then(function (response) {
                    context.loadGetDFUWorkunitsResponse(context.i18n.Owner, response);
                }),
                //  Logical Files  ---
                WsDfu.DFUQuery({ request: { LogicalName: "*" + this.searchText + "*" } }).then(function (response) {
                    context.loadDFUQueryResponse(context.i18n.LogicalName, response);
                }),
                WsDfu.DFUQuery({ request: { Description: "*" + this.searchText + "*" } }).then(function (response) {
                    context.loadDFUQueryResponse(context.i18n.Description, response);
                }),
                WsDfu.DFUQuery({ request: { Owner: this.searchText } }).then(function (response) {
                    context.loadDFUQueryResponse(context.i18n.Owner, response);
                })
            ]).then(function (results) {
                context.standby.hide();
            }, function (error) {
                context.standby.hide();
            });
        },

        refreshGrid: function (args) {
            this.store.setData([]);
            this.grid.refresh();
            if (this.searchText) {
                this.searchAll();
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
            }
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);
        }
    });
});
