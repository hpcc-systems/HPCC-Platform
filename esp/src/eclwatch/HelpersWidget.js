/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/dom-construct",
    "dojo/on",

    "dijit/registry",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPWorkunit",
    "hpcc/DelayLoadWidget"

],  function (declare, lang, i18n, nlsHPCC, arrayUtil, Memory, Observable, domConstruct, on,
            registry, Button, ToolbarSeparator,
            selector,
            GridDetailsWidget, ESPUtil, ESPRequest, ESPWorkunit, DelayLoadWidget) {
        return declare("LogsWidget", [GridDetailsWidget], {
            baseClass: "LogsWidget",
            i18n: nlsHPCC,

            gridTitle: nlsHPCC.Helpers,
            idProperty: "id",
	    
            lastSelection: null,

            downloadFrameID: 0,

            _getURL: function (item, option) {
                var params = "";
                switch (item.Type) {
                    case "dll":
                        var parts = item.Orig.Name.split("/");
                        if (parts.length) {
                            var leaf = parts[parts.length - 1];
                            params = "/WUFile/" + leaf + "?Wuid=" + this.wu.Wuid + "&Name=" + item.Orig.Name + "&Type=" + item.Orig.Type;
                        }
                        break;
                    case "res":
                        params = "/WUFile/res.txt?Wuid=" + this.wu.Wuid + "&Type=" + item.Orig.Type;
                        break;
                    case "EclAgentLog":
                        params = "/WUFile/" + item.Type + "?Wuid=" + this.wu.Wuid + "&Process=" + item.Orig.PID + "&Name=" + item.Orig.Name + "&Type=" + item.Orig.Type;
                        break;
                    case "ThorSlaveLog":
                        params = "/WUFile?Wuid=" + this.wu.Wuid + "&Process=" + item.Orig.ProcessName + "&ClusterGroup=" + item.Orig.ProcessName + "&LogDate=" + item.Orig.LogDate + "&SlaveNumber=" + item.Orig.SlaveNumber + "&Type=" + item.Type;
                        break;
                    case "Archive Query":
                        params = "/WUFile/ArchiveQuery?Wuid=" + this.wu.Wuid + "&Name=ArchiveQuery&Type=ArchiveQuery";
                        break;
                    case "ECL":
                        params = "/WUFile?Wuid=" + this.wu.Wuid + "&Type=WUECL";
                        break;
                    case "Workunit XML":
                        params = "/WUFile?Wuid=" + this.wu.Wuid + "&Type=XML";
                        break;
                    case "cpp":
                    case "hpp":
                        params = "/WUFile?Wuid=" + this.wu.Wuid + "&Name=" + item.Orig.Name + "&IPAddress=" + item.Orig.IPAddress + "&Description=" + item.Orig.Description + "&Type=" + item.Orig.Type;
                        break;
                    case "xml":
                        if (option !== undefined)
                            params = "/WUFile?Wuid=" + this.wu.Wuid + "&Name=" + item.Orig.Name + "&IPAddress=" + item.Orig.IPAddress + "&Description=" + item.Orig.Description + "&Type=" + item.Orig.Type;
                        break;
                    default:
                        if (item.Type.indexOf("ThorLog") === 0)
                            params = "/WUFile/" + item.Type + "?Wuid=" + this.wu.Wuid + "&Process=" + item.Orig.PID + "&Name=" + item.Orig.Name + "&Type=" + item.Orig.Type;
                        break;
                }

                return ESPRequest.getBaseURL() + params + (option ? "&Option=" + option : "&Option=1");
            },

            _doDownload: function (option) {
                var selection = this.grid.getSelected();
                var urls = [];

                for (var i = 0; i < selection.length; ++i) {
                    window.open(this._getURL(selection[i], option));
                }
            },
            //  Plugin wrapper  ---
            init: function (params) {
                if (this.inherited(arguments))
                    return;

                var context = this;
                if (params.Wuid) {
                    this.wu = ESPWorkunit.Get(params.Wuid);
                    var monitorCount = 4;
                    this.wu.monitor(function () {
                        if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                            context.refreshGrid();
                        }
                    });
                }
            },

            createGrid: function (domID) {
                var context = this;
                this.openButton = registry.byId(this.id + "Open");
                this.downloadGZip = new Button({
                    label: this.i18n.GZip,
                    onClick: function (event) {
                        context._doDownload(3);
                    }
                }).placeAt(this.openButton.domNode, "after");
                this.downloadZip = new Button({
                    label: this.i18n.Zip,
                    onClick: function (event) {
                        context._doDownload(2);
                    }
                }).placeAt(this.openButton.domNode, "after");
                this.downloadFile = new Button({
                    label: this.i18n.File,
                    onClick: function (event) {
                        context._doDownload(1);
                    }
                }).placeAt(this.openButton.domNode, "after");
                var label = document.createTextNode("");
                var downloadLabal = domConstruct.toDom("<b> " + this.i18n.Download + ":  </b>");
                domConstruct.place(downloadLabal, this.openButton.domNode, "after");
                tmpSplitter = new ToolbarSeparator().placeAt(this.openButton.domNode, "after");

                var retVal = new declare([ESPUtil.Grid(false, true)])({
                    store: this.store,
                    columns: {
                        sel: selector({
                            width: 27,
                            selectorType: "checkbox"
                        }),
                        Type: {
                            label: this.i18n.Type,
                            width: 160,
                            formatter: function (Type, row) {
                                if (context.canShowContent(Type)) {
                                    if (!row.Orig) {
                                        return "<a href='#' class='dgrid-row-url'>" + Type + "</a>"
                                    } if (row.Orig.Description === undefined && row.Orig.Type === Type) {
                                        return "<a href='#' class='dgrid-row-url'>" + Type + "</a>"
                                    } else {
                                        return "<a href='#' class='dgrid-row-url'>" + Type + " (" + row.Orig.Description + ")" + "</a>"
                                    }
                                }
                            return Type;
                            }
                        },
                        Description: {
                            label: this.i18n.Description
                        },
                        FileSize: {
                            label: this.i18n.FileSize,
                            width: 90
                        }
                    }
                }, domID);
                retVal.on(".dgrid-row-url:click", function (evt) {
                    if (context._onRowDblClick) {
                        var row = context.grid.row(evt).data;
                        context._onRowDblClick(row);
                    }
                });

                return retVal;
            },

            createDetail: function (id, row, params) {
                if (this.canShowContent(row.Type)) {
                    var name = row.Type;
                    if (row.Description) {
                        var descParts = row.Description.split("/");
                        name = descParts.length ? descParts[descParts.length - 1] : row.Description;
                    }
                    var Wuid = "";
                    var sourceMode = "text";
                    switch (row.Type) {
                        case "ECL":
                            Wuid = this.wu.Wuid;
                            sourceMode = "ecl";
                            break;
                        case "Workunit XML":
                        case "Archive Query":
                        case "xml":
                            sourceMode = "xml";
                            break;
                    }
                    return new DelayLoadWidget({
                        id: id,
                        title: name,
                        closable: true,
                        delayWidget: "ECLSourceWidget",
                        hpcc: {
                            params: {
                                Wuid: Wuid,
                                sourceMode: sourceMode,
                                sourceURL: this._getURL(row, id)
                            }
                        }
                    });
                }
                return null;
            },

            refreshGrid: function (args) {
                var context = this;
                this.wu.getInfo({
                    onAfterSend: function (response) {
                        context.logData = [];
                        context.logData.push({
                            id: "E:0",
                            Type: "ECL"
                        });
                        context.logData.push({
                            id: "X:0",
                            Type: "Workunit XML",
                            FileSize: response.WUXMLSize
                        });
                        if (response.hasArchiveQuery) {
                            context.logData.push({
                                id: "A:0",
                                Type: "Archive Query"
                            });
                        }
                        if (response.helpers) {
                            context.loadHelpers(response.helpers);
                        }
                        if (response.thorLogList) {
                            context.loadThorLogInfo(response.thorLogList);
                        }
                        context.store.setData(context.logData);
                        context.grid.refresh();
                    }
                });
            },
            
            canShowContent: function (type) {
                switch (type) {
                    case "dll":
                        return false;
                }
                return true;
            },

            refreshActionState: function (selection) {
                var canShowContent = false;
                var canDownload = false;
                arrayUtil.forEach(selection, function (item, idx) {
                    if (item.Type !== "dll") {
                        isNotDll = true;
                    }
                    canShowContent = canShowContent ? canShowContent : this.canShowContent(item.Type);
                    canDownload = true;
                }, this);
                registry.byId(this.id + "Open").set("disabled", !canShowContent);
                this.downloadGZip.set("disabled", !canDownload);
                this.downloadZip.set("disabled", !canDownload);
                this.downloadFile.set("disabled", !canDownload);
            },

            loadLogs: function (logs) {
                for (var i = 0; i < logs.length; ++i) {
                    this.logData.push({
                        id: "L:" + i,
                        Type: "dfulog",
                        Description: logs[i],
                        Orig: logs[i]
                    });
                }
            },

            loadHelpers: function (helpers) {
                for (var i = 0; i < helpers.length; ++i) {
                    this.logData.push({
                        id: "H:" + i,
                        Type: helpers[i].Type,
                        Description: helpers[i].IPAddress ? "//" + helpers[i].IPAddress + helpers[i].Name : helpers[i].Name,
                        FileSize: helpers[i].FileSize,
                        Orig: helpers[i]
                    });
                }
            },

            loadThorLogInfo: function (thorLogInfo) {
                for (var i = 0; i < thorLogInfo.length; ++i) {
                    for (var j = 0; j < thorLogInfo[i].NumberSlaves; ++j) {
                        this.logData.push({
                            id: "T:" + i + "_" + j,
                            Type: "ThorSlaveLog",
                            Description: thorLogInfo[i].ClusterGroup + "." + thorLogInfo[i].LogDate + ".log (slave " + (j + 1) + " of " + thorLogInfo[i].NumberSlaves + ")",
                            Orig: lang.mixin({
                                SlaveNumber: j + 1
                            }, thorLogInfo[i])
                        });
                    }
                }
            }
        });
    });
