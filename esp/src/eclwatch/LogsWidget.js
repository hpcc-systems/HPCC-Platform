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
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/request/iframe",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/_Widget",
    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/LogsWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",
    "dijit/layout/ContentPane"
],
    function (declare, lang, i18n, nlsHPCC, array, Memory, Observable, iframe,
            registry,
            OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
            _Widget, ESPUtil, ESPRequest, ESPWorkunit,
            template) {
        return declare("LogsWidget", [_Widget], {
            templateString: template,
            baseClass: "LogsWidget",
            i18n: nlsHPCC,

            borderContainer: null,
            logsGrid: null,

            lastSelection: null,

            downloadFrameID: 0,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
            },

            startup: function (args) {
                this.inherited(arguments);
                var store = new Memory({
                    idProperty: "id",
                    data: []
                });
                this.logsStore = Observable(store);

                this.logsGrid = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                    allowSelectAll: true,
                    columns: {
                        sel: selector({
                            width: 27,
                            selectorType: "checkbox"
                        }),
                        Type: {
                            label: this.i18n.Type,
                            width: 117
                        },
                        Description: {
                            label: this.i18n.Description
            }
                    },
                    store: this.logsStore
                }, this.id + "LogsGrid");
                this.logsGrid.startup();
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            _onRefresh: function (evt) {
                this.fetchLogs();
            },

            _doDownload: function (option) {
                var selection = this.logsGrid.getSelected();

                for (var i = 0; i < selection.length; ++i) {
                    var downloadPdfIframeName = "downloadIframe_" + this.downloadFrameID++;
                    var frame = iframe.create(downloadPdfIframeName);
                    var params = "";

                    switch (selection[i].Type) {
                        case "dll":
                            var parts = selection[i].Orig.Name.split("/");
                            if (parts.length) {
                                var leaf = parts[parts.length - 1];
                                params = "/WUFile/" + leaf + "?Wuid=" + this.wu.Wuid + "&Name=" + selection[i].Orig.Name + "&Type=" + selection[i].Orig.Type;
                            }
                            break;
                        case "res":
                            params = "/WUFile/res.txt?Wuid=" + this.wu.Wuid + "&Type=" + selection[i].Orig.Type;
                            break;
                        case "ThorLog":
                        case "EclAgentLog":
                            params = "/WUFile/" + selection[i].Type + "?Wuid=" + this.wu.Wuid + "&Name=" + selection[i].Orig.Name + "&Type=" + selection[i].Orig.Type;
                            break;
                        case "ThorSlaveLog":
                            params = "/WUFile?Wuid=" + this.wu.Wuid + "&Process=" + selection[i].Orig.ProcessName + "&ClusterGroup=" + selection[i].Orig.ProcessName + "&LogDate=" + selection[i].Orig.LogDate + "&SlaveNumber=" + selection[i].Orig.SlaveNumber + "&Type=" + selection[i].Type;
                            break;
                        case "Archive Query":
                            params = "/WUFile/ArchiveQuery?Wuid=" + this.wu.Wuid + "&Name=ArchiveQuery&Type=ArchiveQuery";
                            break;
                    }

                    var url = ESPRequest.getBaseURL() + params + (option ? "&Option=" + option : "&Option=1");
                    iframe.setSrc(frame, url, true);
                }

            },
            _onDownload: function (args) {
                this._doDownload(1);
            },
            _onDownloadZip: function (args) {
                this._doDownload(2);
            },
            _onDownloadGZip: function (args) {
                this._doDownload(3);
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
                            context.fetchLogs();
                        }
                    });
                }
            },

            fetchLogs: function () {
                var context = this;
                this.wu.getInfo({
                    onAfterSend: function (response) {
                        context.logData = [];
                        if (response.HasArchiveQuery) {
                            context.logData.push({
                                id: "A:0",
                                Type: "Archive Query"
                            });
                        }
                        if (response.Helpers && response.Helpers.ECLHelpFile) {
                            context.loadHelpers(response.Helpers.ECLHelpFile);
                        }
                        if (response.ThorLogList && response.ThorLogList.ThorLogInfo) {
                            context.loadThorLogInfo(response.ThorLogList.ThorLogInfo);
                        }
                        context.logsStore.setData(context.logData);
                        context.logsGrid.refresh();
                    }
                });
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
