/*##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/array",
    "dojo/_base/lang",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/request/iframe",

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/ESPUtil",
    "hpcc/ESPRequest",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/LogsWidget.html"
],
    function (declare, array, lang, Memory, Observable, iframe,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin,
            OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry,
            ESPUtil, ESPRequest, ESPWorkunit,
            template) {
        return declare("LogsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "LogsWidget",
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
                            selectorType: 'checkbox'
                        }),
                        Type: {
                            field: "Type",
                            width: 117
                        },
                        Description: {
                            field: "Description"
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
                if (this.initalized)
                    return;
                this.initalized = true;

                this.logData = [];
                var context = this;
                if (params.wu) {
                    this.wu = params.wu;
                    this.wu.fetchLogs(function (logs) {
                        context.loadLogs(logs);
                        context.logsStore.setData(context.logData);
                        context.logsGrid.refresh();
                    });
                } else {
                    this.wu = ESPWorkunit.Get(params.Wuid);
                    this.wu.monitor(function () {
                        context.wu.getInfo({
                            onAfterSend: function (response) {
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
                    });
                }
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
