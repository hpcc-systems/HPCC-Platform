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
    "dojo/data/ObjectStore",
    "dojo/request/iframe",

    "dijit/registry",
    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/IndirectSelection",

    "hpcc/ESPWorkunit",

    "dojo/text!../templates/LogsWidget.html"
],
    function (declare, array, lang, Memory, ObjectStore, iframe,
            registry, _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, 
            EnhancedGrid, IndirectSelection,
            ESPWorkunit,
            template) {
        return declare("LogsWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "LogsWidget",
            borderContainer: null,
            logsGrid: null,

            dataStore: null,

            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.logsGrid = new dojox.grid.EnhancedGrid({
                    id: this.id + "LogsGrid",
                    structure: [
                        { name: "Type", field: "Type", width: 8 },
                        { name: "Description", field: "Description", width: "100%" }
                    ],
                    plugins: {
                        indirectSelection: {
                            headerSelector:true, width:"40px", styles:"text-align: center;"
                        }
                    }
                });
                this.logsGrid.placeAt(this.id + 'CenterPane');
                this.logsGrid.startup();

                var context = this;
                this.logsGrid.on("RowClick", function (evt) {
                });

                this.logsGrid.on("RowDblClick", function (evt) {
                });
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            _doDownload: function (option) {
                var selection = this.logsGrid.selection.getSelected();

                for (var i = 0; i < selection.length; ++i) {
                    var downloadPdfIframeName = "downloadIframe_" + i;
                    var frame = iframe.create(downloadPdfIframeName);
                    var params = "";

                    switch (selection[i].Type) {
                        case "dll":
                            var parts = selection[i].Orig.Name.split("/");
                            if (parts.length) {
                                var leaf = parts[parts.length - 1];
                                params = "/WUFile/" + leaf + "?Wuid=" + this.wu.wuid + "&Name=" + selection[i].Orig.Name + "&Type=" + selection[i].Orig.Type;
                            }
                            break;
                        case "res":
                            params = "/WUFile/res.txt?Wuid=" + this.wu.wuid + "&Type=" + selection[i].Orig.Type;
                            break;
                        case "ThorLog":
                        case "EclAgentLog":
                            params = "/WUFile/" + selection[i].Type + "?Wuid=" + this.wu.wuid + "&Process=" + selection[i].Orig.Description + "&Type=" + selection[i].Orig.Type;
                            break;
                        case "ThorSlaveLog":
                            params = "/WUFile?Wuid=" + this.wu.wuid + "&Process=" + selection[i].Orig.ProcessName + "&ClusterGroup=" + selection[i].Orig.ProcessName + "&LogDate=" + selection[i].Orig.LogDate + "&SlaveNumber=" + selection[i].Orig.SlaveNumber + "&Type=" + selection[i].Type;
                            break;
                    }

                    var url = this.wu.getBaseURL() + params + (option ? "&Option=" + option : "&Option=1");
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
                this.wu = new ESPWorkunit({
                    wuid: params.Wuid
                });

                var context = this;
                this.wu.monitor(function () {
                    context.wu.getInfo({
                        onGetAll: function (response) {
                            context.logData = [];
                            if (response.Helpers && response.Helpers.ECLHelpFile) {
                                context.loadHelpers(response.Helpers.ECLHelpFile);
                            }
                            if (response.ThorLogList && response.ThorLogList.ThorLogInfo) {
                                context.loadThorLogInfo(response.ThorLogList.ThorLogInfo);
                            }
                            var memory = new Memory({ data: context.logData });
                            var store = new ObjectStore({ objectStore: memory });
                            context.logsGrid.setStore(store);
                            context.logsGrid.setQuery({
                                Type: "*"
                            });
                        }
                    });
                });
            },

            loadHelpers: function (helpers) {
                for (var i = 0; i < helpers.length; ++i) {
                    this.logData.push({
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
