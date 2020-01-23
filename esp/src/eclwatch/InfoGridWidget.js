define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-class",
    "dojo/store/Memory",
    "dojo/store/Observable",
    "dojo/topic",
    "dojo/has",

    "dijit/registry",

    "hpcc/_Widget",
    "src/ESPUtil",
    "src/ESPWorkunit",
    "src/Utility",

    "dojo/text!../templates/InfoGridWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Button",
    "dijit/form/CheckBox",
    "dijit/form/SimpleTextarea",
    "dijit/Dialog",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator"
],
    function (declare, lang, i18n, nlsHPCC, arrayUtil, dom, domConstruct, domClass, Memory, Observable, topic, has,
        registry,
        _Widget, ESPUtil, ESPWorkunit, Utility,
        template) {
        return declare("InfoGridWidget", [_Widget], {
            templateString: template,
            baseClass: "InfoGridWidget",
            i18n: nlsHPCC,

            borderContainer: null,
            infoGrid: null,
            errorsCheck: null,
            warningsCheck: null,
            infoCheck: null,

            dataStore: null,

            lastSelection: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.toolbar = registry.byId(this.id + "Toolbar");
                if (!this.showToolbar) {
                    this.toolbar.destroyRecursive();
                    this.toolbar = null;
                }
                this.errorsCheck = registry.byId(this.id + "Errors");
                this.warningsCheck = registry.byId(this.id + "Warnings");
                this.infoCheck = registry.byId(this.id + "Info");
                this.errWarnMenuItem = registry.byId("stubErrWarn");
                this.widget.ErrWarnDialog = registry.byId(this.id + "ErrWarnDialog");
                this.widget.ErrWarnDialogTextArea = registry.byId(this.id + "ErrWarnDialogTextArea");
            },

            extractGraphInfo: function (msg) {
                var retVal = {};
                var parts = msg.split("Graph graph");
                if (parts.length > 1) {
                    var parts1 = parts[1].split("[")
                    if (parts1.length > 1) {
                        retVal.graphID = "graph" + parts1[0];
                        parts1.shift();
                        var parts2 = parts1.join("[").split("], ");
                        retVal.subgraphID = parts2[0];
                        if (parts2.length > 1) {
                            var parts3 = parts2[1].split("[");
                            retVal.activityName = parts3[0];
                            if (parts3.length > 1) {
                                var parts4 = parts3[1].split("]");
                                retVal.activityID = parts4[0];
                            }
                        }
                    }
                }
                return retVal;
            },

            startup: function (args) {
                this.inherited(arguments);
                this.errorsCheckLabel = dom.byId(this.id + "ErrorsLabel");
                this.errorsCheckLabelOrigText = this.errorsCheckLabel.textContent;
                this.warningsCheckLabel = dom.byId(this.id + "WarningsLabel");
                this.warningsCheckLabelOrigText = this.warningsCheckLabel.textContent;
                this.infoCheckLabel = dom.byId(this.id + "InfoLabel");
                this.infoCheckLabelOrigText = this.infoCheckLabel.textContent;

                if (this.showToolbar) {
                    if (has("ie") <= 9 || has("ff")) {
                        this.widget.Download.set("disabled", true);
                        this.widget.Download.set("title", this.i18n.UnsupportedIE9FF);
                    }
                }

                var context = this;
                var store = new Memory({
                    idProperty: "id",
                    data: []
                });
                this.infoStore = Observable(store);

                this.infoGrid = new declare([ESPUtil.Grid(false, true)])({
                    selectionMode: "single",
                    columns: {
                        Severity: {
                            label: this.i18n.Severity, field: "", width: 72, sortable: false,
                            renderCell: function (object, value, node, options) {
                                switch (value) {
                                    case "Error":
                                        domClass.add(node, "ErrorCell");
                                        break;
                                    case "Alert":
                                        domClass.add(node, "AlertCell");
                                        break;
                                    case "Warning":
                                        domClass.add(node, "WarningCell");
                                        break;
                                }
                                node.innerText = value;
                            }
                        },
                        Source: { label: this.i18n.Source, field: "", width: 144, sortable: false },
                        Code: { label: this.i18n.Code, field: "", width: 45, sortable: false },
                        Message: {
                            label: this.i18n.Message, field: "",
                            sortable: false,
                            formatter: function (Message, idx) {
                                var info = context.extractGraphInfo(Message);
                                if (info.graphID && info.subgraphID && info.activityID) {
                                    var txt = "Graph " + info.graphID + "[" + info.subgraphID + "], " + info.activityName + "[" + info.activityID + "]";
                                    Message = Message.replace(txt, "<a href='#' class='dgrid-row-url'>" + txt + "</a>")
                                } else if (info.graphID && info.subgraphID) {
                                    var txt = "Graph " + info.graphID + "[" + info.subgraphID + "]";
                                    Message = Message.replace(txt, "<a href='#' class='dgrid-row-url'>" + txt + "</a>")
                                } else {
                                    Message = Utility.xmlEncode2(Message);
                                }
                                return Message;
                            }
                        },
                        Column: { label: this.i18n.Col, field: "", width: 36, sortable: false },
                        LineNo: { label: this.i18n.Line, field: "", width: 36, sortable: false },
                        FileName: { label: this.i18n.FileName, field: "", width: 360, sortable: false }
                    },
                    store: this.infoStore
                }, this.id + "InfoGrid");

                this.infoGrid.on(".dgrid-row:click", function (evt) {
                    var item = context.infoGrid.row(evt).data;
                    var line = parseInt(item.LineNo, 10);
                    var col = parseInt(item.Column, 10);
                    context.onErrorClick(line, col);
                });
                this.infoGrid.on(".dgrid-row-url:click", function (evt) {
                    var item = context.infoGrid.row(evt).data;
                    var info = context.extractGraphInfo(item.Message);
                    window.open("/?Widget=GraphTreeWidget&Wuid=" + context.wu.Wuid + "&GraphName=" + info.graphID + "&SubGraphId=" + info.subgraphID + "&ActivityId=" + info.activityID, "_blank");
                });
                this.infoGrid.startup();

                if (this.errWarn) {
                    this.infoData = [];
                    topic.subscribe("hpcc/brToaster", function (topic) {
                        context.loadTopic(topic);
                        context.refreshTopics(topic);
                    });

                    var target = dom.byId("stubMore");
                    if (target) {
                        this.errWarnCount = domConstruct.create("div", {
                            style: {
                                position: "relative",
                                top: "16px",
                                left: "12px",
                                color: "white"
                            }
                        }, target.firstChild);
                    }
                }
            },

            destroy: function (args) {
                this.widget.ErrWarnDialog.destroyRecursive();
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            onErrorClick: function (line, col) {
            },

            _onClear: function (evt) {
                this.infoData = [];
                this.refreshTopics();
            },

            toCSVCell: function (str) {
                str = "" + str;
                var mustQuote = (str.indexOf(",") >= 0 || str.indexOf("\"") >= 0 || str.indexOf("\r") >= 0 || str.indexOf("\n") >= 0);
                if (mustQuote) {
                    var retVal = "\"";
                    for (var i = 0; i < str.length; ++i) {
                        var c = str.charAt(i);
                        retVal += c === "\"" ? "\"\"" : c;

                    }
                    retVal += "\"";
                    return retVal;
                }
                return str;
            },
            csvFormatHeader: function (data, delim) {
                var retVal = ""
                if (data.length) {
                    for (var key in data[0]) {
                        if (retVal.length)
                            retVal += delim;
                        retVal += key;
                    }
                }
                return retVal;
            },
            csvFormatRow: function (row, idx, delim) {
                var retVal = ""
                for (var key in row) {
                    if (retVal.length)
                        retVal += delim;
                    retVal += this.toCSVCell(row[key]);
                }
                return retVal;
            },
            csvFormatFooter: function (data) {
                return "";
            },
            toCSV: function (data, delim) {
                var retVal = this.csvFormatHeader(data, delim) + "\n";
                var context = this;
                arrayUtil.forEach(data, function (item, idx) {
                    retVal += context.csvFormatRow(item, idx, delim) + "\n";
                });
                retVal += this.csvFormatFooter(data);
                return retVal;
            },
            _onCopy: function (evt) {
                var csvContent = this.toCSV(this.infoData, "\t");
                this.widget.ErrWarnDialogTextArea.set("value", csvContent);
                this.widget.ErrWarnDialog.show();
                this.widget.ErrWarnDialogTextArea.focus();
                this.widget.ErrWarnDialogTextArea.domNode.select();
            },
            _onDownload: function (evt) {
                var csvContent = this.toCSV(this.infoData, ",");
                var encodedUri = "data:text/csv;charset=utf-8,\uFEFF" + encodeURI(csvContent);
                if (navigator.msSaveBlob) {
                    var blob = new Blob([csvContent], {
                        type: "text/csv;charset=utf-8;"
                    });
                    navigator.msSaveBlob(blob, "ErrWarn.csv")
                } else {
                    var link = document.createElement("a");
                    link.setAttribute("href", encodedUri);
                    link.appendChild(document.createTextNode("ErrWarn.csv"));
                    link.click();
                }
            },

            _onErrors: function (args) {
                this.refreshFilter();
            },

            _onWarnings: function (args) {
                this.refreshFilter();
            },

            _onInfo: function (args) {
                this.refreshFilter();
            },

            _onErrWarnDialogClose: function (evt) {
                this.widget.ErrWarnDialog.hide();
            },

            //  Plugin wrapper  ---
            _onStyleRow: function (row) {
                var item = this.infoGrid.getItem(row.index);
                if (item) {
                    var severity = this.store.getValue(item, "Severity", null);
                    if (severity === "Error") {
                        row.customStyles += "background-color: red;";
                    } else if (severity === "Alert") {
                        row.customStyles += "background-color: #febe47;";
                    } else if (severity === "Warning") {
                        row.customStyles += "background-color: yellow;";
                    }
                }
                this.infoGrid.focus.styleRow(row);
                this.infoGrid.edit.styleRow(row);
            },

            reset: function () {
                this.initalized = false;
                this.params = null;
                this.wu = null;
                this.loadExceptions([]);
            },

            init: function (params) {
                if (this.inherited(arguments))
                    return;

                if (params.onErrorClick) {
                    this.onErrorClick = params.onErrorClick;
                }

                if (params.Wuid) {
                    this.wu = ESPWorkunit.Get(params.Wuid);

                    var context = this;
                    this.wu.monitor(function () {
                        context.wu.getInfo({
                            onGetWUExceptions: function (exceptions) {
                                context.loadExceptions(exceptions);
                            }
                        });
                    });
                    this.wu.watch(function (name, oldValue, newValue) {
                        if (name === "Exceptions") {
                            context.loadExceptions(newValue.ECLException);
                        }
                    });
                }
            },

            refreshFilter: function () {
                var data = [];
                var filter = "";
                var errorChecked = this.errorsCheck.get("checked");
                var warningChecked = this.warningsCheck.get("checked");
                var infoChecked = this.infoCheck.get("checked");
                this._counts = {
                    error: 0,
                    warning: 0,
                    errorWarning: 0,
                    info: 0,
                    alert: 0
                };
                arrayUtil.forEach(this.infoData, function (item, idx) {
                    lang.mixin(item, {
                        id: idx
                    });
                    switch (item.Severity) {
                        case "Error":
                            this._counts.error++;
                            this._counts.errorWarning++;
                            if (errorChecked) {
                                data.push(item);
                            }
                            break;
                        case "Warning":
                            this._counts.warning++;
                            this._counts.errorWarning++;
                            if (warningChecked) {
                                data.push(item);
                            }
                            break;
                        case "Message":
                        case "Info":
                            this._counts.info++;
                            if (infoChecked) {
                                data.push(item);
                            }
                            break;
                        case "Alert":
                            this._counts.alert++;
                            if (errorChecked) {
                                data.push(item);
                            }
                            break;
                    }
                }, this);
                this.infoStore.setData(data);
                this.infoGrid.refresh();
                this.errorsCheckLabel.innerText = this._counts.error + " " + this.errorsCheckLabelOrigText;
                this.warningsCheckLabel.innerText = this._counts.warning + " " + this.warningsCheckLabelOrigText;
                this.infoCheckLabel.innerText = this._counts.info + " " + this.infoCheckLabelOrigText;
            },

            getSelected: function () {
                return this.infoGrid.selection.getSelected();
            },

            setSelected: function (selItems) {
                for (var i = 0; i < this.infoGrid.rowCount; ++i) {
                    var row = this.infoGrid.getItem(i);
                    this.infoGrid.selection.setSelected(i, (row.SubGraphId && arrayUtil.indexOf(selItems, row.SubGraphId) !== -1));
                }
            },

            loadExceptions: function (exceptions) {
                exceptions.sort(function (l, r) {
                    if (l.Severity === r.Severity) {
                        return 0;
                    } else if (l.Severity === "Error") {
                        return -1;
                    } else if (r.Severity === "Error") {
                        return 1;
                    } else if (l.Severity === "Alert") {
                        return -1;
                    } else if (r.Severity === "Alert") {
                        return 1;
                    } else if (l.Severity === "Warning") {
                        return -1;
                    } else if (r.Severity === "Warning") {
                        return 1;
                    }
                    return l.Severity > r.Severity;
                });
                this.infoData = exceptions;
                this.refreshFilter();
            },

            loadTopic: function (topic, toEnd) {
                if (lang.exists("Exceptions", topic)) {
                    var context = this;
                    arrayUtil.forEach(topic.Exceptions, function (item, idx) {
                        var errWarnRow = lang.mixin({
                            Severity: topic.Severity,
                            Source: topic.Source
                        }, item);
                        if (toEnd) {
                            context.infoData.push(errWarnRow);
                        } else {
                            context.infoData.unshift(errWarnRow);
                        }
                    });
                }
            },

            refreshTopics: function () {
                this.refreshFilter();
                if (this.errWarnCount) {
                    this.errWarnCount.textContent = this._counts.errorWarning > 0 ? this._counts.errorWarning : "";
                }
                if (this.errWarnMenuItem) {
                    this.errWarnMenuItem.set("label", this.i18n.ErrorWarnings + (this._counts.errorWarning > 0 ? " (" + this._counts.errorWarning + ")" : ""));
                }
            }
        });
    });
