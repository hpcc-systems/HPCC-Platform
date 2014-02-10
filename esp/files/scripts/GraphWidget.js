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
    "dojo/i18n!./nls/common",
    "dojo/i18n!./nls/GraphWidget",
    "dojo/aspect",
    "dojo/has",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-class",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",

    "dojo/text!/esp/files/templates/GraphWidget.html",

    "dijit/Toolbar", 
    "dijit/ToolbarSeparator", 
    "dijit/form/Button"
    
], function (declare, lang, i18n, nlsCommon, nlsSpecific, aspect, has, dom, domConstruct, domClass,
            _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, ContentPane,
            template) {
        return declare("GraphWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "GraphWidget",
            i18n: lang.mixin(nlsCommon, nlsSpecific),

            borderContainer: null,
            graphContentPane: null,
            _isPluginInstalled: false,
            _plugin: null,
            eventsRegistered: false,
            xgmml: "",
            dot: "",
            svg: "",

            isIE11: false,
            isIE: false,

            //  Known control properties  ---
            DOT_META_ATTR: "DOT_META_ATTR",

            constructor: function() {
                if (has("ie")) {
                    this.isIE = true;
                } else if (has("trident")) {
                    this.isIE11 = true;
                }
            },

            _onClickRefresh: function () {
                this.setMessage(this.i18n.PerformingLayout);
                this.startLayout("dot");
            },

            _onClickZoomOrig: function (args) {
                this.setScale(100);
                this.centerOnItem(0);
            },

            _onClickZoomAll: function (args) {
                this.centerOnItem(0, true);
            },

            _onClickZoomWidth: function (args) {
                this.centerOnItem(0, true, true);
            },

            onSelectionChanged: function (items) {
            },

            onDoubleClick: function (globalID) {
            },

            onLayoutFinished: function () {
            },

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = dijit.byId(this.id + "BorderContainer");
                this.graphContentPane = dijit.byId(this.id + "GraphContentPane");
            },

            startup: function (args) {
                this.inherited(arguments);
                this._isPluginInstalled = this.isPluginInstalled();
                this.createPlugin();
                this.watchStyleChange();
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            showToolbar: function (show) {
                if (show) {
                    domClass.remove(this.id + "Toolbar", "hidden");
                } else {
                    domClass.add(this.id + "Toolbar", "hidden");
                }
                this.resize();
            },

            clear: function () {
                if (this._plugin) {
                    this.xgmml = "";
                    this.svg = "";
                    this.dot = "";
                    this._plugin.clear();
                }
            },

            loadXGMML: function (xgmml, merge) {
                this.registerEvents();
                if (this._plugin && this.xgmml != xgmml) {
                    this.setMessage(this.i18n.LoadingData);
                    if (merge)
                        this._plugin.mergeXGMML(xgmml);
                    else
                        this._plugin.loadXGMML(xgmml);
                    this.setMessage(this.i18n.PerformingLayout);
                    this._plugin.startLayout("dot");
                    this.xgmml = xgmml;
                }
            },

            mergeXGMML: function (xgmml) {
                this.registerEvents();
                if (this._plugin && this.xgmml != xgmml) {
                    this._plugin.mergeXGMML(xgmml);
                    this.xgmml = xgmml;
                }
            },

            loadDOT: function (dot) {
                this.registerEvents();
                this.load(dot, "dot");
            },

            load: function (dot, layout) {
                this.registerEvents();
                if (this._plugin && this.dot != dot) {
                    this.setMessage(this.i18n.LoadingData);
                    this._plugin.loadDOT(dot);
                    this.setMessage(this.i18n.PerformingLayout);
                    this._plugin.startLayout(layout);
                    this.dot = dot;
                }
            },

            setLayout: function (layout) {
                if (this._plugin) {
                    this.setMessage(this.i18n.PerformingLayout);
                    this._plugin.startLayout(layout);
                }
            },

            centerOn: function (globalID) {
                if (this._plugin) {
                    var item = this._plugin.getItem(globalID);
                    this._plugin.centerOnItem(item, true);
                    var items = [item];
                    this._plugin.setSelected(items, true);
                }
            },

            getVersion: function () {
                this.registerEvents();
                if (this._plugin) {
                    return this._plugin.version;
                }
                return "";
            },

            getSVG: function () {
                return this._plugin.getSVG();
            },

            getXGMML: function () {
                return this.xgmml;
            },

            localLayout: function(callback) {
                var context = this;
                require(
                  ["hpcc/viz"],
                  function (viz) {
                      callback(Viz(context.dot, "svg"));
                  }
                );
            },

            displayProperties: function (globalID, place) {
                if (this._plugin) {
                    var item = this._plugin.getItem(globalID);
                    var props = this._plugin.getProperties(item);
                    if (props.id) {
                        var table = domConstruct.create("h3", {
                            innerHTML: props.id,
                            align: "center"
                        }, place);
                        delete props.id;
                    }
                    if (props.count) {
                        var table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, place);
                        var tr = domConstruct.create("tr", null, table);
                        var td = domConstruct.create("td", { innerHTML: this.i18n.Count }, tr);
                        var td = domConstruct.create("td", {
                            align: "right",
                            innerHTML: props.count
                        }, tr);
                        delete props.count;
                        domConstruct.create("br", null, place);
                    }
                    if (props.max) {
                        var table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, place);
                        var tr = domConstruct.create("tr", null, table);
                        domConstruct.create("th", { innerHTML: "    " }, tr);
                        domConstruct.create("th", { innerHTML: this.i18n.Skew }, tr);
                        domConstruct.create("th", { innerHTML: this.i18n.Node }, tr);
                        domConstruct.create("th", { innerHTML: this.i18n.Rows }, tr);
                        tr = domConstruct.create("tr", null, table);
                        domConstruct.create("td", { innerHTML: this.i18n.Max }, tr);
                        domConstruct.create("td", { innerHTML: props.maxskew }, tr);
                        domConstruct.create("td", { innerHTML: props.maxEndpoint }, tr);
                        domConstruct.create("td", { innerHTML: props.max }, tr);
                        tr = domConstruct.create("tr", null, table);
                        domConstruct.create("td", { innerHTML: this.i18n.Min }, tr);
                        domConstruct.create("td", { innerHTML: props.minskew }, tr);
                        domConstruct.create("td", { innerHTML: props.minEndpoint }, tr);
                        domConstruct.create("td", { innerHTML: props.min }, tr);
                        delete props.maxskew;
                        delete props.maxEndpoint;
                        delete props.max;
                        delete props.minskew;
                        delete props.minEndpoint;
                        delete props.min;
                        domConstruct.create("br", null, place);
                    }
                    if (props.slaves) {
                        var table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, place);
                        var tr = domConstruct.create("tr", null, table);
                        domConstruct.create("th", { innerHTML: this.i18n.Slaves }, tr);
                        domConstruct.create("th", { innerHTML: this.i18n.Started }, tr);
                        domConstruct.create("th", { innerHTML: this.i18n.Stopped }, tr);
                        tr = domConstruct.create("tr", null, table);
                        domConstruct.create("td", { innerHTML: props.slaves }, tr);
                        domConstruct.create("td", { innerHTML: props.started }, tr);
                        domConstruct.create("td", { innerHTML: props.stopped }, tr);
                        delete props.slaves;
                        delete props.started;
                        delete props.stopped;
                        domConstruct.create("br", null, place);
                    }
                    var first = true;
                    var table = {};
                    var tr = {};
                    for (var key in props) {
                        if (key[0] == "_")
                            continue;

                        if (first) {
                            first = false;
                            table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, place);
                            tr = domConstruct.create("tr", null, table);
                            domConstruct.create("th", { innerHTML: this.i18n.Property }, tr);
                            domConstruct.create("th", { innerHTML: this.i18n.Value }, tr);
                        }
                        tr = domConstruct.create("tr", null, table);
                        domConstruct.create("td", { innerHTML: key }, tr);
                        domConstruct.create("td", { innerHTML: props[key] }, tr);
                    }
                    if (first == false) {
                        domConstruct.create("br", null, place);
                    }
                }
            },

            isPluginInstalled: function () {
                if (this.isIE || this.isIE11) {
                    try {
                        var o = new ActiveXObject("HPCCSystems.HPCCSystemsGraphViewControl.1");
                        o = null;
                        return true;
                    } catch (e) { }
                    return false;
                } else {
                    for (var i = 0, p = navigator.plugins, l = p.length; i < l; i++) {
                        if (p[i].name.indexOf("HPCCSystemsGraphViewControl") > -1) {
                            return true;
                        }
                    }
                    return false;
                }
            },

            createPlugin: function () {
                if (this._plugin == null) {
                    if (this._isPluginInstalled) {
                        var pluginID = this.id + "Plugin";
                        if (this.isIE || this.isIE11) {
                            this.graphContentPane.domNode.innerHTML = '<object type="application/x-hpccsystemsgraphviewcontrol" '
                                                    + 'id="' + pluginID + '" '
                                                    + 'name="' + pluginID + '" '
                                                    + 'width="100%" '
                                                    + 'height="100%">'
                                                    + '</object>';
                        } else {
                            this.graphContentPane.domNode.innerHTML = '<embed type="application/x-hpccsystemsgraphviewcontrol" '
                                                    + 'id="' + pluginID + '" '
                                                    + 'name="' + pluginID + '" '
                                                    + 'width="100%" '
                                                    + 'height="100%">'
                                                    + '</embed>';
                        }
                        this._plugin = dom.byId(pluginID);
                        var context = this;
                    } else {
                        domConstruct.create("div", {
                            innerHTML: "<h4>" + this.i18n.GraphView + "</h4>" +
                                        "<p>" + this.i18n.Toenablegraphviews + ":</p>" +
                                        this.getResourceLinks()
                        }, this.graphContentPane.domNode);
                    }
                }
            },

            getResourceLinks: function () {
                return "<a href=\"http://hpccsystems.com/download/free-community-edition/graph-control\" target=\"_blank\">" + this.i18n.BinaryInstalls + "</a><br/>" +
                "<a href=\"https://github.com/hpcc-systems/GraphControl\" target=\"_blank\">" + this.i18n.SourceCode + "</a><br/><br/>" +
                "<a href=\"http://hpccsystems.com\" target=\"_blank\">" + this.i18n.HPCCSystems + "</a>"
            },

            setMessage: function (message) {
                if (this._plugin) {
                    return this._plugin.setMessage(message);
                }
                return null;
            },

            getLocalisedXGMML: function (items, depth, distance) {
                if (this._plugin) {
                    return this._plugin.getLocalisedXGMML(items, depth, distance);
                }
                return null;
            },

            mergeSVG: function (svg) {
                if (this._plugin) {
                    return this._plugin.mergeSVG(svg);
                }
                return null;
            },

            startLayout: function (layout) {
                if (this._plugin) {
                    return this._plugin.startLayout(layout);
                }
                return null;
            },

            find: function (findText) {
                if (this._plugin) {
                    return this._plugin.find(findText);
                }
                return [];
            },

            findAsGlobalID: function (findText) {
                if (this._plugin) {
                    var items = this.find(findText);
                    var globalIDs = [];
                    for (var i = 0; i < items.length; ++i) {
                        globalIDs.push(this._plugin.getGlobalID(items[i]));
                    }
                    return globalIDs;
                }
                return [];
            },

            setScale: function(percent) {
                if (this._plugin) {
                    return this._plugin.setScale(percent);
                }
                return 100;
            },

            centerOnItem: function (item, scaleToFit, widthOnly) {
                if (this._plugin) {
                    return this._plugin.centerOnItem(item, scaleToFit, widthOnly);
                }
                return null;
            },

            centerOnGlobalID: function (globalID, scaleToFit, widthOnly) {
                if (this._plugin) {
                    var item = this._plugin.getItem(globalID);
                    if (item) {
                        return this.centerOnItem(item, scaleToFit, widthOnly);
                    }
                }
                return null;
            },

            setSelected: function (items) {
                if (this._plugin) {
                    return this._plugin.setSelected(items);
                }
                return null;
            },

            setSelectedAsGlobalID: function (items) {
                if (this._plugin) {
                    return this._plugin.setSelectedAsGlobalID(items);
                }
                return null;
            },

            getSelection: function () {
                if (this._plugin) {
                    return this._plugin.getSelection();
                }
                return [];
            },

            getSelectionAsGlobalID: function () {
                if (this._plugin) {
                    return this._plugin.getSelectionAsGlobalID();
                }
                return [];
            },

            getItem: function (globalID) {
                if (this._plugin) {
                    return this._plugin.getItem(globalID);
                }
                return null;
            },

            hide: function () {
                if (this._plugin) {
                    dojo.style(this._plugin, "width", "1px");
                    dojo.style(this._plugin, "height", "1px");
                }
            },

            show: function () {
                if (this._plugin) {
                    dojo.style(this._plugin, "width", "100%");
                    dojo.style(this._plugin, "height", "100%");
                }
            },

            watchSplitter: function (splitter) {
                if (has("chrome")) {
                    //  Chrome can ignore splitter events
                    return;
                }
                var context = this;
                dojo.connect(splitter, "_startDrag", function () {
                    context.hide();
                });
                dojo.connect(splitter, "_stopDrag", function (evt) {
                    context.show();
                });
            },

            watchSelect: function (select) {
                if (select) {
                    //  Only chrome needs to monitor select drop downs.
                    var context = this;
                    select.watch("_opened", function () {
                        if (select._opened) {
                            context.hide();
                        } else {
                            context.show();
                        }
                    });
                }
            },

            watchStyleChange: function () {
                //  Prevent control from being "hidden" as it gets destroyed on Chrome/FF/(Maybe IE11?)
                var watchList = [];
                var context = this;
                var domNode = this.domNode;

                //  There are many places that may cause the plugin to be hidden, the possible places are calculated by walking the hierarchy upwards. 
                while (domNode) {
                    if (domNode.id) {
                        watchList[domNode.id] = false;
                    }
                    domNode = domNode.parentElement;
                }

                //  Hijack the dojo style class replacement call and monitor for elements in our watchList. 
                aspect.around(domClass, "replace", function (origFunc) {
                    return function (node, addStyle, removeStyle) {
                        if (node.firstChild && (node.firstChild.id in watchList)) {
                            if (addStyle == "dijitHidden") {
                                watchList[node.firstChild.id] = true;
                                dojo.style(node, "width", "1px");
                                dojo.style(node, "height", "1px");
                                dojo.style(node.firstChild, "width", "1px");
                                dojo.style(node.firstChild, "height", "1px");
                                return;
                            } else if (addStyle == "dijitVisible" && watchList[node.firstChild.id] == true) {
                                watchList[node.firstChild.id] = false;
                                dojo.style(node, "width", "100%");
                                dojo.style(node, "height", "100%");
                                dojo.style(node.firstChild, "width", "100%");
                                dojo.style(node.firstChild, "height", "100%");
                                return;
                            }
                        }
                        return origFunc(node, addStyle, removeStyle);
                    }
                });
            },

            getDotMetaAttributes: function () {
                if (this._plugin && this._plugin.getControlProperty) {
                    return this._plugin.getControlProperty(this.DOT_META_ATTR);
                }
                return "";
            },

            setDotMetaAttributes: function (dotMetaAttr) {
                if (this._plugin && this._plugin.setControlProperty) {
                    this._plugin.setControlProperty(this.DOT_META_ATTR, dotMetaAttr);
                }
            },

            getProperty: function (item, key) {
                if (this._plugin && this._plugin.getProperty) {
                    return this._plugin.getProperty(item, key);
                }
                return "";
            },

            setProperty: function (item, key, value) {
                if (this._plugin && this._plugin.setProperty) {
                    this._plugin.setProperty(item, key, value);
                }
            },

            getProperties: function (item) {
                if (this._plugin) {
                    return this._plugin.getProperties(item);
                }
                return [];
            },

            getSubgraphsWithProperties: function () {
                if (this._plugin) {
                    return this._plugin.getSubgraphsWithProperties();
                }
                return [];
            },

            getVerticesWithProperties: function () {
                if (this._plugin) {
                    return this._plugin.getVerticesWithProperties();
                }
                return [];
            },

            getEdgesWithProperties: function () {
                if (this._plugin) {
                    return this._plugin.getEdgesWithProperties();
                }
                return [];
            },

            registerEvents: function () {
                if (!this.eventsRegistered) {
                    this.eventsRegistered = true;
                    var context = this;
                    this.registerEvent("MouseDoubleClick", function (item) {
                        context.onDoubleClick(context._plugin.getGlobalID(item));
                    });
                    this.registerEvent("LayoutFinished", function () {
                        context._plugin.centerOnItem(0, true);
                        context.setMessage('');
                        context.dot = context._plugin.getDOT();
                        context.svg = context._plugin.getSVG();
                        context.onLayoutFinished();
                    });
                    this.registerEvent("SelectionChanged", function (items) {
                        context.onSelectionChanged(items);
                    });
                }
            },

            registerEvent: function (evt, func) {
                if (this._plugin) {
                    if (this._plugin.attachEvent !== undefined) {
                        return this._plugin.attachEvent("on" + evt, func);
                    } else {
                        return this._plugin.addEventListener(evt, func, false);
                    }
                }
                return false;
            }
        });
    });
