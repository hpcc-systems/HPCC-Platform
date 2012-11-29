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
require([
    "dojo/_base/declare",
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

    "dojo/text!./templates/GraphWidget.html",

    "dijit/Toolbar", "dijit/ToolbarSeparator", "dijit/form/Button"
],
    function (declare, aspect, has, dom, domConstruct, domClass,
            _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, BorderContainer, ContentPane,
            template) {
        return declare("GraphWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
            templateString: template,
            baseClass: "GraphWidget",
            borderContainer: null,
            graphContentPane: null,
            _isPluginInstalled: false,
            plugin: null,
            eventsRegistered: false,
            xgmml: "",
            dot: "",
            svg: "",

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
            clear: function () {
                if (this.plugin) {
                    this.plugin.clear();
                }
            },

            loadXGMML: function (xgmml, merge) {
                this.registerEvents();
                if (this.plugin && this.xgmml != xgmml) {
                    this.setMessage("Loading Data...");
                    if (merge)
                        this.plugin.mergeXGMML(xgmml);
                    else
                        this.plugin.loadXGMML(xgmml);
                    this.setMessage("Performing Layout...");
                    this.plugin.startLayout("dot");
                    this.xgmml = xgmml;
                }
            },

            mergeXGMML: function (xgmml) {
                this.registerEvents();
                if (this.plugin && this.xgmml != xgmml) {
                    this.plugin.mergeXGMML(xgmml);
                    this.xgmml = xgmml;
                }
            },

            loadDOT: function (dot) {
                this.registerEvents();
                this.load(dot, "dot");
            },

            load: function (dot, layout) {
                this.registerEvents();
                if (this.plugin && this.xgmml != xgmml) {
                    this.setMessage("Loading Data...");
                    this.plugin.loadDOT(dot);
                    this.setMessage("Performing Layout...");
                    this.plugin.startLayout(layout);
                    this.xgmml = xgmml;
                }
            },

            setLayout: function (layout) {
                if (this.plugin) {
                    this.setMessage("Performing Layout...");
                    this.plugin.startLayout(layout);
                }
            },

            centerOn: function (globalID) {
                var item = this.plugin.getItem(globalID);
                this.plugin.centerOnItem(item, true);
                var items = [item];
                this.plugin.setSelected(items, true);
            },

            getVersion: function () {
                return this.plugin.version;
            },

            displayProperties: function (item, place) {
                var props = this.plugin.getProperties(item);
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
                    var td = domConstruct.create("td", { innerHTML: "Count" }, tr);
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
                    domConstruct.create("th", { innerHTML: "Skew" }, tr);
                    domConstruct.create("th", { innerHTML: "Node" }, tr);
                    domConstruct.create("th", { innerHTML: "Rows" }, tr);
                    tr = domConstruct.create("tr", null, table);
                    domConstruct.create("td", { innerHTML: "Max" }, tr);
                    domConstruct.create("td", { innerHTML: props.maxskew }, tr);
                    domConstruct.create("td", { innerHTML: props.maxEndpoint }, tr);
                    domConstruct.create("td", { innerHTML: props.max }, tr);
                    tr = domConstruct.create("tr", null, table);
                    domConstruct.create("td", { innerHTML: "Min" }, tr);
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
                    domConstruct.create("th", { innerHTML: "Slaves" }, tr);
                    domConstruct.create("th", { innerHTML: "Started" }, tr);
                    domConstruct.create("th", { innerHTML: "Stopped" }, tr);
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
                for (key in props) {
                    if (key[0] == "_")
                        continue;

                    if (first) {
                        first = false;
                        table = domConstruct.create("table", { border: 1, cellspacing: 0, width: "100%" }, place);
                        tr = domConstruct.create("tr", null, table);
                        domConstruct.create("th", { innerHTML: "Property" }, tr);
                        domConstruct.create("th", { innerHTML: "Value" }, tr);
                    }
                    tr = domConstruct.create("tr", null, table);
                    domConstruct.create("td", { innerHTML: key }, tr);
                    domConstruct.create("td", { innerHTML: props[key] }, tr);
                }
                if (first == false) {
                    domConstruct.create("br", null, place);
                }
            },

            isPluginInstalled: function () {
                if (has("ie")) {
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
                var pluginID = this.id + "Plugin";
                if (this.plugin == null) {
                    if (this._isPluginInstalled) {
                        if (has("ie")) {
                            this.graphContentPane.domNode.innerHTML = '<object '
                                                    + 'type="application/x-hpccsystemsgraphviewcontrol" '
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
                        this.plugin = dom.byId(pluginID);
                        var context = this;
                    } else {
                        domConstruct.create("div", {
                            innerHTML: "<h4>Graph View</h4>" +
                                        "<p>To enable graph views, please install the Graph View Control plugin:</p>" +
                                        this.getResourceLinks()
                        }, this.graphContentPane.domNode);
                    }
                }
            },

            getResourceLinks: function () {
                return "<a href=\"http://hpccsystems.com/download/free-community-edition/graph-control\" target=\"_blank\">Binary Installs</a><br>" +
                "<a href=\"https://github.com/hpcc-systems/GraphControl\" target=\"_blank\">Source Code</a><br><br>" + 
                "<a href=\"http://hpccsystems.com\" target=\"_blank\">HPCC Systems</a>"
            },

            setMessage: function (message) {
                if (this.plugin) {
                    return this.plugin.setMessage(message);
                }
                return null;
            },

            getLocalisedXGMML: function (items) {
                if (this.plugin) {
                    return this.plugin.getLocalisedXGMML(items);
                }
                return null;
            },

            mergeSVG: function (svg) {
                if (this.plugin) {
                    return this.plugin.mergeSVG(svg);
                }
                return null;
            },

            startLayout: function (layout) {
                if (this.plugin) {
                    return this.plugin.startLayout(layout);
                }
                return null;
            },

            find: function (findText) {
                if (this.plugin) {
                    return this.plugin.find(findText);
                }
                return [];
            },

            centerOnItem: function (item, scaleToFit, widthOnly) {
                if (this.plugin) {
                    return this.plugin.centerOnItem(item, scaleToFit, widthOnly);
                }
                return null;
            },

            setSelected: function (items) {
                if (this.plugin) {
                    return this.plugin.setSelected(items);
                }
                return null;
            },

            setSelectedAsGlobalID: function (items) {
                if (this.plugin) {
                    return this.plugin.setSelectedAsGlobalID(items);
                }
                return null;
            },

            getSelectionAsGlobalID: function () {
                if (this.plugin) {
                    return this.plugin.getSelectionAsGlobalID();
                }
                return [];
            },

            getItem: function (globalID) {
                if (this.plugin) {
                    return this.plugin.getItem(globalID);
                }
                return null;
            },

            hide: function () {
                if (this.plugin) {
                    dojo.style(this.plugin, "width", "1px");
                    dojo.style(this.plugin, "height", "1px");
                }
            },

            show: function () {
                if (this.plugin) {
                    dojo.style(this.plugin, "width", "100%");
                    dojo.style(this.plugin, "height", "100%");
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
                if (has("chrome") && select) {
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
                //  When chrome hides the plugin it destroys it.  To prevent this it is just made very small.  
                if (has("chrome")) {
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
                            if (node.id in watchList) {
                                if (addStyle == "dijitHidden") {
                                    watchList[node.id] = true;
                                    dojo.style(node, "width", "1px");
                                    dojo.style(node, "height", "1px");
                                    return;
                                } else if (addStyle == "dijitVisible" && watchList[node.id] == true) {
                                    watchList[node.id] = false;
                                    dojo.style(node, "width", "100%");
                                    dojo.style(node, "height", "100%");
                                    return;
                                }
                            }
                            return origFunc(node, addStyle, removeStyle);
                        }
                    });
                }
            },

            registerEvents: function () {
                if (!this.eventsRegistered) {
                    this.eventsRegistered = true;
                    var context = this;
                    this.registerEvent("MouseDoubleClick", function (item) {
                        context.plugin.centerOnItem(item, true);
                        context.onDoubleClick(context.plugin.getGlobalID(item));
                    });
                    this.registerEvent("LayoutFinished", function () {
                        context.plugin.centerOnItem(0, true);
                        context.setMessage('');
                        context.dot = context.plugin.getDOT();
                        context.svg = context.plugin.getSVG();
                        context.onLayoutFinished();
                    });
                    this.registerEvent("SelectionChanged", function (items) {
                        context.onSelectionChanged(items);
                    });
                }
            },

            registerEvent: function (evt, func) {
                if (this.plugin) {
                    if (this.plugin.attachEvent) {
                        return this.plugin.attachEvent("on" + evt, func);
                    } else {
                        return this.plugin.addEventListener(evt, func, false);
                    }
                }
                return false;
            }

        });
    });
