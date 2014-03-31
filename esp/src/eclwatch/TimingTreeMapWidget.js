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
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/store/Memory",
    "dojo/dom",
    "dojo/dom-class",

    "dijit/registry",

    "dojox/treemap/TreeMap",

    "hpcc/_Widget",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/TimingTreeMapWidget.html"
],
    function (declare, lang, i18n, nlsHPCC, arrayUtil, Memory, dom, domClass,
            registry, 
            TreeMap,
            _Widget, ESPWorkunit,
            template) {
        return declare("TimingTreeMapWidget", [_Widget], {
            templateString: template,
            baseClass: "TimingTreeMapWidget",
            i18n: nlsHPCC,

            treeMap: null,
            store: null,

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.treeMap = registry.byId(this.id + "TreeMap");

                var context = this;
                this.treeMap.on("click", function (evt) {
                    context.onClick(context.treeMap.selectedItems);
                });
                this.treeMap.on("dblclick", function (evt) {
                    context.onDblClick(context.treeMap.selectedItem);
                });
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            resize: function (args) {
                this.inherited(arguments);
                this.treeMap._dataChanged = true;
                this.treeMap.resize(args);
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            onClick: function (value) {
            },

            onDblClick: function (value) {
            },

            init: function (params) {
                if (this.inherited(arguments))
                    return;

                if (params.hideHelp) {
                    domClass.add(this.id + "Help", "hidden");
                }

                var context = this;
                if (params.Wuid) {
                    this.wu = ESPWorkunit.Get(params.Wuid);

                    this.wu.fetchTimers(function (timers) {
                        context.timers = timers;
                        context.loadTimers(timers, params.query);
                    });
                }
            },

            setQuery: function (query) {
                this.loadTimers(this.timers, query);
            },

            getSelected: function () {
                return this.treeMap.selectedItems;
            },

            setSelectedAsGlobalID: function (selItems) {
                if (this.store) {
                    var selectedItems = [];
                    for (var i = 0; i < selItems.length; ++i) {
                        var item = this.store.get(selItems[i]);
                        if (item) {
                            selectedItems.push(item);
                        }
                    }
                    try {  //  Throws an exception in IE 8
                        this.treeMap.set("selectedItems", selectedItems);
                    } catch (e) {
                    }
                }
            },

            setSelected: function (selItems) {
                if (this.store) {
                    var selectedItems = [];
                    for (var i = 0; i < selItems.length; ++i) {
                        var item = this.store.get(selItems[i].SubGraphId);
                        if (item) {
                            selectedItems.push(item);
                        }
                    }
                    try {  //  Throws an exception in IE 8
                        this.treeMap.set("selectedItems", selectedItems);
                    } catch (e) {
                    }
                }
            },

            setSelectedGraphs: function (selItems) {
                if (this.store) {
                    var selectedItems = [];
                    for (var i = 0; i < selItems.length; ++i) {
                        arrayUtil.forEach(this.store.data, function (item, idx) {
                            if (selItems[i].__hpcc_id && item.__hpcc_id == selItems[i].__hpcc_id) {
                                selectedItems.push(item);
                            } else if (item.GraphName == selItems[i].Name) {
                                selectedItems.push(item);
                            }
                        });
                    }
                    this.treeMap.set("selectedItems", selectedItems);
                }
            },

            loadTimers: function (timers, query) {
                this.largestValue = 0;
                var timerData = [];
                if (timers) {
                    for (var i = 0; i < timers.length; ++i) {
                        if (query.graphsOnly) {
                            if (timers[i].SubGraphId && (query.graphName === "*" || query.graphName === timers[i].GraphName) && (query.subGraphId === "*" || query.subGraphId === timers[i].SubGraphId)) {
                                timerData.push(lang.mixin({
                                    __hpcc_prefix: timers[i].GraphName
                                }, timers[i]));
                                if (this.largestValue < timers[i].Seconds * 1000) {
                                    this.largestValue = timers[i].Seconds * 1000;
                                }
                            }
                        } else if ( timers[i].Name != "Process" &&
                                    timers[i].Name != "Total thor time") {
                            var prefix = "other";
                            if (timers[i].Name.indexOf("Graph graph") == 0) {
                                if (!timers[i].SubGraphId) {
                                    continue;
                                }
                                prefix = timers[i].GraphName;
                            } else {
                                var nameParts = timers[i].Name.split(":");
                                if (nameParts.length > 1) {
                                    prefix = nameParts[0];
                                }
                            }
                            timerData.push(lang.mixin({
                                __hpcc_prefix: prefix
                            }, timers[i]));
                            if (this.largestValue < timers[i].Seconds * 1000) {
                                this.largestValue = timers[i].Seconds * 1000;
                            }
                        }
                    }
                }
                this.store = new Memory({
                    idProperty: "__hpcc_id",
                    data: timerData
                });

                var context = this;
                this.treeMap.set("store", this.store);
                this.treeMap.set("areaAttr", "Seconds");
                this.treeMap.set("colorFunc", function (item) {
                    var redness = Math.floor(255 * (item.Seconds * 1000 / context.largestValue));
                    return {
                        r: 255,
                        g: 255 - redness,
                        b: 255 - redness
                    };
                });
                this.treeMap.set("groupAttrs", ["__hpcc_prefix"]);
                this.treeMap.set("labelAttr", "Name");
                this.treeMap.set("tooltipFunc", function (item) {
                    return item.Name + " " + item.Seconds;
                });
            }
        });
    });
