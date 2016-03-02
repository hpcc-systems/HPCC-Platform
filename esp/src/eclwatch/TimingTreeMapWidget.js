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
    "dojo/dom",
    "dojo/dom-class",
    "dojo/dom-style",

    "dijit/registry",

    "dojox/treemap/TreeMap",

    "hpcc/_Widget",
    "hpcc/ESPWorkunit",

    "dojo/text!../templates/TimingTreeMapWidget.html"
],
    function (declare, lang, i18n, nlsHPCC, arrayUtil, Memory, dom, domClass, domStyle,
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

            calcHeight: function (elmID) {
                var elmHeight, elmMargin, elm = document.getElementById(elmID);
                var computedStyle = domStyle.getComputedStyle(elm);
                elmHeight = parseFloat(computedStyle.getPropertyValue("height"));
                elmMargin = parseFloat(computedStyle.getPropertyValue('margin-top')) + parseInt(computedStyle.getPropertyValue('margin-bottom'));
                return elmHeight + elmMargin;
            },

            resize: function (args) {
                this.inherited(arguments);
                var helpHeight = this.params.hideHelp ? 0 : this.calcHeight(this.id + "Help");
                args.h -= helpHeight + 2;
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
                    var monitorCount = 4;
                    this.wu.monitor(function () {
                        if (context.wu.isComplete() || ++monitorCount % 5 == 0) {
                            context.refreshTreeMap();
                        }
                    });
                }
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

            setActivities: function (activities) {
                var context = this;
                setTimeout(function () {
                    context.loadTimers(activities.map(function (activity) {
                        return {
                            __hpcc_prefix: "Activites",
                            __hpcc_id: activity._globalID,
                            ActivityID: activity._globalID,
                            Name: activity.label,
                            Seconds: activity.TimeMaxLocalExecute
                        };
                    }));
                }, 20);
            },

            refreshTreeMap: function () {
                var context = this;
                this.wu.fetchTimers(function (timers) {
                    context.timers = timers;
                    context.loadTimers(timers);
                });
            },

            timerFilter: function (timer) {
                if (lang.exists("params.query.graphsOnly", this) && this.params.query.graphsOnly) {
                    return (timer.SubGraphId && (this.params.query.graphName === "*" || this.params.query.graphName === timer.GraphName) && (this.params.query.subGraphId === "*" || this.params.query.subGraphId === timer.SubGraphId));
                }
                return (timer.Name != "Process" &&
                        timer.Name != "compile" &&
                        timer.Name != "Total thor time" &&
                        timer.Name != "Total cluster time" &&
                        timer.Name.indexOf(":TimeElapsed") < 0);
            },

            loadTimers: function (_timers) {
                var context = this;
                var timers = _timers.filter(function (d) { return context.timerFilter(d); });
                var timerData = [];
                if (timers) {
                    this.avg = timers.reduce(function (sum, timer) { return sum + timer.Seconds; }, 0) / timers.length;
                    var sqrDiffs = timers.map(function (timer) { return Math.pow(timer.Seconds - context.avg, 2); });
                    var variance = sqrDiffs.reduce(function (sum, value) { return sum + value; }, 0) / sqrDiffs.length;
                    this.stdDev = Math.sqrt(variance);
                    for (var i = 0; i < timers.length; ++i) {
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
                    var deviation = (item.Seconds - context.avg) / context.stdDev;
                    var redness = 0;
                    var greeness = 0;
                    if (deviation > 0) {
                        redness = Math.min(255, Math.floor(255 * deviation / 3));
                    } else {
                        greeness = -Math.min(255, Math.floor(255 * deviation / 3));
                    }
                    return {
                        r: 255 - greeness,
                        g: 255 - redness,
                        b: 255 - redness - greeness
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
