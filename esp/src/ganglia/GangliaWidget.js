define([
    "dojo/_base/declare",
    "dojo/_base/array",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/ganglia",
    "dojo/dom",
    "dojo/_base/array",
    "dojo/dom-construct",
    "dojo/store/Memory",
    "dojo/request/xhr",
    "dojo/query",

    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",

    "./GangliaFilterDropDownWidget",
    "./ws_rrd",
    "hpcc/_TabContainerWidget",

    "dojo/text!./templates/GangliaWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/form/Textarea",
    "dijit/Dialog",
    "dijit/form/Button",
    "dijit/form/CheckBox",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/TitlePane",
    "dijit/form/DropDownButton",
    "dijit/form/FilteringSelect",
    "dijit/CheckedMenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem"

], function (declare, arrayUtil, lang, i18n, nlsHPCC, dom, arrayUtil, domConstruct, Memory, xhr, query,
    registry, Menu, MenuItem,
    GangliaFilterDropDownWidget, WsRrd, _TabContainerWidget,
    template,
    BorderContainer, TabContainer, ContentPane, Toolbar, Button) {
        return declare("GangliaWidget", [_TabContainerWidget], {
            templateString: template,
            baseClass: "GangliaWidget",
            i18n: nlsHPCC,

            initalized: false,

            filter: null,
            server: "",
            cluster: "",
            metrics: null,
            epochFilter: null,
            epochNow: null,

            postCreate: function (args) {
                this.inherited(arguments);
                this.clusterTargetSelect = registry.byId(this.id + "ClusterTargetSelect");
                this.serverTargetSelect = registry.byId(this.id + "ServerTargetSelect");
                this.metricsTargetSelect = registry.byId(this.id + "MetricsTargetSelect");
                this.fromGanliaDateRange = registry.byId(this.id + "FromGanliaDateRange");
                this.tabContainer = registry.byId(this.id + "TabContainer");
                this.defaultTab = registry.byId(this.id + "_Custom");
                this.filter = registry.byId(this.id + "Filter");
            },

            //  Hitched actions  ---

            //  Implementation  ---
            init: function (params) {
                var context = this;
                if (this.inherited(arguments))
                    return;

                this._buildClusters();
                this._buildMetrics();
                this._buildTabs();

                this.clusterTargetSelect.on('change', function (newValue) {
                    context.serverTargetSelect.set("value", "");
                    context.cluster = newValue;
                    context._buildServers();
                });

                this.serverTargetSelect.on('change', function (newValue) {
                    context.server = newValue;
                    context._buildMetrics();
                });

                this.metricsTargetSelect.on('change', function (newValue) {
                    context.metrics = newValue;
                });

                this.filter.on("apply", function (evt) {
                    context._onFilterApply();
                });

                this.fromGanliaDateRange.set("value", "Year");

                this.fromGanliaDateRange.on('change', function (newValue) {
                    context._calculateEpoch(newValue);
                });
            },

            _calculateEpoch: function (newValue) {
                var context = this;
                this.epochNow = Math.round(new Date().getTime() / 1000.0);
                switch (newValue) {
                    case "Year":
                        context.epochFilter = context.epochNow - 31556926;
                        break;
                    case "Month":
                        context.epochFilter = context.epochNow - 2629743;
                        break;
                    case "Week":
                        context.epochFilter = context.epochNow - 604800;
                        break;
                    case "Day":
                        context.epochFilter = context.epochNow - 86400;
                        break;
                    case "Hour":
                        context.epochFilter = context.epochNow - 3600;
                        break;
                    default:
                        context.epocFilter = context.epochNow - 31556926;
                }
            },

            _buildClusters: function () {
                var context = this;
                this.clusterTargetSelect.required = false;
                WsRrd.GangliaClusterList({
                    request: {
                        Cluster: "",
                        Server: ""
                    },
                }).then(function (response) {
                    if (lang.exists("GetAvailableClustersResponse.Clusters.Item", response)) {
                        var output = arrayUtil.map(response.GetAvailableClustersResponse.Clusters.Item, function (item, idx) {
                            return {
                                name: item,
                                id: item
                            };
                        });
                        var myStore = new Memory({
                            data: output
                        });
                        context.clusterTargetSelect.set("store", myStore);
                    }
                });
            },

            _buildServers: function (newValue) {
                var context = this;
                this.serverTargetSelect.required = false;
                WsRrd.GangliaServerList({
                    request: {
                        Cluster: this.cluster
                    },
                }).then(function (response) {
                    if (lang.exists("GetAvailableServersForMetricsResponse.Servers.Item", response)) {
                        var output = arrayUtil.map(response.GetAvailableServersForMetricsResponse.Servers.Item, function (item, idx) {
                            return {
                                name: item,
                                id: item
                            };
                        });
                        var myStore = new Memory({
                            data: output
                        });
                        context.serverTargetSelect.set("store", myStore);
                    }
                });
            },

            _buildMetrics: function (newValue) {
                var context = this;
                this.metricsTargetSelect.required = true;
                WsRrd.GangliaMetricList({
                    request: {
                        Cluster: this.cluster,
                        Server: this.server
                    },
                }).then(function (response) {
                    if (lang.exists("GetAvailableMetricsResponse.Metrics.Item", response)) {
                        var output = arrayUtil.map(response.GetAvailableMetricsResponse.Metrics.Item.sort(), function (item, idx) {
                            return {
                                name: item,
                                id: item
                            };
                        });

                        var myStore = new Memory({
                            data: output
                        });
                        context.metricsTargetSelect.set("store", myStore);
                    }
                });
            },

            _buildTabs: function () {
                var context = this;
                var tabs = registry.byId(this.id + "TabContainer");
                xhr("/esp/files/ganglia/ganglia.json", {
                    handleAs: "json"
                }).then(function (data) {
                    arrayUtil.forEach(data.tabs, function (tabItem, idx) {
                        var cleanId = tabItem.name.split(' ').join('_');
                        arrayUtil.forEach(tabItem.metrics, function (metricItem) {
                            var clean = metricItem.join('\n');
                            arrayUtil.forEach(tabItem.time, function (timeItem) {
                                context._calculateEpoch(timeItem)
                                WsRrd.GangliaRRDGraphList({
                                    request: {
                                        Clusters: "",
                                        Servers: "",
                                        RRDMetrics: clean,
                                        StartTime: context.epochFilter,
                                        EndTime: context.epochNow,
                                        Width: data.width,
                                        Height: data.height,
                                        Title: timeItem
                                    },
                                }).then(function (response) {
                                    if (lang.exists("GraphSVGDataResponse.Graph", response)) {
                                        var node = dom.byId(tabItem.name + idx);
                                        var graph = domConstruct.create("div", {
                                            innerHTML: response.GraphSVGDataResponse.Graph,
                                            id: cleanId + node.children.length,
                                            class: 'left'
                                        }, tabItem.name + idx);
                                    }
                                });
                            });
                        });
                        var bordercontainer = new BorderContainer({
                            style: "height: 100%; width: 100%;",
                            title: cleanId,
                            id: cleanId,
                            closable: true
                        });
                        var toolbar = new Toolbar({
                            region: "top"
                        });
                        var button = new Button({
                            label: "Refresh",
                            showLabel: true,
                            iconClass: "iconRefresh",
                            onClick: function () {
                                var node = dom.byId(tabItem.name + idx);
                                query(".left", node).forEach(domConstruct.destroy);
                                if (node.children.length === 0) {
                                    context._genGraphRefresh(tabItem.name + idx);
                                }
                            }
                        });
                        var contentpane = new ContentPane({
                            id: tabItem.name + idx,
                            region: "center"
                        });

                        bordercontainer.addChild(contentpane);
                        bordercontainer.addChild(toolbar);
                        toolbar.addChild(button);
                        bordercontainer.placeAt(context.tabContainer, idx);
                        bordercontainer.startup();
                    });
                    context.tabContainer.selectChild(data.tabs[0].name.split(' ').join('_'));
                    /*TODO add interval refreshes of each tab
                    setInterval(function () {
                        var node = dom.byId(tabItem.name+idx);
                        query(".left", node).forEach(domConstruct.destroy);
                        context._genGraphRefresh();
                    }, data.refreshInterval);*/
                });
            },

            _genGraphRefresh: function (id) {
                var context = this;
                xhr("/esp/files/ganglia/ganglia.json", {
                    handleAs: "json"
                }).then(function (data) {
                    arrayUtil.forEach(data.tabs, function (tabItem, idx) {
                        arrayUtil.forEach(tabItem.metrics, function (metricItem) {
                            arrayUtil.forEach(tabItem.time, function (timeItem) {
                                var clean = metricItem.join('\n');
                                context._calculateEpoch(timeItem)
                                WsRrd.GangliaRRDGraphList({
                                    request: {
                                        Clusters: "",
                                        Servers: "",
                                        RRDMetrics: clean,
                                        StartTime: context.epochFilter,
                                        EndTime: context.epochNow,
                                        Width: data.width,
                                        Height: data.height,
                                        Title: timeItem
                                    },
                                }).then(function (response) {
                                    if (lang.exists("GraphSVGDataResponse.Graph", response)) {
                                        var graph = domConstruct.create("div", {
                                            innerHTML: response.GraphSVGDataResponse.Graph,
                                            class: 'left'
                                        }, tabItem.name + idx);
                                    }
                                });
                            });
                        });
                    });
                });
            },

            _onFilterApply: function () {
                var context = this;
                var cluster = this.cluster;
                var server = this.server;
                var metrics = this.metrics;
                var epochFilter = this.epochFilter;
                var epochNow = this.epochNow;
                var graphId = this.cluster + "_" + this.server + "_" + this.metrics + "_" + this.epochFilter;

                WsRrd.GangliaRRDGraphList({
                    request: {
                        Clusters: this.cluster,
                        Servers: this.server,
                        RRDMetrics: this.metrics,
                        StartTime: this.epochFilter,
                        EndTime: this.epochNow,
                        Width: 300,
                        Height: 120,
                        Title: this.cluster + ":" + this.server + ":" + this.metrics
                    },
                }).then(function (response) {
                    if (dojo.byId(graphId)) {
                        alert(context.i18n.GraphExists);
                    } else {
                        if (lang.exists("GraphSVGDataResponse.Graph", response)) {
                            var graph = domConstruct.create("div", {
                                id: graphId,
                                innerHTML: response.GraphSVGDataResponse.Graph,
                                class: 'left'
                            }, dom.byId(context.id + "graphs"));
                        }
                    }

                    var pMenu;
                    pMenu = new Menu({
                        targetNodeIds: [graphId]
                    });
                    pMenu.addChild(new MenuItem({
                        label: "Delete Graph",
                        onClick: function () {
                            domConstruct.destroy(this.getParent().currentTarget);
                        }

                    }));
                    pMenu.addChild(new MenuItem({
                        label: "Pop Out Graph",
                        onClick: function () {
                            context._onNewGraphPage(cluster, server, metrics, epochFilter, epochNow);
                        }
                    }));
                    pMenu.startup();
                });
            },

            _onNewGraphPage: function (cluster, server, metrics, epochFilter, epochNow) {
                xhr("/esp/files/ganglia/ganglia.json", {
                    handleAs: "json"
                }).then(function (data) {
                    WsRrd.GangliaRRDGraphList({
                        request: {
                            Clusters: cluster,
                            Servers: server,
                            RRDMetrics: metrics,
                            StartTime: epochFilter,
                            EndTime: epochNow,
                            Width: data.width,
                            Height: data.height
                        },
                    }).then(function (response) {
                        if (lang.exists("GraphSVGDataResponse.Graph", response)) {
                            var newWindow = window.open('', '_blank', 'width=200,height=100');
                            var newContent = "<HTML><HEAD><TITLE>Graph</TITLE></HEAD>";
                            newContent += "<BODY><div>" + response.GraphSVGDataResponse.Graph + "</div>";
                            newContent += "</BODY></HTML>";
                            newWindow.document.write(newContent);
                            newWindow.document.close();
                        }
                    });
                });
            }
        });
    });
