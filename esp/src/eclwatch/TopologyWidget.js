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
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/on",

    "dijit/registry",
    "dijit/form/ToggleButton",
    "dijit/ToolbarSeparator",
    "dijit/layout/ContentPane",

    "dgrid/tree",

    "hpcc/GridDetailsWidget",
    "hpcc/ESPRequest",
    "hpcc/ESPTopology",
    "hpcc/TopologyDetailsWidget",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil"

], function (declare, i18n, nlsHPCC, on,
                registry, ToggleButton, ToolbarSeparator, ContentPane,
                tree,
                GridDetailsWidget, ESPRequest, ESPTopology, TopologyDetailsWidget, DelayLoadWidget, ESPUtil) {
    return declare("TopologyWidget", [GridDetailsWidget], {

        i18n: nlsHPCC,
        gridTitle: nlsHPCC.title_Topology,
        idProperty: "__hpcc_id",

        postCreate: function (args) {
            this.inherited(arguments);
            this.detailsWidget = new TopologyDetailsWidget({
                id: this.id + "Details",
                region: "right",
                splitter: true,
                style: "width: 66%",
                minSize: 240
            });
            this.detailsWidget.placeAt(this.gridTab, "last");
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            this.viewModeDebug = new ToggleButton({
                showLabel: true,
                checked: false,
                style:{display: "none"},
                onChange: function (val) {
                    if (val) {
                        context.viewModeMachines.set("checked", false);
                        context.viewModeServices.set("checked", false);
                        context.viewModeTargets.set("checked", false);
                        context.refreshGrid("Debug");
                    }
                },
                label: "Debug"
            }).placeAt(this.openButton.domNode, "after");
            this.viewModeMachines = new ToggleButton({
                showLabel: true,
                checked: false,
                onChange: function (val) {
                    if (val) {
                        context.viewModeDebug.set("checked", false);
                        context.viewModeServices.set("checked", false);
                        context.viewModeTargets.set("checked", false);
                        context.refreshGrid("Machines");
                    }
                },
                label: "Machines"
            }).placeAt(this.openButton.domNode, "after");
            this.viewModeServices = new ToggleButton({
                showLabel: true,
                checked: false,
                onChange: function (val) {
                    if (val) {
                        context.viewModeDebug.set("checked", false);
                        context.viewModeMachines.set("checked", false);
                        context.viewModeTargets.set("checked", false);
                        context.refreshGrid("Services");
                    }
                },
                label: "Services"
            }).placeAt(this.openButton.domNode, "after");
            this.viewModeTargets = new ToggleButton({
                showLabel: true,
                checked: true,
                onChange: function (val) {
                    if (val) {
                        context.viewModeDebug.set("checked", false);
                        context.viewModeMachines.set("checked", false);
                        context.viewModeServices.set("checked", false);
                        context.refreshGrid("Targets");
                    }
                },
                label: "Targets"
            }).placeAt(this.openButton.domNode, "after");
            new ToolbarSeparator().placeAt(this.openButton.domNode, "after");

            this.store = new ESPTopology.Store();
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: [
                tree({
                        field: "__hpcc_displayName",
                        label: this.i18n.Topology,
                        width: 150,
                        collapseOnRefresh: true,
                        shouldExpand: function (row, level, previouslyExpanded) {
                            if (previouslyExpanded !== undefined) {
                                return previouslyExpanded;
                            } else if (level < -1) {
                                return true;
                            }
                            return false;
                        },
                        formatter: function (_id, row) {
                            return "<img src='" + dojoConfig.getImageURL(row.getIcon()) + "'/>&nbsp;" + row.getLabel();
                        }
                    })
                ]
            }, domID);

            retVal.on("dgrid-select", function (event) {
                var selection = context.grid.getSelected();
                if (selection.length) {
                    context.detailsWidget.init(selection[0]);
                }
            });

            return retVal;
        },

        createDetail: function (id, row, params) {
            return new DelayLoadWidget({
                id: id,
                title: row.__hpcc_displayName,
                closable: true,
                delayWidget: "TopologyDetailsWidget",
                hpcc: {
                    params: row
                }
            });
        },

        refreshGrid: function (mode) {
            var context = this;
            if (mode) {
                this.store.viewMode(mode);
                this.grid.refresh();
            } else {
                this.store.viewMode("Targets");
                this.store.refresh(function () {
                    context.grid.refresh();
                });
            }
        }
    });
});
