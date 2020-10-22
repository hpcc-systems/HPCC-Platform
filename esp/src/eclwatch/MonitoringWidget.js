define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom-class",
    "dojo/topic",

    "dgrid/tree",

    "hpcc/GridDetailsWidget",
    "src/ws_machine",
    "src/ESPUtil",
    "src/Utility"

], function (declare, lang, nlsHPCCMod, arrayUtil, domClass, topic,
    tree,
    GridDetailsWidget, WsMachine, ESPUtil, Utility) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("MonitoringWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.PrimaryMonitoring,
        idProperty: "__hpcc_id",

        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;
            this._refreshActionState();
            this.refreshGrid();
            this.startTimer();

            topic.subscribe("hpcc/monitoring_component_update", function (topic) {
                context.refreshGrid();
            });
        },

        createGrid: function (domID) {
            this.store.mayHaveChildren = function (item) {
                if (item.StatusReports && item.StatusReports.StatusReport && item.StatusReports.StatusReport.length) {
                    return true;
                }
                return false;
            };

            this.store.getChildren = function (parent, options) {
                var retVal = this.query({ __hpcc_parentName: parent.__hpcc_id }, options);
                return retVal;
            };

            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                sort: [{ attribute: "StatusID", descending: true }, { attribute: "Status", descending: true }],
                columns: {
                    StatusID: { label: "", width: 0, sortable: false, hidden: true },
                    ComponentType: tree({
                        label: "Name", sortable: true, width: 200,
                        formatter: function (Name, row) {
                            switch (row.Status) {
                                case "Normal":
                                    return Utility.getImageHTML("normal.png") + Name;
                                case "Warning":
                                    return Utility.getImageHTML("warning.png") + Name;
                                case "Error":
                                    return Utility.getImageHTML("error.png") + Name;
                            }
                            return "";
                        }
                    }),
                    StatusDetails: { label: "Details", sortable: false },
                    URL: {
                        label: "URL", width: 200, sortable: false,
                        formatter: function (Name, row) {
                            if (Name) {
                                return "<a href=http://" + Name + " target='_blank'>" + Name + "</a>";
                            } else {
                                return "";
                            }
                        }
                    },
                    EndPoint: { label: "IP", sortable: true, width: 140 },
                    TimeReportedStr: { label: "Time Reported", width: 140, sortable: true },
                    Status: {
                        label: this.i18n.Severity, width: 130, sortable: false,
                        renderCell: function (object, value, node, options) {
                            switch (value) {
                                case "Error":
                                    domClass.add(node, "ErrorCell");
                                    break;
                                case "Warning":
                                    domClass.add(node, "WarningCell");
                                    break;
                                case "Normal":
                                    domClass.add(node, "NormalCell");
                                    break;
                            }
                            node.innerText = value;
                        }
                    }
                }
            }, domID);

            return retVal;
        },

        refreshGrid: function () {
            var context = this;

            WsMachine.GetComponentStatus({
                request: {}
            }).then(function (response) {
                var results = [];
                var newRows = [];
                if (lang.exists("GetComponentStatusResponse.ComponentStatusList.ComponentStatus", response)) {
                    results = response.GetComponentStatusResponse.ComponentStatusList.ComponentStatus;
                }
                arrayUtil.forEach(results, function (row, idx) {
                    lang.mixin(row, {
                        __hpcc_parentName: null,
                        __hpcc_id: row.ComponentType + row.EndPoint
                    });

                    arrayUtil.forEach(row.StatusReports.StatusReport, function (statusReport, idx) {
                        newRows.push({
                            __hpcc_parentName: row.ComponentType + row.EndPoint,
                            __hpcc_id: row.ComponentType + row.EndPoint + "_" + idx,
                            ComponentType: statusReport.Reporter,
                            Status: statusReport.Status,
                            StatusDetails: statusReport.StatusDetails,
                            URL: statusReport.URL
                        });
                    });
                });

                arrayUtil.forEach(newRows, function (newRow) {
                    results.push(newRow);
                });

                context.store.setData(results);
                context.grid.set("query", { __hpcc_parentName: null });
            });
        },

        startTimer: function () {
            WsMachine.MonitorComponentStatus({ request: {} })
        }
    });
});
