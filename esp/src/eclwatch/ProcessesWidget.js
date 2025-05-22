define([
    "dojo/_base/declare",
    "src/nlsHPCC",

    "hpcc/GridDetailsWidget",
    "src/ESPWorkunit",
    "src/ESPUtil"

], function (declare, nlsHPCCMod,
    GridDetailsWidget, ESPWorkunit, ESPUtil) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("ProcessesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.Processes,
        idProperty: "PID",

        wu: null,

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                        context.refreshGrid();
                    }
                });
            }
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    Name: { label: nlsHPCC.Name, width: 180 },
                    Type: { label: nlsHPCC.Type, width: 100 },
                    ...(
                        dojoConfig.isContainer ?
                            {
                                PodName: { label: nlsHPCC.PodName, width: 320 }
                            } :
                            {
                                Log: { label: nlsHPCC.Log, width: 400 },
                                PID: { label: nlsHPCC.ProcessID, width: 80 },
                            }
                    ),
                    InstanceNumber: { label: nlsHPCC.InstanceNumber, width: 120 },
                    Max: { label: nlsHPCC.Max, width: 80 }
                }
            }, domID);
            return retVal;
        },

        refreshGrid: function (args) {
            var context = this;
            this.wu.getInfo({
                onGetProcesses: function (processes) {
                    context.store.setData(processes);
                    context.grid.refresh();
                }
            });
        }
    });
});
