import { Gauge } from "@hpcc-js/chart";
import { Palette } from "@hpcc-js/common";
import { GetTargetClusterUsageEx, MachineService } from "@hpcc-js/comms";
import { ColumnFormat, Table } from "@hpcc-js/dgrid";
import { FlexGrid } from "@hpcc-js/layout";
import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";

Palette.rainbow("DiskUsage", ["green", "green", "green", "green", "green", "green", "green", "green", "orange", "red", "red"]);

export class Summary extends FlexGrid {

    private _connection = new MachineService({ baseUrl: "", timeoutSecs: 360 });
    private _loadingMsg;
    private _usage: { [id: string]: { details: GetTargetClusterUsageEx.TargetClusterUsage, gauge: Gauge } } = {};

    constructor() {
        super();
        this
            .itemMinHeight(100)
            .itemMinWidth(100)
            .forceYScroll(true)
            .widgetsFlexGrow([1, 1, 1]);
    }

    enter(domNode, element) {
        super.enter(domNode, element);
        this._loadingMsg = element.append("div")
            .style("float", "left")
            .style("margin-left", "4px")
            .style("margin-top", "4px")
            .style("color", "darkgray")
            .style("font-size", "14px")
            .attr("title", nlsHPCC.DiskUsage)
            ;
    }

    update(domNode, element) {
        const widgets: Gauge[] = [];
        for (const key in this._usage) {
            widgets.push(this._usage[key].gauge);
        }
        this
            .widgets(widgets)
            .flexBasis(`${100 / widgets.length}%`)
            .flexBasis("100px")
            ;

        super.update(domNode, element);
    }

    refresh(bypassCachedResult: boolean) {
        let hasGauge = false;
        for (const key in this._usage) {
            hasGauge = true;
            this._usage[key].gauge
                .value(0)
                .tickValue(0)
                .render()
                ;
        }
        if (!hasGauge) {
            this._loadingMsg && this._loadingMsg
                .text(nlsHPCC.loadingMessage)
                ;
        }
        this._connection.GetTargetClusterUsageEx(undefined, bypassCachedResult).then(response => {
            this._loadingMsg && this._loadingMsg
                .html('<i class="fa fa-database"></i>')
                ;
            response.forEach(details => {
                if (!this._usage[details.Name]) {
                    this._usage[details.Name] = {
                        details,
                        gauge: new Gauge()
                            .title(details.Name)
                            .showTick(true)
                            .on("click", (gauge: Gauge) => {
                                this.click(gauge, details);
                            })
                    };
                }
                this._usage[details.Name].gauge
                    .value((details.max || 0) / 100)
                    .valueDescription(nlsHPCC.Max)
                    .tickValue((details.mean || 0) / 100)
                    .tickValueDescription(nlsHPCC.Mean)
                    .tooltip(details.ComponentUsagesDescription)
                    ;
            });
            this.render();
        });
        return this;
    }

    //  Events
    click(gauge: Gauge, details: GetTargetClusterUsageEx.TargetClusterUsage) {
    }
}

export class Details extends Table {

    private _connection = new MachineService({ baseUrl: "" });

    constructor(readonly _targetCluster: string) {
        super();
        this
            .sortable(true)
            .columnFormats([
                new ColumnFormat()
                    .column("% Used")
                    .paletteID("DiskUsage")
            ])
            .columns([nlsHPCC.PercentUsed, nlsHPCC.Component, nlsHPCC.Type, nlsHPCC.IPAddress, nlsHPCC.Path, nlsHPCC.InUse, nlsHPCC.Total])
            ;
    }

    private _details: GetTargetClusterUsageEx.TargetClusterUsage;
    details(_: GetTargetClusterUsageEx.TargetClusterUsage) {
        this._details = _;
        const data = [];
        this._details.ComponentUsages.forEach(cu => {
            cu.MachineUsages.forEach(mu => {
                mu.DiskUsages.forEach(du => {
                    data.push([du.PercentUsed, cu.Name, du.Name, mu.NetAddress !== "." ? mu.NetAddress : mu.Name, du.Path, du.InUse, du.Total]);
                });
            });
        });
        this.data(data);
        return this;
    }

    refresh() {
        this
            .noDataMessage(nlsHPCC.loadingMessage)
            .data([])
            .render()
            ;
        this._connection.GetTargetClusterUsageEx([this._targetCluster]).then(details => {
            this
                .noDataMessage(nlsHPCC.noDataMessage)
                .details(details[0])
                .render()
                ;
        });
        return this;
    }
}
