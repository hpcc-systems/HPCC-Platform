import { Gauge } from "@hpcc-js/chart";
import { Palette } from "@hpcc-js/common";
import { GetTargetClusterUsageEx, MachineService } from "@hpcc-js/comms";
import { ColumnFormat, Table } from "@hpcc-js/dgrid";
import { FlexGrid } from "@hpcc-js/layout";
import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";

Palette.rainbow("DiskUsage", ["green", "green", "green", "green", "green", "green", "green", "green", "orange", "red", "red"]);

const connection = new MachineService({ baseUrl: "", timeoutSecs: 360 });

interface CompontentT {
    rowCount: number;
    inUse: number;
    total: number;
    max: number;
}

type DetailsT = GetTargetClusterUsageEx.TargetClusterUsage | CompontentT;

export class Summary extends FlexGrid {

    private _loadingMsg;
    private _usage: { [id: string]: { details: DetailsT, gauge: Gauge } } = {};

    constructor(readonly _targetCluster?: string) {
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
        connection.GetTargetClusterUsageEx(this._targetCluster !== undefined ? [this._targetCluster] : undefined, bypassCachedResult).then(response => {
            this._loadingMsg && this._loadingMsg
                .html('<i class="fa fa-database"></i>')
                ;
            if (!this._targetCluster) {
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
            } else {
                response.filter(details => details.Name === this._targetCluster).forEach(_details => {
                    const data: { [key: string]: CompontentT } = {};
                    _details.ComponentUsages.forEach(cu => {
                        cu.MachineUsages.forEach(mu => {
                            mu.DiskUsages.forEach(du => {
                                if (data[du.Name] === undefined) {
                                    data[du.Name] = {
                                        rowCount: 0,
                                        inUse: 0,
                                        total: 0,
                                        max: 0
                                    };
                                }
                                const details = data[du.Name];
                                details.rowCount++;
                                details.inUse += du.InUse;
                                details.total += du.Total;
                                details.max = details.max < du.InUse ? du.InUse : details.max;
                            });
                        });
                    });
                    for (const key in data) {
                        const details = data[key];
                        if (!this._usage[key]) {
                            this._usage[key] = {
                                details,
                                gauge: new Gauge()
                                    .title(key)
                                    .showTick(true)
                            };
                        }

                        const totalMean = details.total / details.rowCount;
                        const inUseMean = details.inUse / details.rowCount;
                        this._usage[key].gauge
                            .value((details.max / totalMean))
                            .valueDescription(nlsHPCC.Max)
                            .tickValue((inUseMean / totalMean))
                            .tickValueDescription(nlsHPCC.Mean)
                            .tooltip(key)
                            ;
                    }
                });
            }
            this.render();
        });
        return this;
    }

    //  Events
    click(gauge: Gauge, details: DetailsT) {
    }
}

export class Details extends Table {

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
    private details(_: GetTargetClusterUsageEx.TargetClusterUsage) {
        this._details = _;
        const data = [];
        this
            .sortByDescending(false)
            .data([])
            .render()
            ;
        this._details.ComponentUsages.forEach(cu => {
            cu.MachineUsages.forEach(mu => {
                mu.DiskUsages.forEach(du => {
                    data.push([du.PercentUsed, cu.Name, du.Name, mu.NetAddress !== "." ? mu.NetAddress : mu.Name, du.Path, du.InUse, du.Total]);
                });
            });
        });
        this
            .sortBy(nlsHPCC.PercentUsed)
            .sortByDescending(true)
            .data(data)
            ;
        return this;
    }

    refresh() {
        this
            .noDataMessage(nlsHPCC.loadingMessage)
            .data([])
            .render()
            ;
        connection.GetTargetClusterUsageEx([this._targetCluster]).then(details => {
            this
                .noDataMessage(nlsHPCC.noDataMessage)
                .details(details[0])
                .render()
                ;
        });
        return this;
    }
}
