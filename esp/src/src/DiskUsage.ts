import { Gauge } from "@hpcc-js/chart";
import { Palette } from "@hpcc-js/common";
import { GetTargetClusterUsageEx, MachineService, DFUXRefService, WsDFUXRef } from "@hpcc-js/comms";
import { ColumnFormat, Table } from "@hpcc-js/dgrid";
import { FlexGrid } from "@hpcc-js/layout";
import * as dojoOn from "dojo/on";
import nlsHPCC from "./nlsHPCC";

Palette.rainbow("DiskUsage", ["green", "green", "green", "green", "green", "green", "green", "green", "orange", "red", "red"]);

const machineService = new MachineService({ baseUrl: "", timeoutSecs: 360 });

interface CompontentT {
    rowCount: number;
    inUse: number;
    total: number;
    max: number;
}

type DetailsT = GetTargetClusterUsageEx.TargetClusterUsage | CompontentT;

const calcPct = (val, tot) => Math.round((val / tot) * 100);

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
        machineService.GetTargetClusterUsageEx(this._targetCluster !== undefined ? [this._targetCluster] : undefined, bypassCachedResult).then(response => {
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
                            mu.DiskUsages.filter(du => !isNaN(du.InUse) || !isNaN(du.Total)).forEach(du => {
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
                                const usage = calcPct(du.InUse, du.Total);
                                details.max = details.max < usage ? usage : details.max;
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
                            .value(details.max / 100)
                            .valueDescription(nlsHPCC.Max)
                            .tickValue(inUseMean / totalMean)
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

const netAddress = mu => mu.NetAddress !== "." ? mu.NetAddress : mu.Name;

interface DirectoryEx {
    Cluster: string;
    Num: number;
    Name: string;
    MaxSize: number;
    MaxIP: string;
    MinSize: number;
    MinIP: string;
    Size: number;
    PositiveSkew: string;
}

interface XREFDirectories {
    nodes: WsDFUXRef.XRefNode[];
    directories: DirectoryEx[];
}

function xrefDirectory(cluster: string): Promise<WsDFUXRef.DFUXRefDirectoriesQueryResponse> {
    const service = new DFUXRefService({ baseUrl: "" });
    return service.DFUXRefDirectories({ Cluster: cluster }).catch(e => {
        return {} as WsDFUXRef.DFUXRefDirectoriesQueryResponse;
    });
}

function xrefDirectories(): Promise<XREFDirectories> {
    const service = new DFUXRefService({ baseUrl: "" });
    return service.DFUXRefList().then(response => {
        return Promise.all(response.DFUXRefListResult.XRefNode.map(xrefNode => xrefDirectory(xrefNode.Name)))
            .then(responses => {
                const directories: DirectoryEx[] = [];
                for (const response of responses) {
                    response.DFUXRefDirectoriesQueryResult?.Directory?.forEach(dir => {
                        directories.push({
                            Cluster: response.DFUXRefDirectoriesQueryResult?.Cluster,
                            Name: dir.Name,
                            Num: Number(dir.Num),
                            PositiveSkew: dir.PositiveSkew,
                            Size: Number(dir.Size),
                            MaxIP: dir.MaxIP,
                            MaxSize: Number(dir.MaxSize),
                            MinIP: dir.MinIP,
                            MinSize: Number(dir.MinSize)
                        });
                    });
                }
                return {
                    nodes: response.DFUXRefListResult.XRefNode,
                    directories: directories.filter(dir => Number(dir.Num) > 0)
                };
            });
    });
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
                mu.DiskUsages.forEach((du, i) => {
                    data.push([calcPct(du.InUse, du.Total), cu.Name, du.Name, "<a id='" + netAddress(mu) + "' href='#' class='ip' onClick='return false;'>" + netAddress(mu) + "</a>", du.Path, du.InUse, du.Total]);
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

    private _signal;
    enter(domNode, element) {
        super.enter(domNode, element);
        this._signal = dojoOn(domNode, ".ip:click", evt => {
            const component = evt.selectorTarget.id;
            this.componentClick(component);
        });
    }

    exit(domNode, element) {
        this._signal?.remove();
        super.exit(domNode, element);
    }

    refresh() {
        this
            .noDataMessage(nlsHPCC.loadingMessage)
            .data([])
            .render()
            ;

        Promise.all([
            machineService.GetTargetClusterUsageEx([this._targetCluster]),
            xrefDirectories().then(xref => {
                return xref;
            })
        ]).then(([details, xref]) => {
            this
                .noDataMessage(nlsHPCC.noDataMessage)
                .details(details[0])
                .render()
                ;
        });
        return this;
    }

    //  Events  ---
    componentClick(component: string) {
    }
}

export class ComponentDetails extends Table {

    constructor(readonly _ipAddress: string) {
        super();
        this
            .sortable(true)
            .columns([nlsHPCC.Folder, nlsHPCC.Files, nlsHPCC.Size, nlsHPCC.MaxNode, nlsHPCC.MaxSize, nlsHPCC.MinNode, nlsHPCC.MinSize, nlsHPCC.SkewPositive])
            ;
    }

    private details(xref: XREFDirectories) {
        const data = [];
        this
            .sortByDescending(false)
            .data([])
            .render()
            ;
        xref.directories?.filter(dir => dir.MaxIP === this._ipAddress)
            .forEach(dir => {
                data.push([dir.Name, dir.Num, dir.Size, dir.MaxIP, dir.MaxSize, dir.MinIP, dir.MinSize, dir.PositiveSkew]);
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

        xrefDirectories().then(xrefDirs => {
            this
                .noDataMessage(nlsHPCC.noDataMessage)
                .details(xrefDirs)
                .render()
                ;
        });
        return this;
    }
}
