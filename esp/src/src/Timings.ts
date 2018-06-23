import { Workunit } from "@hpcc-js/comms";
import * as hpccCommon from "@hpcc-js/common";
import { WUTimeline } from "@hpcc-js/eclwatch";
import { Column } from "@hpcc-js/chart";

const d3Select = (hpccCommon as any).select;

export class Timings {
    private wu: Workunit;

    private timeline = new WUTimeline()
        .overlapTolerence(1)
        .baseUrl("")
        .request({
            ScopeFilter: {
                MaxDepth: 2,
                ScopeTypes: []
            },
            NestedFilter: {
                Depth: 0,
                ScopeTypes: []
            },
            PropertiesToReturn: {
                AllProperties: true,
                AllStatistics: true,
                AllHints: true,
                Properties: ["WhenStarted", "TimeElapsed"]
            },
            ScopeOptions: {
                IncludeId: true,
                IncludeScope: true,
                IncludeScopeType: true
            },
            PropertyOptions: {
                IncludeName: true,
                IncludeRawValue: true,
                IncludeFormatted: true,
                IncludeMeasure: true,
                IncludeCreator: true,
                IncludeCreatorType: false
            }
        })
        .on("click", (row, col, sel) => {
            this.refresh();
        })
        ;

    private chart = new Column()
        .yAxisDomainLow(0 as any)
        .yAxisTickFormat("s")
        ;

    private metricsSelect;

    constructor(wuid: string, timelineTarget: string, chartTarget: string, metricsSelectTarget: string) {
        this.wu = Workunit.attach({ baseUrl: "" }, wuid);
        this.timeline
            .target(timelineTarget)
            .wuid(wuid)
        this.chart
            .target(chartTarget)
            ;
        this.metricsSelect = d3Select(`#${metricsSelectTarget}`);
    }

    init(wuid: string) {

    }

    protected walkScopeName(id: string, visitor: (name: string) => boolean) {
        const parts = id.split(":");
        while (parts.length > 1) {
            parts.pop();
            if (visitor(parts.join(":"))) {
                break;
            }
        }
    }

    graphID(id: string): string | undefined {
        let retVal: string;
        this.walkScopeName(id, partialID => {
            retVal = this._graphLookup[partialID];
            if (retVal) return true;
        });
        return retVal;
    }

    subgraphID(id: string): string {
        let retVal: string;
        this.walkScopeName(id, partialID => {
            retVal = this._subgraphLookup[partialID];
            if (retVal) return true;
        });
        return retVal;
    }

    _scopeFilter: string = "";
    _metricSelectLabel: string = "";
    _metricSelectValue: string = "";
    _graphLookup: { [id: string]: string } = {};
    _subgraphLookup: { [id: string]: string } = {};
    fetchDetailsNormalizedPromise;
    refresh(force: boolean = false) {
        if (force) {
            this.timeline
                .clear()
                .on("click", (row, col, sel) => {
                    this._scopeFilter = sel ? row.__lparam.ScopeName : undefined;
                    this.click(row, col, sel);
                })
                .render()
                ;
        }
        if (force || !this.fetchDetailsNormalizedPromise) {
            this.fetchDetailsNormalizedPromise = this.wu.fetchDetailsNormalized({
                ScopeFilter: {
                    MaxDepth: 999999,
                    ScopeTypes: []
                },
                NestedFilter: {
                    Depth: 999999,
                    ScopeTypes: []
                },
                PropertiesToReturn: {
                    AllProperties: true,
                    AllStatistics: true,
                    AllHints: true,
                    Properties: []
                },
                ScopeOptions: {
                    IncludeId: true,
                    IncludeScope: true,
                    IncludeScopeType: true
                },
                PropertyOptions: {
                    IncludeName: true,
                    IncludeRawValue: true,
                    IncludeFormatted: true,
                    IncludeMeasure: true,
                    IncludeCreator: false,
                    IncludeCreatorType: false
                }
            });
        }

        var context = this;
        this._metricSelectValue = this.metricsSelect.node().value || "TimeElapsed";
        return this.fetchDetailsNormalizedPromise.then(function (response) {
            context._graphLookup = {};
            context._subgraphLookup = {};
            var data = response.data.filter(function (row) {
                if (row.type === "graph") context._graphLookup[row.name] = row.id;
                if (row.type === "subgraph") context._subgraphLookup[row.name] = row.id;
                if (!row.id) return false;
                if (context._scopeFilter && row.name !== context._scopeFilter && row.name.indexOf(`${context._scopeFilter}:`) !== 0) return false;
                if (row[context._metricSelectValue] === undefined) return false;
                return true;
            }).sort(function (l, r) {
                if (l.WhenStarted === undefined && r.WhenStarted !== undefined || l.WhenStarted < r.WhenStarted) return -1;
                if (l.WhenStarted !== undefined && r.WhenStarted === undefined || l.WhenStarted > r.WhenStarted) return 1;
                if (l.id < r.id) return -1;
                if (l.id > r.id) return 1;
                return 0;
            });

            var measure = "";
            var colArr = [];
            for (var key in response.columns) {
                if (response.columns[key].Measure && response.columns[key].Measure !== "label") {
                    colArr.push(key)
                }
                if (key === context._metricSelectValue) {
                    measure = response.columns[key].Measure;
                }
            }
            context._metricSelectLabel = context._metricSelectValue + (measure ? " (" + measure + ")" : "");
            var options = context.metricsSelect.selectAll("option").data(colArr, function (d) { return d; });
            options.enter().append("option")
                .property("value", function (d) { return d; })
                .merge(options)
                .text(function (d) { return d; })
                ;
            options.exit().remove();
            context.metricsSelect.node().value = context._metricSelectValue;

            context.chart
                .columns([context._scopeFilter || "All", context._metricSelectLabel || "Weight"])
                .data(data.filter(function (row, i) {
                    return row.name !== context._scopeFilter;
                }).map(function (row, i) {
                    return [row.id, row[context._metricSelectValue]];
                }))
                .yAxisTitle(measure)
                .render()
                ;

            return data;
        });
    }

    resizeTimeline() {
        this.timeline
            .resize()
            .lazyRender()
            ;
    }

    resizeChart() {
        this.chart
            .resize()
            .lazyRender()
            ;
    }

    //  Events ---
    click(row, col, sel) {
    }
}
