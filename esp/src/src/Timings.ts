import { Workunit } from "@hpcc-js/comms";
import * as hpccCommon from "@hpcc-js/common";
import { WUTimeline } from "@hpcc-js/eclwatch";
import { ChartPanel } from "@hpcc-js/layout";
import { Column } from "@hpcc-js/chart";
import { ascending as d3Ascending, max as d3Max } from "d3-array";
import { scaleLinear as d3ScaleLinear } from "d3-scale";

const d3Select = (hpccCommon as any).select;

class TimingColumn extends Column {

    _columnsMetric = {};
    layerEnter(host, element, duration) {
        super.layerEnter(host, element, duration);
        this.tooltipHTML(d => {
            const lparam = d.origRow[d.origRow.length - 1];
            const metric = this._columnsMetric[d.column];
            const prop = lparam["__" + metric.Name];
            const formattedValue = prop.Formatted;
            const rawValue = prop.RawValue;
            return `${d.column}:  ${formattedValue !== rawValue ? `${formattedValue} (${rawValue} ${prop.Measure})` : `${formattedValue}`}`;
        });
    }
}

export class Timings {
    private wu: Workunit;

    private timeline = new WUTimeline()
        .maxZoom(Number.MAX_SAFE_INTEGER)
        .overlapTolerence(1)
        .baseUrl("")
        .request({
            ScopeFilter: {
                MaxDepth: 3,
                ScopeTypes: []
            },
            NestedFilter: {
                Depth: 0,
                ScopeTypes: []
            },
            PropertiesToReturn: {
                AllProperties: false,
                AllStatistics: true,
                AllHints: false,
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

    private chart = new TimingColumn()
        .yAxisDomainLow(0 as any)
        ;
    private chartPanel = new ChartPanel()
        .dataButtonVisible(false)
        .downloadButtonVisible(false)
        .legendButtonVisible(false)
        .legendVisible(true)
        .titleOverlay(true)
        .widget(this.chart)
        ;

    private metricsSelect;

    constructor(wuid: string, timelineTarget: string, chartTarget: string, metricsSelectTarget: string) {
        this.wu = Workunit.attach({ baseUrl: "" }, wuid);
        this.timeline
            .target(timelineTarget)
            .wuid(wuid)
            ;
        delete (this.timeline as any).__prop_tickFormat;
        this.chartPanel
            .target(chartTarget)
            ;
        this.metricsSelect = d3Select(`#${metricsSelectTarget}`);
    }

    selectedMetricValues() {
        this._metricSelectValues = this.metricsSelect.selectAll("option").nodes()
            .filter(n => n.selected === true)
            .map(n => n.value)
            ;
        return this._metricSelectValues;
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

    _rawColumns = {};
    _scopeFilter: string = "";
    _metricSelectLabel: string = "";
    _metricSelectValues: string[] = ["TimeElapsed"];
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
            this.fetchDetailsNormalizedPromise = Promise.all([this.wu.fetchDetailsMeta(), this.wu.fetchDetailsRaw({
                ScopeFilter: {
                    MaxDepth: 999999,
                    ScopeTypes: []
                },
                NestedFilter: {
                    Depth: 0,
                    ScopeTypes: []
                },
                PropertiesToReturn: {
                    AllProperties: false,
                    AllStatistics: true,
                    AllHints: false,
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
            })]).then(promises => {
                const meta = promises[0];
                const scopes = promises[1];
                const columns: { [id: string]: any } = {
                    id: {
                        Measure: "label"
                    },
                    name: {
                        Measure: "label"
                    },
                    type: {
                        Measure: "label"
                    }
                };
                const data: object[] = [];
                for (const scope of scopes) {
                    const props = {};
                    if (scope && scope.Id && scope.Properties && scope.Properties.Property) {
                        for (const key in scope.Properties.Property) {
                            const scopeProperty = scope.Properties.Property[key];
                            columns[scopeProperty.Name] = { ...scopeProperty };
                            delete columns[scopeProperty.Name].RawValue;
                            delete columns[scopeProperty.Name].Formatted;
                            switch (scopeProperty.Measure) {
                                case "bool":
                                    props[scopeProperty.Name] = !!+scopeProperty.RawValue;
                                    break;
                                case "sz":
                                    props[scopeProperty.Name] = +scopeProperty.RawValue;
                                    break;
                                case "ns":
                                    props[scopeProperty.Name] = +scopeProperty.RawValue;
                                    break;
                                case "ts":
                                    props[scopeProperty.Name] = new Date(+scopeProperty.RawValue / 1000).toISOString();
                                    break;
                                case "cnt":
                                    props[scopeProperty.Name] = +scopeProperty.RawValue;
                                    break;
                                case "cpu":
                                case "skw":
                                case "node":
                                case "ppm":
                                case "ip":
                                case "cy":
                                case "en":
                                case "txt":
                                case "id":
                                case "fname":
                                default:
                                    props[scopeProperty.Name] = scopeProperty.RawValue;
                            }
                            props["__" + scopeProperty.Name] = scopeProperty;
                        }
                        data.push({
                            id: scope.Id,
                            name: scope.ScopeName,
                            type: scope.ScopeType,
                            ...props
                        });
                    }
                }
                return {
                    meta,
                    columns,
                    data
                };
            });
        }

        return this.fetchDetailsNormalizedPromise.then(response => {
            this._rawColumns = response.columns;
            this._graphLookup = {};
            this._subgraphLookup = {};
            var rawData = response.data.filter(row => {
                if (row.type === "graph") this._graphLookup[row.name] = row.id;
                if (row.type === "subgraph") this._subgraphLookup[row.name] = row.id;
                if (!row.id) return false;
                if (this._scopeFilter && row.name !== this._scopeFilter && row.name.indexOf(`${this._scopeFilter}:`) !== 0) return false;
                if (this._metricSelectValues.every(m => row[m] === undefined)) return false;
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
            }
            colArr.sort(d3Ascending);
            this._metricSelectLabel = this._metricSelectValues + (measure ? " (" + measure + ")" : "");
            var options = this.metricsSelect.selectAll("option").data(colArr, function (d) { return d; });
            options.enter().append("option")
                .merge(options)
                .property("value", d => d)
                .property("selected", d => this._metricSelectValues.indexOf(d) >= 0)
                .text(d => d)
                ;
            options.exit().remove();

            let filteredData = rawData.filter((row, i) => row.name !== this._scopeFilter);
            if (this.needsNormalize()) {
                this.chart.yAxisTickFormat(".0%");
                filteredData = this.normalize(filteredData);
            } else {
                this.chart.yAxisTickFormat(".02s");
            }

            this.chart._columnsMetric = {};
            const columns = ["id", ...this._metricSelectValues.map(mv => {
                const retVal = `${mv}(${this._rawColumns[mv].Measure})`;
                this.chart._columnsMetric[retVal] = this._rawColumns[mv];
                return retVal;
            })];

            this.chartPanel
                .columns(columns)
                .data(filteredData.map((row, i) => {
                    return [row.id, ...this._metricSelectValues.map(metric => row[metric]), row];
                }))
                .lazyRender()
                ;

            return [this._metricSelectValues, rawData];
        });
    }

    needsNormalize(): boolean {
        const measures = {};
        this._metricSelectValues.forEach(metricLabel => {
            const metric = this._rawColumns[metricLabel];
            measures[metric.Measure] = true;
        });
        return Object.keys(measures).length > 1;
    }

    normalize(data) {
        const normalizedData = data.map(row => {
            return {
                ...row
            };
        });
        this._metricSelectValues.forEach(metric => {
            var max = d3Max(data.map(row => row[metric]));
            var scale = d3ScaleLinear().domain([0, max]).range([0, 1]);
            normalizedData.forEach(row => {
                row[metric] = scale(row[metric]);
            });
        });
        return normalizedData;
    }

    resizeTimeline() {
        this.timeline
            .resize()
            .lazyRender()
            ;
    }

    resizeChart() {
        this.chartPanel
            .resize()
            .lazyRender()
            ;
    }

    //  Events ---
    click(row, col, sel) {
    }
}
