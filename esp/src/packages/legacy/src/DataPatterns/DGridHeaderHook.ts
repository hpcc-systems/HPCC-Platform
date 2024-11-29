import { HTMLWidget, select as d3Select, selectAll as d3SelectAll } from "@hpcc-js/common";
import { Workunit } from "@hpcc-js/comms";
import { Cardinality } from "./Cardinality";
import { PopularPatterns } from "./PopularPatterns";
import { StatChart } from "./StatChart";

export class DGridHeaderHook {

    protected _grid;
    protected _gridID;
    protected _statChart;

    constructor(grid, gridID: string) {
        this._grid = grid;
        this._gridID = gridID;
    }

    createPlaceholder(fieldID) {
        const headerElement = d3Select<HTMLElement, HTMLWidget>(`#${this._gridID} tr:not(.dgrid-spacer-row) .field-${fieldID} > .dgrid-resize-header-container`);
        const rect = headerElement.node().getBoundingClientRect();
        const placeholder = headerElement.append("div") // insert("div", ":first-child")
            .attr("class", "placeholder")
            ;
        return { rect, placeholder };
    }

    render(wu: Workunit): Promise<void> {
        d3SelectAll<HTMLElement, object>(`#${this._gridID} > .dgrid-header tr:not(.dgrid-spacer-row) > th .placeholder`).remove();
        if (wu) {
            return wu.watchUntilComplete().then(wu => {
                return wu.fetchResults();
            }).then(results => {
                return results.length ? results[0].fetchRows() : [];
            }).then(fields => {
                fields.forEach(field => {
                    if (field.is_numeric) {
                        const { rect, placeholder } = this.createPlaceholder(field.attribute);
                        this._statChart = new StatChart()
                            .target(placeholder.node())
                            .resize({ width: rect.width, height: 180 })
                            .mean(field.numeric_mean)
                            .standardDeviation(field.numeric_std_dev)
                            .quartiles([
                                field.numeric_min,
                                field.numeric_lower_quartile,
                                field.numeric_median,
                                field.numeric_upper_quartile,
                                field.numeric_max
                            ])
                            .lazyRender()
                            ;
                    } else if (field.cardinality_breakdown && field.cardinality_breakdown.Row && field.cardinality_breakdown.Row.length) {
                        const { rect, placeholder } = this.createPlaceholder(field.attribute);
                        this._statChart = new Cardinality(field.cardinality_breakdown.Row, false)
                            .target(placeholder.node())
                            .resize({ width: rect.width, height: 180 })
                            .lazyRender()
                            ;
                    } else if (field.popular_patterns && field.popular_patterns.Row && field.popular_patterns.Row.length) {
                        const { rect, placeholder } = this.createPlaceholder(field.attribute);
                        this._statChart = new PopularPatterns(field.popular_patterns.Row.map(row => ({
                            data_pattern: `[${row.data_pattern.trim()}]`,
                            rec_count: row.rec_count
                        })), false)
                            .target(placeholder.node())
                            .resize({ width: rect.width, height: 180 })
                            .lazyRender()
                            ;
                    }
                });
            });
        }
        return Promise.resolve();
    }

    resize(columnId: string) {
        const headerElement = d3Select<HTMLElement, HTMLWidget>(`#${this._gridID} tr:not(.dgrid-spacer-row) th.dgrid-column-${columnId} > .dgrid-resize-header-container`);
        const element = headerElement.select(".common_HTMLWidget");
        if (!element.empty()) {
            const widget = element.datum();
            if (widget) {
                const rect = headerElement.node().getBoundingClientRect();
                widget
                    .resize({ width: rect.width, height: 180 })
                    .lazyRender()
                    ;
            }
        }
    }
}
// DGridHeaderWidget.prototype._class += " eclwatch_DGridHeaderWidget";
