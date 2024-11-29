import { Widget } from "@hpcc-js/common";
import { Grid } from "@hpcc-js/layout";
import { AttributeDesc } from "./AttributeDesc";
import { Cardinality } from "./Cardinality";
import { config } from "./config";
import { DPWorkunit } from "./DPWorkunit";
import { NAWidget } from "./NAWidget";
import { PopularPatterns } from "./PopularPatterns";
import { NumericStatsWidget, StatChart, StringStatsWidget } from "./StatChart";

export class Report extends Grid {

    private _data: any[];
    private _fixedHeight?: number;
    private _showBreakdownColumn = true;
    private _showPopularPatternsColumn = true;
    private _showQuartileColumn = true;

    constructor() {
        super();
        this
            .gutter(12)
            .surfaceShadow(false)
            .surfacePadding("0")
            .surfaceBorderWidth(0)
            ;
    }

    private _wu: DPWorkunit;
    wu(): DPWorkunit;
    wu(_: DPWorkunit): this;
    wu(_?: DPWorkunit): DPWorkunit | this {
        if (_ === void 0) return this._wu;
        this._wu = _;
        return this;
    }

    enter(domNode, element) {
        this._fixedHeight = this._data.length * config.rowHeight;
        domNode.style.height = this._fixedHeight + "px";
        this.height(this._fixedHeight);
        super.enter(domNode, element);
        const statsDataWidth = this.calcStatsWidgetDataColumnWidth();
        this._showQuartileColumn = this._data.filter(row => row.is_numeric).length > 0;
        this._showBreakdownColumn = this._data.filter(row => row.cardinality_breakdown.Row.length > 0).length > 0;
        this._showPopularPatternsColumn = this._data.filter(row => row.popular_patterns.Row.length > 0).length > 0;
        let colCount = 3;
        this._data.forEach((row, i) => {
            const cc = this.enterDataRow(row, i, { statsDataWidth });
            if (cc > colCount) {
                colCount = cc;
            }
        });
        element.classed("report-col-count-" + colCount, true);
    }

    enterDataRow(row, i, ext) {
        const y = i * config.rowHeight;

        let c = 2;
        let cPos = 0;
        const cStep = 12;
        this.setContent(y, cPos, new AttributeDesc(row), undefined, config.rowHeight, cStep * config.colRatios.attributeDesc);
        cPos += cStep * config.colRatios.attributeDesc;
        this.setContent(y, cPos, getStatsWidget(row, ext.statsDataWidth), undefined, config.rowHeight, cStep * config.colRatios.statsData);
        cPos += cStep * config.colRatios.statsData;
        if (this._showQuartileColumn) {
            this.setContent(y, cPos, getQuartileWidget(row), undefined, config.rowHeight, cStep * config.colRatios.quartile);
            cPos += cStep * config.colRatios.quartile;
            ++c;
        }
        if (this._showBreakdownColumn) {
            this.setContent(y, cPos, getBreakdownWidget(row), undefined, config.rowHeight, cStep * config.colRatios.breakdown);
            cPos += cStep * config.colRatios.breakdown;
            ++c;
        }
        if (this._showPopularPatternsColumn) {
            this.setContent(y, cPos, getPopularPatternsWidget(row), undefined, config.rowHeight, cStep * config.colRatios.popularPatterns);
            cPos += cStep * config.colRatios.popularPatterns;
            ++c;
        }
        return c;

        function getBreakdownWidget(row) {
            if (row.cardinality_breakdown.Row.length > 0) {
                return new Cardinality(row.cardinality_breakdown.Row);
            } else {
                return new NAWidget("Cardinality", "N/A");
            }
        }
        function getStatsWidget(row, dataWidth) {
            if (row.is_numeric) {
                return new NumericStatsWidget(row, dataWidth);
            } else if (row.popular_patterns.Row.length > 0) {
                return new StringStatsWidget(row);
            }
            return new AttributeDesc(row);
        }
        function getPopularPatternsWidget(row) {
            if (row.popular_patterns.Row.length > 0) {
                return new PopularPatterns(row.popular_patterns.Row);
            } else {
                return new NAWidget("Popular Patterns", "N/A");
            }
        }
        function getQuartileWidget(row) {
            if (row.is_numeric) {
                return new StatChart()
                    // .columns(["Min", "25%", "50%", "75%", "Max"])
                    .mean(row.numeric_mean)
                    .standardDeviation(row.numeric_std_dev)
                    .quartiles([
                        row.numeric_min,
                        row.numeric_lower_quartile,
                        row.numeric_median,
                        row.numeric_upper_quartile,
                        row.numeric_max
                    ])
                    ;
            } else {
                return new NAWidget("Quartile", "N/A");
            }
        }
    }

    update(domNode, element) {
        if (this.width() < 800) {
            this.width(800);
        }
        this.height(this._fixedHeight);
        super.update(domNode, element);
    }

    resize(size?) {
        const retVal = super.resize(size);
        if (this._placeholderElement) {
            this._placeholderElement
                .style("height", (this._fixedHeight ? this._fixedHeight : this._size.height) + "px")
                ;
        }
        return retVal;
    }

    render(callback?: (w: Widget) => void): this {
        this._wu.fetchResults().then(rows => {
            this._data = rows;
            super.render(w => {
                if (callback) {
                    callback(this);
                }
            });
        });
        return this;
    }

    calcStatsWidgetDataColumnWidth() {
        let ret = 0;
        this._data.forEach(row => {
            const _w = Math.max(
                this.textSize(row.numeric_mean).width,
                this.textSize(row.numeric_std_dev).width,
                this.textSize(row.numeric_min).width,
                this.textSize(row.numeric_lower_quartile).width,
                this.textSize(row.numeric_median).width,
                this.textSize(row.numeric_upper_quartile).width,
                this.textSize(row.numeric_max).width
            );
            if (_w > ret) {
                ret = _w;
            }
        });
        return ret;
    }
}
Report.prototype._class += " eclwatch_DPReport";
