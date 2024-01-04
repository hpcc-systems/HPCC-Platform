/* eslint-disable @typescript-eslint/no-unsafe-declaration-merging */
import { QuartileCandlestick, Scatter } from "@hpcc-js/chart";
import { format as d3Format, HTMLWidget, Palette } from "@hpcc-js/common";
import { StyledTable } from "@hpcc-js/html";

const rainbow = Palette.rainbow("Blues");
const palette = Palette.ordinal("Quartile", [rainbow(100, 0, 100), rainbow(50, 0, 100), rainbow(50, 0, 100), rainbow(75, 0, 100)]);
palette("Std. Dev.");
palette("MinMax");
palette("25%");
palette("50%");

type QuertilesType = [number, number, number, number, number];
type Mode = "min_max" | "25_75" | "normal";
type Tick = { label: string, value: number };
type Ticks = Tick[];
type AxisTick = { label: string, value: string };
type AxisTicks = AxisTick[];

const CANDLE_HEIGHT = 20;   // Pixels
const DOMAIN_PADDING = 10;  // Percentage

function myFormatter(format: string): (num: number) => string {
    const formatter = d3Format(format);
    return function (num: number) {
        const strVal = (Math.round(num * 100) / 100).toString();
        if (strVal.length <= 4) return strVal;
        return formatter(num);
    };
}

export class StatChart extends HTMLWidget {

    private _selectMode: any;
    private _tickFormatter: (_: number) => string;

    private _bellCurve = new Scatter()
        .columns(["", "Std. Dev."])
        .paletteID("Quartile")
        .interpolate_default("basis")
        .pointSize(0)
        .xAxisType("linear")
        .xAxisOverlapMode("none")
        .xAxisTickFormat(".2s")
        .yAxisHidden(true)
        .yAxisDomainLow(0)
        .yAxisDomainHigh(110)
        .yAxisGuideLines(false)
        ;

    private _candle = new QuartileCandlestick()
        .columns(["Min", "25%", "50%", "75%", "Max"])
        .edgePadding(0)
        .candleWidth(CANDLE_HEIGHT - 4)
        .roundedCorners(1)
        .lineWidth(1)
        .upperTextRotation(-90)
        .lowerTextRotation(-90)
        .labelFontSize(0)
        .valueFontSize(0)
        .lineColor(rainbow(90, 0, 100))
        .innerRectColor(rainbow(10, 0, 100))
        ;

    private stdDev(degrees: number): number {
        return this.mean() + degrees * this.standardDeviation();
    }

    private formatStdDev(degrees: number): string {
        return this._tickFormatter(this.stdDev(degrees));
    }

    private quartile(q: 0 | 1 | 2 | 3 | 4): number {
        return this.quartiles()[q];
    }

    private formatQ(q: 0 | 1 | 2 | 3 | 4): string {
        return this._tickFormatter(this.quartile(q));
    }

    private domain(mode: Mode): [number, number] {
        switch (mode) {
            case "25_75":
                return [this.quartile(1), this.quartile(3)];
            case "normal":
                return [this.stdDev(-4), this.stdDev(4)];
            case "min_max":
            default:
                return [this.quartile(0), this.quartile(4)];
        }
    }

    enter(domNode, element) {
        super.enter(domNode, element);

        this._bellCurve.target(element.append("div").node());

        this._candle.target(element.append("div").node());

        this._selectMode = element.append("div")
            .style("position", "absolute")
            .style("top", "0px")
            .style("right", "0px").append("select")
            .on("change", () => this.render())
            ;
        this._selectMode.append("option").attr("value", "min_max").text("Min / Max");
        this._selectMode.append("option").attr("value", "25_75").text("25% / 75%");
        this._selectMode.append("option").attr("value", "normal").text("Normal");
    }

    private bellTicks(mode: Mode): AxisTicks {
        let ticks: Ticks;
        switch (mode) {
            case "25_75":
                ticks = [
                    { label: this.formatQ(1), value: this.quartile(1) },
                    { label: this.formatQ(2), value: this.quartile(2) },
                    { label: this.formatQ(3), value: this.quartile(3) }
                ];
                break;
            case "normal":
                ticks = [
                    { label: this.formatStdDev(-4), value: this.stdDev(-4) },
                    { label: "-3σ", value: this.stdDev(-3) },
                    { label: "-2σ", value: this.stdDev(-2) },
                    { label: "-1σ", value: this.stdDev(-1) },
                    { label: this.formatStdDev(0), value: this.stdDev(0) },
                    { label: "+1σ", value: this.stdDev(1) },
                    { label: "+2σ", value: this.stdDev(2) },
                    { label: "+3σ", value: this.stdDev(3) },
                    { label: this.formatStdDev(4), value: this.stdDev(4) }
                ];
                break;
            case "min_max":
            default:
                ticks = [
                    { label: this.formatQ(0), value: this.quartile(0) },
                    { label: this.formatQ(1), value: this.quartile(1) },
                    { label: this.formatQ(2), value: this.quartile(2) },
                    { label: this.formatQ(3), value: this.quartile(3) },
                    { label: this.formatQ(4), value: this.quartile(4) }
                ];
        }

        const [domainLow, domainHigh] = this.domain(this._selectMode.node().value);
        return ticks
            .filter(sd => sd.value >= domainLow && sd.value <= domainHigh)
            .map(sd => ({ label: sd.label, value: sd.value.toString() }))
            ;
    }

    updateBellCurve() {
        const mode = this._selectMode.node().value;
        const [domainLow, domainHigh] = this.domain(mode);
        const padding = (domainHigh - domainLow) * (DOMAIN_PADDING / 100);

        this._bellCurve
            .xAxisDomainLow(domainLow - padding)
            .xAxisDomainHigh(domainHigh + padding)
            .xAxisTicks(this.bellTicks(mode))
            .data([
                [this.stdDev(-4), 0],
                [this.stdDev(-3), 0.3],
                [this.stdDev(-2), 5],
                [this.stdDev(-1), 68],
                [this.stdDev(0), 100],
                [this.stdDev(1), 68],
                [this.stdDev(2), 5],
                [this.stdDev(3), 0.3],
                [this.stdDev(4), 0]
            ])
            .resize({ width: this.width(), height: this.height() - CANDLE_HEIGHT })
            .render()
            ;
    }

    updateCandle() {
        const candleX = this._bellCurve.dataPos(this.quartile(0));
        const candleW = this._bellCurve.dataPos(this.quartile(4)) - candleX;
        this._candle
            .resize({ width: this.width(), height: CANDLE_HEIGHT })
            .pos({ x: candleX + candleW / 2, y: CANDLE_HEIGHT / 2 })
            .width(candleW)
            .data(this.quartiles())
            .render()
            ;
    }

    update(domNode, element) {
        super.update(domNode, element);
        this._tickFormatter = myFormatter(this.tickFormat());
        this.updateBellCurve();
        this.updateCandle();
    }
}
export interface StatChart {
    tickFormat(): string;
    tickFormat(_: string): this;
    mean(): number;
    mean(_: number): this;
    standardDeviation(): number;
    standardDeviation(_: number): this;
    quartiles(): QuertilesType;
    quartiles(_: QuertilesType): this;
}
StatChart.prototype.publish("tickFormat", ".2e", "string", "X-Axis Tick Format");
StatChart.prototype.publish("mean", 0, "number", "Mean");
StatChart.prototype.publish("standardDeviation", 0, "number", "Standard Deviation");
StatChart.prototype.publish("quartiles", [0, 0, 0, 0, 0], "array", "Quartiles");

export class NumericStatsWidget extends StyledTable {
    constructor(row, dataWidth) {
        super();

        this
            .tbodyColumnStyles([
                { "font-weight": "bold", "text-align": "right", "width": "100px" },
                { "font-weight": "normal", "width": dataWidth + "px" },
                { "font-weight": "normal", "width": "auto" }
            ])
            .data([
                ["Mean", row.numeric_mean, ""],
                ["Std. Deviation", row.numeric_std_dev, ""],
                ["", "", ""],
                ["Quartiles", row.numeric_min, "Min"],
                ["", row.numeric_lower_quartile, "25%"],
                ["", row.numeric_median, "50%"],
                ["", row.numeric_upper_quartile, "75%"],
                ["", row.numeric_max, "Max"]
            ])
            ;
    }
}

export class StringStatsWidget extends StyledTable {
    constructor(row) {
        super();
        return new StyledTable()
            .fontColor("inherit")
            .tbodyColumnStyles([
                { "font-weight": "bold", "text-align": "right", "width": "100px" },
                { "font-weight": "normal", "width": "auto" }
            ])
            .data([
                ["Min Length", row.min_length],
                ["Avg Length", row.ave_length],
                ["Max Length", row.max_length]
            ])
            ;
    }
}
