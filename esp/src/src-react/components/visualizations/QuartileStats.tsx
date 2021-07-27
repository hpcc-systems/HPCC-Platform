
import * as React from "react";

import { IStackStyles, Stack, Dropdown } from "@fluentui/react";

import { format as d3Format, Palette } from "@hpcc-js/common";

import { QuartileCandlestick, Scatter } from "@hpcc-js/chart";
import { AutosizeHpccJSComponent } from "../../layouts/HpccJSAdapter";

const rainbow = Palette.rainbow("Blues");
const palette = Palette.ordinal("Quartile", [rainbow(100, 0, 100), rainbow(50, 0, 100), rainbow(50, 0, 100), rainbow(75, 0, 100)]);
palette("Std. Dev.");
palette("MinMax");
palette("25%");
palette("50%");

const CANDLE_HEIGHT = 20;   // Pixels
const DOMAIN_PADDING = 10;  // Percentage

type QuartilesType = [number, number, number, number, number];
type Mode = "min_max" | "25_75" | "normal";
type Tick = { label: string, value: number };
type Ticks = Tick[];
type AxisTick = { label: string, value: string };
type AxisTicks = AxisTick[];

interface VizProps {
    mean: number;
    quartiles: QuartilesType;
    standardDeviation: number;
    tickFormat?: string;
    showBell?: boolean;
    showCandlestick?: boolean;
    bellHeight?: number;
    candlestickHeight?: number;
    defaultMode?: Mode;
    isHorizontal?: boolean;
    width?: number;
}

export const QuartileStats: React.FunctionComponent<VizProps> = ({
    mean,
    quartiles,
    standardDeviation,
    tickFormat = ".2e",
    showBell = true,
    showCandlestick = true,
    bellHeight = 48,
    candlestickHeight = 20,
    defaultMode = "min_max",
    isHorizontal = false,
    width
}) => {

    const [mode, setMode] = React.useState<Mode>(defaultMode);

    const modeOptions = [
        { key: "min_max", text: "Min / Max" },
        { key: "25_75", text: "25% / 75%" },
        { key: "normal", text: "Normal" }
    ];

    const bellChart = React.useRef(
        new Scatter()
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
    ).current;

    const candleChart = React.useRef(
        new QuartileCandlestick()
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
    ).current;

    const [domainLow, domainHigh] = domain(mode);
    const padding = (domainHigh - domainLow) * (DOMAIN_PADDING / 100);

    const tickFormatter = myFormatter(tickFormat);

    if (showBell) {

        bellChart
            .xAxisDomainLow(domainLow - padding)
            .xAxisDomainHigh(domainHigh + padding)
            .xAxisTicks(bellTicks(mode))
            .data([
                [stdDev(-4), 0],
                [stdDev(-3), 0.3],
                [stdDev(-2), 5],
                [stdDev(-1), 68],
                [stdDev(0), 100],
                [stdDev(1), 68],
                [stdDev(2), 5],
                [stdDev(3), 0.3],
                [stdDev(4), 0]
            ])
            ;
    }

    const candleX = bellChart.dataPos(quartiles[0]) + 2;
    const candleW = bellChart.dataPos(quartiles[4]) - candleX;
    
    candleChart
        .pos({ x: candleX + candleW / 2, y: CANDLE_HEIGHT / 2 })
        .width(Math.max(candleW, 1))
        .data(quartiles)
        ;

    const containerStackStyles: IStackStyles = {
        root: {
            height: 46,
            width: width ?? "auto",
            textAlign: "right"
        },
    };
    const dropdownStackStyles: IStackStyles = {
        root: {
            height: 32,
            width: 120
        },
    };
    const bellStackStyles: IStackStyles = {
        root: {
            height: bellHeight,
            width: 200
        },
    };
    const candlestickStackStyles: IStackStyles = {
        root: {
            height: candlestickHeight,
            width: Math.max(200 - (candleX * 2), 30),
            marginLeft: candleX,
            marginRight: candleX
        },
    };

    let modeDropdown;
    let bellComponent;
    if(showBell){
        modeDropdown = <Stack.Item grow={0} styles={dropdownStackStyles}>
            <Dropdown
                key={mode}
                // label="Mode"
                defaultSelectedKey={mode}
                options={modeOptions}
                onChange={(ev, row) => {
                    setMode(row.key as Mode);
                }}
                placeholder="Mode"
            />
        </Stack.Item>;
        bellComponent = <Stack.Item grow={0} styles={bellStackStyles}>
            <AutosizeHpccJSComponent widget={bellChart} fixedHeight={bellHeight + "px"} />
        </Stack.Item>;
    }

    let candlestickComponent;
    if(showCandlestick){
        candlestickComponent = <Stack.Item grow={1} styles={candlestickStackStyles}>
            <AutosizeHpccJSComponent widget={candleChart} fixedHeight={candlestickHeight + "px"} />
        </Stack.Item>;
    }
    const horizontal = {horizontal:isHorizontal};
    return <Stack styles={containerStackStyles} {...horizontal}>
        {modeDropdown}
        {bellComponent}
        {candlestickComponent}
    </Stack>;

    function myFormatter(format: string): (num: number) => string {
        const formatter = d3Format(format);
        return function (num: number) {
            const strVal = (Math.round(num * 100) / 100).toString();
            if (strVal.length <= 4) return strVal;
            return formatter(num);
        };
    }

    function stdDev(degrees: number): number {
        return mean + degrees * standardDeviation;
    }

    function domain(mode: Mode): [number, number] {
        switch (mode) {
            case "25_75":
                return [quartiles[1], quartiles[3]];
            case "normal":
                return [
                    stdDev(-4),
                    stdDev(4)
                ];
            case "min_max":
            default:
                return [quartiles[0], quartiles[4]];
        }
    }

    function bellTicks(mode: Mode): AxisTicks {
        let ticks: Ticks;
        switch (mode) {
            case "25_75":
                ticks = [
                    { label: tickFormatter(quartiles[1]), value: quartiles[1] },
                    { label: tickFormatter(quartiles[2]), value: quartiles[2] },
                    { label: tickFormatter(quartiles[3]), value: quartiles[3] }
                ];
                break;
            case "normal":
                ticks = [
                    { label: "" + stdDev(-4), value: stdDev(-4) },
                    { label: "-3σ", value: stdDev(-3) },
                    { label: "-2σ", value: stdDev(-2) },
                    { label: "-1σ", value: stdDev(-1) },
                    { label: "" + stdDev(0), value: stdDev(0) },
                    { label: "+1σ", value: stdDev(1) },
                    { label: "+2σ", value: stdDev(2) },
                    { label: "+3σ", value: stdDev(3) },
                    { label: "" + stdDev(4), value: stdDev(4) }
                ];
                break;
            case "min_max":
            default:
                ticks = [
                    { label: tickFormatter(quartiles[0]), value: quartiles[0] },
                    { label: tickFormatter(quartiles[1]), value: quartiles[1] },
                    { label: tickFormatter(quartiles[2]), value: quartiles[2] },
                    { label: tickFormatter(quartiles[3]), value: quartiles[3] },
                    { label: tickFormatter(quartiles[4]), value: quartiles[4] }
                ];
        }

        const [domainLow, domainHigh] = domain(mode);
        return ticks
            .filter(sd => sd.value >= domainLow && sd.value <= domainHigh)
            .map(sd => ({ label: sd.label, value: sd.value.toString() }))
            ;
    }
};
