import { color as d3Color, rgb as d3Rgb } from "@hpcc-js/common";

export interface HSVColor {
    h: number;
    s: number;
    v: number;
    a?: number;
}

export function cssColorToHSV(value: string): HSVColor {
    const parsedColor = d3Color(value)?.rgb() ?? d3Rgb("black");
    const red = parsedColor.r / 255;
    const green = parsedColor.g / 255;
    const blue = parsedColor.b / 255;
    const maximum = Math.max(red, green, blue);
    const minimum = Math.min(red, green, blue);
    const delta = maximum - minimum;

    let hue = 0;
    if (delta !== 0) {
        if (maximum === red) {
            hue = 60 * (((green - blue) / delta) % 6);
        } else if (maximum === green) {
            hue = 60 * ((blue - red) / delta + 2);
        } else {
            hue = 60 * ((red - green) / delta + 4);
        }
    }

    return {
        h: hue < 0 ? hue + 360 : hue,
        s: maximum === 0 ? 0 : delta / maximum,
        v: maximum,
        a: parsedColor.opacity
    };
}

export function hsvToHex({ h, s, v }: HSVColor): string {
    const chroma = v * s;
    const hue = h / 60;
    const secondary = chroma * (1 - Math.abs(hue % 2 - 1));
    const match = v - chroma;
    let red = 0;
    let green = 0;
    let blue = 0;

    if (hue < 1) {
        [red, green] = [chroma, secondary];
    } else if (hue < 2) {
        [red, green] = [secondary, chroma];
    } else if (hue < 3) {
        [green, blue] = [chroma, secondary];
    } else if (hue < 4) {
        [green, blue] = [secondary, chroma];
    } else if (hue < 5) {
        [red, blue] = [secondary, chroma];
    } else {
        [red, blue] = [chroma, secondary];
    }

    return d3Rgb((red + match) * 255, (green + match) * 255, (blue + match) * 255).formatHex();
}