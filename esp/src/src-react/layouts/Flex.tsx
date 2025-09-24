import * as React from "react";
import { makeStyles } from "@fluentui/react-components";

const GAP_VALUES = [0, 2, 4, 6, 8, 12, 16, 20, 24, 32] as const;
type GapNumericScale = typeof GAP_VALUES[number];

type GapCSSLength = `${number}px` | `${number}rem` | `${number}em` | `${number}%` | `${number}ch` | `${number}vw` | `${number}vh`;

export type GapProp = GapNumericScale | GapCSSLength;

export interface FlexProps extends React.HTMLAttributes<HTMLDivElement> {
    direction?: "row" | "column";
    justify?: "start" | "center" | "end" | "between" | "around" | "evenly";
    align?: "start" | "center" | "end" | "stretch" | "baseline";
    wrap?: boolean;
    inline?: boolean;
    gap?: GapProp;
    gapX?: GapProp;
    gapY?: GapProp;
    grow?: boolean | number;
    shrink?: boolean | number;
    basis?: number | string;
    fullWidth?: boolean;
}
const gapSet = new Set<number>(GAP_VALUES as readonly number[]);

const useStyles = makeStyles({
    base: { display: "flex" },
    inline: { display: "inline-flex" },
    row: { flexDirection: "row" },
    column: { flexDirection: "column" },
    wrap: { flexWrap: "wrap" },
    fullWidth: { width: "100%" },
    // justify
    jStart: { justifyContent: "flex-start" },
    jCenter: { justifyContent: "center" },
    jEnd: { justifyContent: "flex-end" },
    jBetween: { justifyContent: "space-between" },
    jAround: { justifyContent: "space-around" },
    jEvenly: { justifyContent: "space-evenly" },
    // align
    aStart: { alignItems: "flex-start" },
    aCenter: { alignItems: "center" },
    aEnd: { alignItems: "flex-end" },
    aStretch: { alignItems: "stretch" },
    aBaseline: { alignItems: "baseline" },
    // gap classes
    g0: { gap: "0px" },
    g2: { gap: "2px" },
    g4: { gap: "4px" },
    g6: { gap: "6px" },
    g8: { gap: "8px" },
    g12: { gap: "12px" },
    g16: { gap: "16px" },
    g20: { gap: "20px" },
    g24: { gap: "24px" },
    g32: { gap: "32px" }
});

type GapClassKey = `g${GapNumericScale}`;

function isGapClassKey(key: string): key is GapClassKey {
    return /^g(0|2|4|6|8|12|16|20|24|32)$/.test(key);
}

function gapClassName(value: GapProp | undefined): string | undefined {
    if (value === undefined) return;
    if (typeof value === "number" && gapSet.has(value)) return `g${value}`;
    return undefined;
}

function normalizeGap(value: GapProp | undefined): string | undefined {
    if (value === undefined) return undefined;
    if (typeof value === "number") {
        return `${value}px`;
    }
    return value;
}

export const Flex = React.forwardRef<HTMLDivElement, FlexProps>(({
    direction = "row",
    justify,
    align,
    wrap = false,
    inline = false,
    gap,
    gapX,
    gapY,
    grow,
    shrink,
    basis,
    fullWidth = false,
    style,
    className,
    children,
    ...rest
}, ref) => {
    const styles = useStyles();
    const resolvedAlign = align ?? (direction === "column" ? "stretch" : undefined);

    const classes: string[] = [
        styles.base,
        direction === "column" ? styles.column : styles.row
    ];
    if (inline) classes.push(styles.inline);
    if (wrap) classes.push(styles.wrap);
    if (fullWidth) classes.push(styles.fullWidth);

    switch (justify) {
        case "start": classes.push(styles.jStart); break;
        case "center": classes.push(styles.jCenter); break;
        case "end": classes.push(styles.jEnd); break;
        case "between": classes.push(styles.jBetween); break;
        case "around": classes.push(styles.jAround); break;
        case "evenly": classes.push(styles.jEvenly); break;
    }
    switch (resolvedAlign) {
        case "start": classes.push(styles.aStart); break;
        case "center": classes.push(styles.aCenter); break;
        case "end": classes.push(styles.aEnd); break;
        case "stretch": classes.push(styles.aStretch); break;
        case "baseline": classes.push(styles.aBaseline); break;
    }

    const gapCls = gapClassName(gap);
    if (gapCls && isGapClassKey(gapCls)) {
        classes.push(styles[gapCls]);
    }

    const inlineStyle: React.CSSProperties = {};
    if (gap !== undefined && !gapCls) inlineStyle.gap = normalizeGap(gap);
    if (gapX !== undefined) inlineStyle.columnGap = normalizeGap(gapX);
    if (gapY !== undefined) inlineStyle.rowGap = normalizeGap(gapY);
    if (grow !== undefined) inlineStyle.flexGrow = grow === true ? 1 : (typeof grow === "number" ? grow : undefined);
    if (shrink !== undefined) inlineStyle.flexShrink = shrink === true ? 1 : (typeof shrink === "number" ? shrink : undefined);
    if (basis !== undefined) inlineStyle.flexBasis = basis;

    const mergedClassName = [className, ...classes].filter(Boolean).join(" ");
    const mergedStyle = { ...inlineStyle, ...style };

    return <div ref={ref} className={mergedClassName} style={mergedStyle} {...rest}>{children}</div>;
});

Flex.displayName = "Flex";
