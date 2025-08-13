import * as React from "react";
import { makeStyles, mergeClasses, tokens } from "@fluentui/react-components";

const useStyles = makeStyles({
    root: {
        display: "grid",
        alignItems: "start",
        alignContent: "start",
        justifyContent: "start",
        placeContent: "start",
        width: "100%",
        boxSizing: "border-box",
    }
});

export interface CardGroupProps {
    children?: React.ReactNode;
    minColumnWidth?: number | string;
    autoRows?: number | string;
    columnGap?: string;
    rowGap?: string;
    paddingInline?: string;
    paddingBlock?: string;
    scrollY?: boolean;
    className?: string;
    style?: React.CSSProperties;
}

export const CardGroup: React.FC<CardGroupProps> = ({
    children,
    minColumnWidth = 280,
    autoRows = 320,
    columnGap = tokens.spacingHorizontalM,
    rowGap = tokens.spacingHorizontalM,
    paddingInline = tokens.spacingHorizontalM,
    paddingBlock = tokens.spacingVerticalM,
    scrollY = false,
    className,
    style
}) => {
    const styles = useStyles();

    const toCssLen = (v: number | string) => typeof v === "number" ? `${v}px` : v;

    const computedStyle: React.CSSProperties = {
        gridTemplateColumns: `repeat(auto-fill, minmax(${toCssLen(minColumnWidth)}, 1fr))`,
        columnGap,
        rowGap,
        paddingInline,
        paddingBlock,
        gridAutoRows: toCssLen(autoRows),
        overflowY: scrollY ? "auto" : undefined,
        minHeight: scrollY ? 0 : undefined,
        ...style
    };

    return <div className={mergeClasses(styles.root, className)} style={computedStyle}>
        {children}
    </div>;
};
