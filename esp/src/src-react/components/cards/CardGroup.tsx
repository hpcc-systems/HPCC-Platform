import * as React from "react";
import { makeStyles, mergeClasses, tokens } from "@fluentui/react-components";

const useStyles = makeStyles({
    root: {
        display: "grid",
        alignItems: "start",
        alignContent: "start",
        justifyContent: "start",
        width: "100%",
        boxSizing: "border-box",
    },
    flexRoot: {
        display: "flex",
        flexDirection: "column",
        width: "100%",
        boxSizing: "border-box",
    },
    flexCard: {
        width: "100%",
        height: "auto"
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
    listMode?: boolean;
}

export const CardGroup: React.FunctionComponent<CardGroupProps> = ({
    children,
    minColumnWidth = 280,
    autoRows = 320,
    columnGap = tokens.spacingHorizontalM,
    rowGap = tokens.spacingHorizontalM,
    paddingInline = tokens.spacingHorizontalM,
    paddingBlock = tokens.spacingVerticalM,
    scrollY = false,
    className,
    style,
    listMode = false
}) => {
    const styles = useStyles();

    const toCssLen = (v: number | string) => typeof v === "number" ? `${v}px` : v;

    if (listMode) {
        const computedStyle: React.CSSProperties = {
            gap: rowGap,
            paddingInline,
            paddingBlock,
            overflowY: scrollY ? "auto" : undefined,
            minHeight: scrollY ? 0 : undefined,
            ...style
        };
        return <div className={mergeClasses(styles.flexRoot, className)} style={computedStyle}>
            {React.Children.map(children, child =>
                child ? <div className={styles.flexCard}>{child}</div> : null
            )}
        </div>;
    }

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
