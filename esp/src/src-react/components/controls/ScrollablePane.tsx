import * as React from "react";
import { makeStyles, tokens } from "@fluentui/react-components";

const useStyles = makeStyles({
    sticky: {
        backgroundColor: tokens.colorNeutralBackground1,
    },
});

// ---------------------------------------------------------------------------
//  ScrollbarVisibility — CSS overflow values
// ---------------------------------------------------------------------------
export const ScrollbarVisibility = {
    auto: "auto",
    always: "scroll",
    hidden: "hidden",
} as const;

export type ScrollbarVisibility = typeof ScrollbarVisibility[keyof typeof ScrollbarVisibility];

// ---------------------------------------------------------------------------
//  StickyPositionType
// ---------------------------------------------------------------------------
export const StickyPositionType = {
    Header: "Header",
    Footer: "Footer",
    Both: "Both",
} as const;

export type StickyPositionType = typeof StickyPositionType[keyof typeof StickyPositionType];

// ---------------------------------------------------------------------------
//  ScrollablePane — overflow container that enables CSS sticky children
// ---------------------------------------------------------------------------
interface ScrollablePaneProps {
    scrollbarVisibility?: ScrollbarVisibility;
    style?: React.CSSProperties;
    className?: string;
    children?: React.ReactNode;
}

export const ScrollablePane: React.FunctionComponent<ScrollablePaneProps> = ({
    scrollbarVisibility = ScrollbarVisibility.auto,
    style,
    className,
    children,
}) => (
    <div
        className={className}
        style={{ position: "relative", overflow: scrollbarVisibility, height: "100%", ...style }}
    >
        {children}
    </div>
);

// ---------------------------------------------------------------------------
//  Sticky — CSS position:sticky header or footer
// ---------------------------------------------------------------------------
interface StickyProps {
    stickyPosition?: StickyPositionType;
    style?: React.CSSProperties;
    className?: string;
    children?: React.ReactNode;
}

export const Sticky: React.FunctionComponent<StickyProps> = ({
    stickyPosition = StickyPositionType.Header,
    style,
    className,
    children,
}) => {
    const styles = useStyles();
    return (
        <div
            className={`${styles.sticky}${className ? ` ${className}` : ""}`}
            style={{
                position: "sticky",
                top: stickyPosition !== StickyPositionType.Footer ? 0 : undefined,
                bottom: stickyPosition !== StickyPositionType.Header ? 0 : undefined,
                zIndex: 1,
                ...style,
            }}
        >
            {children}
        </div>
    );
};
