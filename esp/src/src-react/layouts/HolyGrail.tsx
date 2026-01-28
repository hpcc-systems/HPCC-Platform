import * as React from "react";
import { makeStyles, mergeClasses } from "@fluentui/react-components";
import { useUserTheme } from "../hooks/theme";

const useStyles = makeStyles({
    root: {
        display: "flex",
        flexDirection: "column",
        minWidth: "0",
        minHeight: "100%",
        overflow: "hidden",
    },
    fullscreen: {
        position: "fixed",
        top: "0",
        left: "0",
        width: "100%",
        height: "100%",
    },
    header: {
        flexGrow: 0,
        flexShrink: 0,
        minWidth: "0",
    },
    body: {
        flexGrow: 1,
        flexShrink: 1,
        display: "flex",
        minWidth: "0",
    },
    left: {
        flexGrow: 0,
        flexShrink: 2,
    },
    main: {
        flexGrow: 1,
        flexShrink: 1,
        flexBasis: "auto",
        minWidth: "1px",
        minHeight: "1px",
    },
    right: {
        flexGrow: 0,
        flexShrink: 2,
    },
    footer: {
        flexGrow: 0,
        flexShrink: 0,
        minWidth: "0",
    },
});

export interface HolyGrailProps {
    header?: any;
    left?: any;
    main?: any;
    right?: any;
    footer?: any;
    fullscreen?: boolean;
}

export const HolyGrail: React.FunctionComponent<HolyGrailProps> = ({
    header,
    left,
    main,
    right,
    footer,
    fullscreen = false
}) => {

    const { themeV9 } = useUserTheme();
    const styles = useStyles();

    return <div
        className={mergeClasses(styles.root, fullscreen && styles.fullscreen)}
        style={fullscreen ? { background: themeV9.colorNeutralBackground1 } : undefined}
    >
        <header className={styles.header}>{header}</header>
        <div className={styles.body}>
            <div className={styles.left}>{left}</div>
            <div className={styles.main}>{main}</div>
            <div className={styles.right}>{right}</div>
        </div>
        <footer className={styles.footer}>{footer}</footer>
    </div>;
};
