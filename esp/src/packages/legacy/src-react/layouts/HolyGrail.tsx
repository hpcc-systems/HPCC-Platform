import * as React from "react";
import { mergeStyleSets } from "@fluentui/react";
import { useUserTheme } from "../hooks/theme";

export interface HolyGrailProps {
    header?: any;
    left?: any;
    main?: any;
    right?: any;
    footer?: any;
    footerStyles?: any;
    fullscreen?: boolean;
}

export const HolyGrail: React.FunctionComponent<HolyGrailProps> = ({
    header,
    left,
    main,
    right,
    footer,
    footerStyles = { flex: "0 0", minWidth: 0 },
    fullscreen = false
}) => {

    const { themeV9 } = useUserTheme();

    const layoutStyles = React.useMemo(() => mergeStyleSets({
        fullscreen: {
            position: "fixed",
            top: "0",
            left: "0",
            width: "100%",
            height: "100%",
            background: themeV9.colorNeutralBackground1,
        },
        normal: {
        }
    }), [themeV9.colorNeutralBackground1]);

    return <div className={fullscreen ? layoutStyles.fullscreen : layoutStyles.normal} style={{ display: "flex", flexDirection: "column", minWidth: 0, minHeight: "100%", overflow: "hidden" }}>
        <header style={{ flex: "0 0", minWidth: 0 }}>{header}</header>
        <div style={{ flex: "1 1", display: "flex", minWidth: 0 }} >
            <div style={{ flex: "0 2" }}>{left}</div>
            <div style={{ flex: "1 1 auto", minWidth: 1, minHeight: 1 }}>{main}</div>
            <div style={{ flex: "0 2" }}>{right}</div>
        </div>
        <footer style={footerStyles}>{footer}</footer>
    </div>;
};
