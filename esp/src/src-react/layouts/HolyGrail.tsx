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
}

export const HolyGrail: React.FunctionComponent<HolyGrailProps> = ({
    header,
    left,
    main,
    right,
    footer,
    footerStyles = { flex: "0 0", minWidth: 0 }
}) => {

    const [, , isDark] = useUserTheme();
    const [btnHoverBgColor, setBtnHoverBgColor] = React.useState("rgb(175, 217, 255)");
    const [btnDisabledColor, setBtnDisabledColor] = React.useState("rgb(180, 180, 180)");
    const [btnDisabledBgColor, setBtnDisabledBgColor] = React.useState("rgb(238, 240, 242)");

    const layoutStyles = React.useMemo(() => mergeStyleSets({
        header: {
            ".ms-CommandBar": {
                ".ms-Button:hover": {
                    backgroundColor: btnHoverBgColor
                },
                ".ms-Button.is-disabled": {
                    color: btnDisabledColor,
                    ".ms-Icon": {
                        color: btnDisabledColor
                    }
                },
                ".ms-Button.is-disabled:hover": {
                    backgroundColor: btnDisabledBgColor,
                }
            }
        }
    }), [btnDisabledColor, btnDisabledBgColor, btnHoverBgColor]);

    React.useEffect(() => {
        if (isDark) {
            setBtnHoverBgColor("rgb(49, 49, 49)");
            setBtnDisabledColor("rgb(130, 130, 130)");
            setBtnDisabledBgColor("rgb(49, 49, 49)");
        } else {
            setBtnHoverBgColor("rgb(175, 217, 255)");
            setBtnDisabledColor("rgb(180, 180, 180)");
            setBtnDisabledBgColor("rgb(238, 240, 242)");
        }
    }, [isDark]);

    return <div style={{ display: "flex", flexDirection: "column", minWidth: 0, minHeight: "100%", overflow: "hidden" }}>
        <header className={layoutStyles.header} style={{ flex: "0 0", minWidth: 0 }}>{header}</header>
        <div style={{ flex: "1 1", display: "flex", minWidth: 0 }} >
            <div style={{ flex: "0 2" }}>{left}</div>
            <div style={{ flex: "1 1 auto", minWidth: 1, minHeight: 1 }}>{main}</div>
            <div style={{ flex: "0 2" }}>{right}</div>
        </div>
        <footer style={footerStyles}>{footer}</footer>
    </div>;
};
