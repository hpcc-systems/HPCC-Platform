import * as React from "react";
import { FontIcon, mergeStyleSets, Text, ThemeProvider } from "@fluentui/react";
import { FluentProvider } from "@fluentui/react-components";
import { Palette } from "@hpcc-js/common";
import { WUStateID } from "@hpcc-js/comms";
import { useWorkunit } from "../../hooks/workunit";
import { useUserTheme } from "../../hooks/theme";

const GOLDEN_RATIO = 1.618033988749894;

const lgHeight = 32;
const lgOverlayHeight = lgHeight - lgHeight / GOLDEN_RATIO;
const lg = mergeStyleSets({
    placeholder: {
        display: "inline-block",
        position: "relative",
        height: lgHeight,
        width: lgHeight,
    },
    iconPlaceholder: {
        position: "absolute",
        height: lgHeight,
        width: lgHeight,
    },
    icon: {
        position: "absolute",
        fontSize: lgHeight,
        height: lgHeight,
        width: lgHeight,
    },
    overlayPlaceholder: {
        position: "absolute",
        right: -2,
        bottom: -2,
        width: lgOverlayHeight + 2,
        height: lgOverlayHeight + 2,
        border: 2,
        borderStyle: "solid",
        borderRadius: "50%",
        backgroundClip: "border-box",
        textAlign: "center",
    },
    overlay: {
        fontSize: lgOverlayHeight,
        lineHeight: lgOverlayHeight + 2,
        verticalAlign: "top",
    }
});

const mdHeight = 22;
const mdOverlayHeight = mdHeight - mdHeight / GOLDEN_RATIO;
const md = mergeStyleSets(lg, {
    placeholder: {
        height: mdHeight,
        width: mdHeight,
    },
    iconPlaceholder: {
        height: mdHeight,
        width: mdHeight,
    },
    icon: {
        fontSize: mdHeight,
        height: mdHeight,
        width: mdHeight,
    },
    overlayPlaceholder: {
        height: mdOverlayHeight + 2,
        width: mdOverlayHeight + 2,
    },
    overlay: {
        fontSize: mdHeight - mdHeight / GOLDEN_RATIO,
        lineHeight: mdOverlayHeight + 2,
    }
});

const smHeight = 16;
const smOverlayHeight = smHeight - smHeight / GOLDEN_RATIO + 2;
const smOverlayBorder = 1;
const sm = mergeStyleSets(lg, {
    placeholder: {
        height: smHeight,
        width: smHeight,
    },
    iconPlaceholder: {
        height: smHeight,
        width: smHeight,
    },
    icon: {
        fontSize: smHeight,
        height: smHeight,
        width: smHeight,
    },
    overlayPlaceholder: {
        height: smOverlayHeight + smOverlayBorder,
        width: smOverlayHeight + smOverlayBorder,
        border: smOverlayBorder
    },
    overlay: {
        fontSize: smOverlayHeight,
        lineHeight: smOverlayHeight + smOverlayBorder,
    }
});

type SizeT = "lg" | "md" | "sm";
interface StateIconProps {
    iconName?: string;
    overlayName?: string;
    overlayColor?: string;
    title?: string;
    size?: SizeT;
}

const classSet = {
    "lg": lg,
    "md": md,
    "sm": sm
};

export const StateIcon: React.FunctionComponent<StateIconProps> = ({
    iconName = "StatusCircleQuestionMark",
    overlayName = "",
    overlayColor = "green",
    title = "",
    size = "md"
}) => {
    const ss = classSet[size];

    const { theme, themeV9 } = useUserTheme();

    const overlayIconColor = Palette.textColor(overlayColor);

    return <FluentProvider theme={themeV9} className={ss.placeholder} >
        <ThemeProvider theme={theme} className={ss.placeholder} title={title}>
            <span className={ss.iconPlaceholder}>
                <FontIcon iconName={iconName} className={ss.icon} />
            </span>
            {overlayName &&
                <span className={ss.overlayPlaceholder} style={{ backgroundColor: overlayColor, borderColor: theme.palette.white }}>
                    <FontIcon iconName={overlayName} className={ss.overlay} style={{ color: overlayIconColor }} />
                </span>
            }
        </ThemeProvider>
    </FluentProvider>;
};

interface WorkunitPersonaProps {
    wuid: string;
    showProtected?: boolean;
    showWuid?: boolean;
    size?: SizeT;
}

export const WorkunitPersona: React.FunctionComponent<WorkunitPersonaProps> = ({
    wuid,
    showProtected = true,
    showWuid = true,
    size = "sm"
}) => {

    const [workunit] = useWorkunit(wuid);
    const [overlayName, setOverlayName] = React.useState("");
    const [overlayColor, setOverlayColor] = React.useState("");
    const { theme, themeV9 } = useUserTheme();

    React.useEffect(() => {
        switch (workunit?.StateID) {
            case WUStateID.Compiling:
            case WUStateID.Compiled:
                setOverlayName("LightningBolt");
                setOverlayColor(theme.semanticColors.warningIcon);
                break;
            case WUStateID.DebugRunning:
            case WUStateID.Running:
                setOverlayName("Play");
                setOverlayColor(theme.semanticColors.warningIcon);
                break;
            case WUStateID.Completed:
                setOverlayName("SkypeCheck");
                setOverlayColor(theme.semanticColors.successIcon);
                break;
            case WUStateID.Aborted:
            case WUStateID.Failed:
                setOverlayName("Warning");
                setOverlayColor(theme.semanticColors.errorIcon);
                break;
            case WUStateID.Aborting:
                setOverlayName("SkypeClock");
                setOverlayColor(theme.semanticColors.errorIcon);
                break;
            case WUStateID.Submitted:
            case WUStateID.Scheduled:
                setOverlayName("SkypeClock");
                setOverlayColor(theme.semanticColors.warningIcon);
                break;
            case WUStateID.UploadingFiled:
                setOverlayName("Upload");
                setOverlayColor(theme.semanticColors.warningIcon);
                break;
            case WUStateID.Wait:
            case WUStateID.Blocked:
                setOverlayName("Blocked");
                setOverlayColor(theme.semanticColors.warningIcon);
                break;
            case WUStateID.DebugPaused:
            case WUStateID.Paused:
                setOverlayName("Pause");
                setOverlayColor(theme.semanticColors.warningIcon);
                break;
            case WUStateID.Archived:
                setOverlayName("Archive");
                setOverlayColor(theme.semanticColors.successIcon);
                break;
            case WUStateID.LAST:
            case WUStateID.Unknown:
            case WUStateID.NotFound:
            default:
                setOverlayName("StatusCircleQuestionMark");
                setOverlayColor(theme.semanticColors.errorIcon);
                break;
        }
    }, [workunit, workunit?.StateID, theme.semanticColors.errorIcon, theme.semanticColors.successIcon, theme.semanticColors.warningIcon]);

    return <FluentProvider theme={themeV9} style={{ marginRight: 4, display: "inline-block" }}>
        <ThemeProvider theme={theme} title={workunit?.State}>
            {showProtected &&
                <span style={{ marginLeft: 8, marginRight: 2 }}>
                    <StateIcon iconName={workunit?.Protected ? "LockSolid" : "Unlock"} size={size} />
                </span>
            }
            <StateIcon iconName="Settings" overlayName={overlayName} overlayColor={overlayColor} size={size} />
            {showWuid &&
                <Text variant="xLarge" style={{ fontWeight: "bold", paddingLeft: "4px" }}>{wuid}</Text>
            }
        </ThemeProvider>
    </FluentProvider>;
};