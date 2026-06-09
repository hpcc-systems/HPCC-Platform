import * as React from "react";
import { FluentProvider, makeStyles, Text } from "@fluentui/react-components";
import {
    ArchiveRegular, CheckmarkCircleRegular, ClockRegular, FlashRegular,
    LockClosedFilled, LockOpenRegular, PauseCircleRegular, PlayRegular,
    ProhibitedRegular, QuestionCircleRegular, SettingsRegular, WarningRegular
} from "@fluentui/react-icons";
import { Palette } from "@hpcc-js/common";
import { WUStateID } from "@hpcc-js/comms";
import { useWorkunit } from "../../hooks/workunit";
import { useUserTheme } from "../../hooks/theme";

const ICON_MAP: Record<string, React.FunctionComponent<{ className?: string; style?: React.CSSProperties }>> = {
    "Archive": ArchiveRegular,
    "Blocked": ProhibitedRegular,
    "LightningBolt": FlashRegular,
    "LockSolid": LockClosedFilled,
    "Pause": PauseCircleRegular,
    "Play": PlayRegular,
    "Settings": SettingsRegular,
    "SkypeCheck": CheckmarkCircleRegular,
    "SkypeClock": ClockRegular,
    "StatusCircleQuestionMark": QuestionCircleRegular,
    "Unlock": LockOpenRegular,
    "Warning": WarningRegular,
};

function renderStateIcon(iconName: string, className?: string, style?: React.CSSProperties): React.ReactElement {
    const IconComponent = ICON_MAP[iconName] ?? QuestionCircleRegular;
    return <IconComponent className={className} style={style} />;
}

//  v8 fluentui semanticColors equivalents — preserved as literal hex so the
//  hpcc-js Palette.textColor() helper can compute contrast against them.
const WARNING_ICON = "#797775";
const SUCCESS_ICON = "#107C10";
const ERROR_ICON = "#A80000";
const WHITE = "#ffffff";

const GOLDEN_RATIO = 1.618033988749894;

const lgHeight = 32;
const lgOverlayHeight = lgHeight - lgHeight / GOLDEN_RATIO;
const useLgStyles = makeStyles({
    placeholder: { display: "inline-block", position: "relative", height: `${lgHeight}px`, width: `${lgHeight}px` },
    iconPlaceholder: { position: "absolute", height: `${lgHeight}px`, width: `${lgHeight}px` },
    icon: { position: "absolute", fontSize: `${lgHeight}px`, height: `${lgHeight}px`, width: `${lgHeight}px` },
    overlayPlaceholder: {
        position: "absolute", right: "-2px", bottom: "-2px",
        width: `${lgOverlayHeight + 2}px`, height: `${lgOverlayHeight + 2}px`,
        borderTopWidth: "2px", borderRightWidth: "2px", borderBottomWidth: "2px", borderLeftWidth: "2px",
        borderTopStyle: "solid", borderRightStyle: "solid", borderBottomStyle: "solid", borderLeftStyle: "solid",
        borderRadius: "50%",
        backgroundClip: "border-box", textAlign: "center",
    },
    overlay: { fontSize: `${lgOverlayHeight}px`, lineHeight: `${lgOverlayHeight + 2}px`, verticalAlign: "top" }
});

const mdHeight = 22;
const mdOverlayHeight = mdHeight - mdHeight / GOLDEN_RATIO;
const useMdStyles = makeStyles({
    placeholder: { display: "inline-block", position: "relative", height: `${mdHeight}px`, width: `${mdHeight}px` },
    iconPlaceholder: { position: "absolute", height: `${mdHeight}px`, width: `${mdHeight}px` },
    icon: { position: "absolute", fontSize: `${mdHeight}px`, height: `${mdHeight}px`, width: `${mdHeight}px` },
    overlayPlaceholder: {
        position: "absolute", right: "-2px", bottom: "-2px",
        width: `${mdOverlayHeight + 2}px`, height: `${mdOverlayHeight + 2}px`,
        borderTopWidth: "2px", borderRightWidth: "2px", borderBottomWidth: "2px", borderLeftWidth: "2px",
        borderTopStyle: "solid", borderRightStyle: "solid", borderBottomStyle: "solid", borderLeftStyle: "solid",
        borderRadius: "50%",
        backgroundClip: "border-box", textAlign: "center",
    },
    overlay: { fontSize: `${mdOverlayHeight}px`, lineHeight: `${mdOverlayHeight + 2}px`, verticalAlign: "top" }
});

const smHeight = 16;
const smOverlayHeight = smHeight - smHeight / GOLDEN_RATIO + 2;
const smOverlayBorder = 1;
const useSmStyles = makeStyles({
    placeholder: { display: "inline-block", position: "relative", height: `${smHeight}px`, width: `${smHeight}px` },
    iconPlaceholder: { position: "absolute", height: `${smHeight}px`, width: `${smHeight}px` },
    icon: { position: "absolute", fontSize: `${smHeight}px`, height: `${smHeight}px`, width: `${smHeight}px` },
    overlayPlaceholder: {
        position: "absolute", right: "-2px", bottom: "-2px",
        width: `${smOverlayHeight + smOverlayBorder}px`, height: `${smOverlayHeight + smOverlayBorder}px`,
        borderTopWidth: `${smOverlayBorder}px`, borderRightWidth: `${smOverlayBorder}px`, borderBottomWidth: `${smOverlayBorder}px`, borderLeftWidth: `${smOverlayBorder}px`,
        borderTopStyle: "solid", borderRightStyle: "solid", borderBottomStyle: "solid", borderLeftStyle: "solid",
        borderRadius: "50%",
        backgroundClip: "border-box", textAlign: "center",
    },
    overlay: { fontSize: `${smOverlayHeight}px`, lineHeight: `${smOverlayHeight + smOverlayBorder}px`, verticalAlign: "top" }
});

type SizeT = "lg" | "md" | "sm";
interface StateIconProps {
    iconName?: string;
    overlayName?: string;
    overlayColor?: string;
    title?: string;
    size?: SizeT;
}

export const StateIcon: React.FunctionComponent<StateIconProps> = ({
    iconName = "StatusCircleQuestionMark",
    overlayName = "",
    overlayColor = "green",
    title = "",
    size = "md"
}) => {
    const lgStyles = useLgStyles();
    const mdStyles = useMdStyles();
    const smStyles = useSmStyles();
    const ss = { lg: lgStyles, md: mdStyles, sm: smStyles }[size];

    const { themeV9 } = useUserTheme();

    const overlayIconColor = Palette.textColor(overlayColor);

    return <FluentProvider theme={themeV9} className={ss.placeholder} title={title}>
        <span className={ss.iconPlaceholder}>
            {renderStateIcon(iconName, ss.icon)}
        </span>
        {overlayName &&
            <span className={ss.overlayPlaceholder} style={{ backgroundColor: overlayColor, borderColor: WHITE }}>
                {renderStateIcon(overlayName, ss.overlay, { color: overlayIconColor })}
            </span>
        }
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

    const { workunit } = useWorkunit(wuid);
    const [overlayName, setOverlayName] = React.useState("");
    const [overlayColor, setOverlayColor] = React.useState("");
    const { themeV9 } = useUserTheme();

    React.useEffect(() => {
        switch (workunit?.StateID) {
            case WUStateID.Compiling:
            case WUStateID.Compiled:
                setOverlayName("LightningBolt");
                setOverlayColor(WARNING_ICON);
                break;
            case WUStateID.DebugRunning:
            case WUStateID.Running:
                setOverlayName("Play");
                setOverlayColor(WARNING_ICON);
                break;
            case WUStateID.Completed:
                setOverlayName("SkypeCheck");
                setOverlayColor(SUCCESS_ICON);
                break;
            case WUStateID.Aborted:
            case WUStateID.Failed:
                setOverlayName("Warning");
                setOverlayColor(ERROR_ICON);
                break;
            case WUStateID.Aborting:
                setOverlayName("SkypeClock");
                setOverlayColor(ERROR_ICON);
                break;
            case WUStateID.Submitted:
            case WUStateID.Scheduled:
                setOverlayName("SkypeClock");
                setOverlayColor(WARNING_ICON);
                break;
            case WUStateID.UploadingFiled:
                setOverlayName("Upload");
                setOverlayColor(WARNING_ICON);
                break;
            case WUStateID.Wait:
            case WUStateID.Blocked:
                setOverlayName("Blocked");
                setOverlayColor(WARNING_ICON);
                break;
            case WUStateID.DebugPaused:
            case WUStateID.Paused:
                setOverlayName("Pause");
                setOverlayColor(WARNING_ICON);
                break;
            case WUStateID.Archived:
                setOverlayName("Archive");
                setOverlayColor(SUCCESS_ICON);
                break;
            case WUStateID.LAST:
            case WUStateID.Unknown:
            case WUStateID.NotFound:
            default:
                setOverlayName("StatusCircleQuestionMark");
                setOverlayColor(ERROR_ICON);
                break;
        }
    }, [workunit, workunit?.StateID]);

    return <FluentProvider theme={themeV9} style={{ paddingTop: 4, flexGrow: 1, height: 26 }} title={workunit?.State}>
        {showProtected &&
            <span style={{ marginLeft: 8, marginRight: 2 }}>
                <StateIcon iconName={workunit?.Protected ? "LockSolid" : "Unlock"} size={size} />
            </span>
        }
        <StateIcon iconName="Settings" overlayName={overlayName} overlayColor={overlayColor} size={size} />
        {showWuid &&
            <Text size={500} weight="bold" style={{ paddingLeft: "4px" }}>{wuid}</Text>
        }
    </FluentProvider>;
};