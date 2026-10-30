import * as React from "react";
import { Button, FluentProvider, makeStyles, Text } from "@fluentui/react-components";
import {
    ArchiveRegular, CheckmarkCircleRegular, ClockRegular, CopyRegular, FlashRegular,
    LockClosedFilled, LockOpenRegular, PauseCircleRegular, PlayRegular,
    ProhibitedRegular, QuestionCircleRegular, SettingsRegular, WarningRegular
} from "@fluentui/react-icons";
import { Palette } from "@hpcc-js/common";
import { FileSprayStates, WUStateID } from "@hpcc-js/comms";
import { useWorkunit, useDfuWorkunit } from "../../hooks/workunit";
import { useUserTheme } from "../../hooks/theme";
import { copyToClipboard } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";

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

const usePersonaStyles = makeStyles({
    wuid: {
        paddingLeft: "4px",
        fontWeight: "bold"
    },
    copyButton: {
        minWidth: "24px",
        maxWidth: "24px",
        height: "23px",
        margin: "2px 0 0 6px",
        "& .fui-Button__icon": {
            height: "16px",
            width: "16px"
        }
    }
});

interface OverlayState {
    name: string;
    color: string;
}

const DEFAULT_OVERLAY: OverlayState = { name: "StatusCircleQuestionMark", color: ERROR_ICON };

const WU_STATE_OVERLAYS = new Map<number, OverlayState>([
    [WUStateID.Compiling, { name: "LightningBolt", color: WARNING_ICON }],
    [WUStateID.Compiled, { name: "LightningBolt", color: WARNING_ICON }],
    [WUStateID.DebugRunning, { name: "Play", color: WARNING_ICON }],
    [WUStateID.Running, { name: "Play", color: WARNING_ICON }],
    [WUStateID.Completed, { name: "SkypeCheck", color: SUCCESS_ICON }],
    [WUStateID.Aborted, { name: "Warning", color: ERROR_ICON }],
    [WUStateID.Failed, { name: "Warning", color: ERROR_ICON }],
    [WUStateID.Aborting, { name: "SkypeClock", color: ERROR_ICON }],
    [WUStateID.Submitted, { name: "SkypeClock", color: WARNING_ICON }],
    [WUStateID.Scheduled, { name: "SkypeClock", color: WARNING_ICON }],
    [WUStateID.UploadingFiled, { name: "Upload", color: WARNING_ICON }],
    [WUStateID.Wait, { name: "Blocked", color: WARNING_ICON }],
    [WUStateID.Blocked, { name: "Blocked", color: WARNING_ICON }],
    [WUStateID.DebugPaused, { name: "Pause", color: WARNING_ICON }],
    [WUStateID.Paused, { name: "Pause", color: WARNING_ICON }],
    [WUStateID.Archived, { name: "Archive", color: SUCCESS_ICON }],
]);

const DFU_STATE_OVERLAYS = new Map<number, OverlayState>([
    [FileSprayStates.scheduled, { name: "SkypeClock", color: WARNING_ICON }],
    [FileSprayStates.queued, { name: "SkypeClock", color: WARNING_ICON }],
    [FileSprayStates.started, { name: "Play", color: WARNING_ICON }],
    [FileSprayStates.monitoring, { name: "Play", color: WARNING_ICON }],
    [FileSprayStates.finished, { name: "SkypeCheck", color: SUCCESS_ICON }],
    [FileSprayStates.aborted, { name: "Warning", color: ERROR_ICON }],
    [FileSprayStates.failed, { name: "Warning", color: ERROR_ICON }],
    [FileSprayStates.aborting, { name: "SkypeClock", color: ERROR_ICON }],
]);

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

    const firstChar = wuid?.trim().charAt(0).toUpperCase();
    const isDFUWorkunit = firstChar === "D" || firstChar === "P";

    const { workunit: eclWorkunit } = useWorkunit(isDFUWorkunit ? "" : wuid);
    const [dfuWorkunit, dfuStateID] = useDfuWorkunit(isDFUWorkunit ? wuid : "");

    const stateID = isDFUWorkunit ? dfuStateID : eclWorkunit?.StateID;
    const isProtected = isDFUWorkunit ? dfuWorkunit?.isProtected : eclWorkunit?.Protected;
    const stateTitle = isDFUWorkunit ? dfuWorkunit?.StateMessage : eclWorkunit?.State;

    const [overlayName, setOverlayName] = React.useState("");
    const [overlayColor, setOverlayColor] = React.useState("");
    const { themeV9 } = useUserTheme();
    const styles = usePersonaStyles();

    React.useEffect(() => {
        const overlay = (isDFUWorkunit ? DFU_STATE_OVERLAYS : WU_STATE_OVERLAYS).get(stateID) ?? DEFAULT_OVERLAY;
        setOverlayName(overlay.name);
        setOverlayColor(overlay.color);
    }, [stateID, isDFUWorkunit]);

    return <FluentProvider theme={themeV9} style={{ display: "flex", alignItems: "center", height: 26 }} title={stateTitle}>
        {showProtected &&
            <span style={{ marginLeft: 8, marginRight: 2 }}>
                <StateIcon iconName={isProtected ? "LockSolid" : "Unlock"} size={size} />
            </span>
        }
        <StateIcon iconName="Settings" overlayName={overlayName} overlayColor={overlayColor} size={size} />
        {showWuid &&
            <>
                <Text size={500} className={styles.wuid}>{wuid}</Text>
                <Button title={nlsHPCC.CopyWUID} aria-label={nlsHPCC.CopyWUID} className={styles.copyButton} icon={<CopyRegular />}
                    onClick={() => copyToClipboard(wuid)}
                />
            </>
        }
    </FluentProvider>;
};
