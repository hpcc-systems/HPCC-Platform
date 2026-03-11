import * as React from "react";
import { Toolbar, ToolbarButton, ToolbarDivider, Divider } from "@fluentui/react-components";
import { ArrowClockwise20Regular, Grid20Regular, TextAlignLeft20Regular } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";
import { QueueCards } from "./cards/QueueCard";
import { HolyGrail } from "../layouts/HolyGrail";
import { SizeMe } from "../layouts/SizeMe";
import { useBuildInfo } from "../hooks/platform";
import { useUserStore } from "../hooks/store";
import { DiskUsageCards } from "./cards/DiskUsageCard";

interface ActivitiesProps {
}

export const Activities: React.FunctionComponent<ActivitiesProps> = ({
}) => {
    const [, { isContainer }] = useBuildInfo();
    const [refreshToken, setRefreshToken] = React.useState(0);
    const [listMode, setListMode] = useUserStore("ACTIVITIES_LIST_MODE", false);

    return <HolyGrail
        header={
            <Toolbar>
                <ToolbarButton appearance="subtle" icon={<ArrowClockwise20Regular />} aria-label={nlsHPCC.Refresh} onClick={() => setRefreshToken(t => t + 1)}>
                    {nlsHPCC.Refresh}
                </ToolbarButton>
                <ToolbarDivider />
                <ToolbarButton appearance="subtle" icon={listMode ? <Grid20Regular /> : <TextAlignLeft20Regular />} aria-label={listMode ? nlsHPCC.Grid : nlsHPCC.List} onClick={() => setListMode(prev => !prev)}>
                    {listMode ? nlsHPCC.Grid : nlsHPCC.List}
                </ToolbarButton>
            </Toolbar>
        }
        main={
            <SizeMe>{({ size }) => {
                return <div style={{ position: "relative", width: "100%", height: "100%" }}>
                    <div style={{ position: "absolute", width: "100%", height: `${size.height}px`, overflowY: "auto" }}>
                        {
                            !isContainer ?
                                <>
                                    <Divider>{nlsHPCC.DiskUsage}</Divider>
                                    <DiskUsageCards refreshToken={refreshToken} />
                                </> :
                                <>
                                </>
                        }
                        <>
                            <Divider>{nlsHPCC.Activities}</Divider>
                            <QueueCards refreshToken={refreshToken} listMode={listMode} />
                        </>
                    </div>
                </div>;
            }}</SizeMe>
        }
    />;
};
