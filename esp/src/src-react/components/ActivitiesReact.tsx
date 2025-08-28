import * as React from "react";
import { Toolbar, ToolbarButton, Divider } from "@fluentui/react-components";
import { ArrowClockwise20Regular } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";
import { QueueCards } from "./cards/QueueCard";
import { HolyGrail } from "../layouts/HolyGrail";
import { SizeMe } from "../layouts/SizeMe";
import { useBuildInfo } from "../hooks/platform";
import { DiskUsageCards } from "./cards/DiskUsageCard";

export const Activities: React.FC = () => {
    const [, { isContainer }] = useBuildInfo();
    const [refreshToken, setRefreshToken] = React.useState(0);

    return <HolyGrail
        header={
            <Toolbar>
                <ToolbarButton appearance="subtle" icon={<ArrowClockwise20Regular />} aria-label={nlsHPCC.Refresh} onClick={() => setRefreshToken(t => t + 1)}>
                    {nlsHPCC.Refresh}
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
                            <QueueCards refreshToken={refreshToken} />
                        </>
                    </div>
                </div>;
            }}</SizeMe>
        }
    />;
};
