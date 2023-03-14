import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Icon } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { useServices } from "../hooks/resources";
import { useFluentGrid } from "../hooks/grid";
import { ShortVerticalDivider } from "./Common";

interface ServicesProps {
}

export const Services: React.FunctionComponent<ServicesProps> = ({
}) => {
    const [services, refreshData] = useServices();

    //  Grid ---
    const { Grid, copyButtons } = useFluentGrid({
        data: services,
        primaryID: "Name",
        filename: "services",
        columns: {
            Name: { label: nlsHPCC.Name, width: 200 },
            Type: { label: nlsHPCC.Container, width: 200 },
            Port: { label: nlsHPCC.Port, width: 120 },
            TLSSecure: {
                headerIcon: "LockSolid", label: nlsHPCC.TLS, width: 16,
                formatter: React.useCallback(secure => secure === true ? <Icon iconName="LockSolid" /> : "", [])
            }
        }
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [refreshData]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <SizeMe monitorHeight>{({ size }) =>
                <div style={{ width: "100%", height: "100%" }}>
                    <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                        <Grid height={`${size.height}px`} />
                    </div>
                </div>
            }</SizeMe>
        }
    />;
};
