import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { usePods } from "../hooks/cloud";
import { useFluentGrid } from "../hooks/grid";
import { ShortVerticalDivider } from "./Common";
import { JSONSourceEditor } from "./SourceEditor";

interface PodsProps {
}

export const PodsJSON: React.FunctionComponent<PodsProps> = ({
}) => {
    const [pods, _refresh] = usePods();

    return <JSONSourceEditor json={pods} readonly={true} />;
};

export const Pods: React.FunctionComponent<PodsProps> = ({
}) => {
    const [pods, refreshData] = usePods();

    //  Grid ---
    const { Grid, copyButtons } = useFluentGrid({
        data: pods,
        primaryID: "name",
        filename: "pods",
        columns: {
            name: { label: nlsHPCC.Name, width: 300 },
            container: { label: nlsHPCC.Container, width: 120 },
            port: { label: nlsHPCC.Port, width: 64 },
            ready: { label: nlsHPCC.Ready, width: 64 },
            status: { label: nlsHPCC.Status, width: 90 },
            restarts: { label: nlsHPCC.Restarts, width: 64 },
            age: { label: nlsHPCC.Age, width: 90 }
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
