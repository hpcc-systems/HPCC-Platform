import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { usePods } from "../hooks/cloud";
import { useFluentGrid } from "../hooks/grid";
import { ShortVerticalDivider } from "./Common";
import { JSONSourceEditor } from "./SourceEditor";

function formatAge(milliseconds: number): string {

    const seconds = Math.floor((milliseconds / 1000) % 60);
    const minutes = Math.floor((milliseconds / 1000 / 60) % 60);
    const hours = Math.floor((milliseconds / 1000 / 60 / 60) % 24);
    const days = Math.floor((milliseconds / 1000 / 60 / 60 / 24));
    if (days) {
        return `${days}d`;
    } else if (hours) {
        return `${hours}h`;
    } else if (minutes) {
        return `${minutes}m`;
    } else if (seconds) {
        return `${seconds}s`;
    }
    return `${milliseconds}ms`;
}

interface PodsProps {
}

export const PodsJSON: React.FunctionComponent<PodsProps> = ({
}) => {
    const [pods, _refresh] = usePods();

    return <JSONSourceEditor json={pods} readonly={true} />;
};

export const Pods: React.FunctionComponent<PodsProps> = ({
}) => {
    const [data, setData] = React.useState<any[]>([]);
    const [pods, refreshData] = usePods();

    //  Grid ---
    const { Grid, copyButtons } = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        filename: "pods",
        columns: {
            name: { label: nlsHPCC.Name, width: 300 },
            ready: { label: nlsHPCC.Ready, width: 64 },
            status: { label: nlsHPCC.Status, width: 90 },
            restarts: { label: nlsHPCC.Restarts, width: 64 },
            age: { label: nlsHPCC.Age, width: 90 }
        }
    });

    React.useEffect(() => {
        const now = Date.now();
        setData(pods.map((pod, idx) => {
            const started = new Date(pod.metadata?.creationTimestamp);
            return {
                __hpcc_id: idx,
                name: pod.metadata?.name,
                ready: `${pod.status?.containerStatuses?.reduce((prev, curr) => prev + (curr.ready ? 1 : 0), 0)}/${pod.status?.containerStatuses?.length}`,
                status: pod.status?.phase,
                restarts: pod.status?.containerStatuses?.reduce((prev, curr) => prev + curr.restartCount, 0),
                age: formatAge(now - +started)
            };
        }));
    }, [pods]);

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
