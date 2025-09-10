import * as React from "react";
import { Link } from "@fluentui/react";
import { Divider, Toolbar, ToolbarButton } from "@fluentui/react-components";
import { ArrowClockwise20Regular } from "@fluentui/react-icons";
import { ComponentDetails as ComponentDetailsWidget, Summary as SummaryWidget } from "src/DiskUsage";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { HolyGrail } from "../layouts/HolyGrail";
import { SizeMe } from "../layouts/SizeMe";
import { pushUrl } from "../util/history";
import { FluentGrid, useFluentStoreState } from "./controls/Grid";
import { FolderUsageCards } from "./cards/DiskUsageCard";
import { useTargetClusterUsageEx } from "../hooks/diskUsage";

interface SummaryProps {
    cluster?: string;
}

export const Summary: React.FunctionComponent<SummaryProps> = ({
    cluster
}) => {
    const summary = React.useMemo(() => {
        const retVal = new SummaryWidget(cluster)
            .refresh(false)
            .on("click", (widget, details) => {
                pushUrl(`/operations/clusters/${details.Name}/usage`);
            })
            ;
        return retVal;
    }, [cluster]);

    return <AutosizeHpccJSComponent widget={summary}></AutosizeHpccJSComponent >;
};

interface ClusterUsageProps {
    cluster: string;
}

export const ClusterUsage: React.FunctionComponent<ClusterUsageProps> = ({
    cluster
}) => {

    const { refreshTable } = useFluentStoreState({});
    const [refreshToken, setRefreshToken] = React.useState(0);
    const { data: usage, refresh } = useTargetClusterUsageEx(cluster);

    //  Grid ---
    const columns = React.useMemo(() => {
        return {
            PercentUsed: {
                label: nlsHPCC.PercentUsed, width: 50, formatter: (percent) => {
                    let className = "";

                    if (percent <= 70) { className = "bgFilled bgGreen"; }
                    else if (percent > 70 && percent < 80) { className = "bgFilled bgOrange"; }
                    else { className = "bgFilled bgRed"; }

                    return <span className={className}>{percent}</span>;
                }
            },
            Component: { label: nlsHPCC.Component, width: 90 },
            Type: { label: nlsHPCC.Type, width: 40 },
            IPAddress: {
                label: nlsHPCC.IPAddress, width: 140,
                formatter: (ip) => <Link href={`#/operations/machines/${ip}/usage`}>{ip}</Link>
            },
            Path: { label: nlsHPCC.Path, width: 220 },
            InUse: { label: nlsHPCC.InUse, width: 50 },
            Total: { label: nlsHPCC.Total, width: 50 },
        };
    }, []);

    type Columns = typeof columns;
    type Row = { __hpcc_id: string } & { [K in keyof Columns]: string | number };
    const data = React.useMemo<Row[]>(() => {
        const rows: Row[] = [];
        (usage ?? []).forEach(component => {
            component.ComponentUsages.forEach(cu => {
                cu.MachineUsages.forEach(mu => {
                    mu.DiskUsages.forEach((du, i) => {
                        rows.push({
                            __hpcc_id: `__usage_${i}`,
                            PercentUsed: Math.round((du.InUse / du.Total) * 100),
                            Component: cu.Name,
                            IPAddress: mu.Name,
                            Type: du.Name,
                            Path: du.Path,
                            InUse: Utility.convertedSize(du.InUse),
                            Total: Utility.convertedSize(du.Total)
                        });
                    });
                });
            });
        });
        return rows;
    }, [usage]);

    return <HolyGrail
        header={
            <Toolbar>
                <ToolbarButton appearance="subtle" icon={<ArrowClockwise20Regular />} aria-label={nlsHPCC.Refresh} onClick={() => { refresh(); setRefreshToken(t => t + 1); }}>
                    {nlsHPCC.Refresh}
                </ToolbarButton>
            </Toolbar>
        }
        main={<SizeMe>{({ size }) => {
            return <div style={{ position: "relative", width: "100%", height: "100%" }}>
                <div style={{ position: "absolute", width: "100%", height: `${size.height}px`, overflowY: "auto" }}>
                    <Divider>{nlsHPCC.Category}</Divider>
                    <FolderUsageCards cluster={cluster} refreshToken={refreshToken} />
                    <Divider>{nlsHPCC.Folders}</Divider>
                    <FluentGrid data={data} primaryID="__hpcc_id" sort={{ attribute: "__hpcc_id", descending: false }} columns={columns} setSelection={() => null} setTotal={() => null} refresh={refreshTable} />
                </div>
            </div>;
        }}</SizeMe>}
    />;
};

interface MachineUsageProps {
    machine: string;
}

export const MachineUsage: React.FunctionComponent<MachineUsageProps> = ({
    machine
}) => {
    const summary = React.useMemo(() => {
        const retVal = new ComponentDetailsWidget(machine)
            .refresh()
            ;
        return retVal;
    }, [machine]);

    return <AutosizeHpccJSComponent widget={summary}></AutosizeHpccJSComponent >;
};
