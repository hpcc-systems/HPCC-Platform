import * as React from "react";
import { Divider, Link, Toolbar, ToolbarButton } from "@fluentui/react-components";
import { ArrowClockwise20Regular, Filter20Regular, FilterFilled } from "@fluentui/react-icons";
import { DFUService, WsDfu } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { ComponentDetails as ComponentDetailsWidget, Summary as SummaryWidget } from "src/DiskUsage";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { HolyGrail } from "../layouts/HolyGrail";
import { SizeMe } from "../layouts/SizeMe";
import { pushParams, pushUrl } from "../util/history";
import { FluentColumns, FluentGrid, useFluentStoreState } from "./controls/Grid";
import { FolderUsageCards } from "./cards/DiskUsageCard";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { useTargetClusterUsageEx } from "../hooks/diskUsage";

const logger = scopedLogger("src-react/components/DiskUsage.tsx");

type DiskUsageRow = WsDfu.DFUSpaceItem & { __hpcc_id: string };

const DiskUsageFilterFields: Fields = {
    "CountBy": {
        type: "dropdown",
        label: nlsHPCC.GroupBy,
        value: "Owner",
        options: [
            { key: "Owner", text: nlsHPCC.Owner },
            { key: "Scope", text: nlsHPCC.Scope },
            { key: "Year", text: nlsHPCC.Year },
            { key: "Quarter", text: nlsHPCC.Quarter },
            { key: "Month", text: nlsHPCC.Month },
            { key: "Day", text: nlsHPCC.Day },
        ]
    },
    "ScopeUnder": { type: "string", label: nlsHPCC.Scope },
    "OwnerUnder": { type: "string", label: nlsHPCC.Owner },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate },
};

function formatDiskUsageQuery(_filter: { [id: string]: any }): { [id: string]: any } {
    const filter = { ..._filter };
    switch (filter.CountBy) {
        case "Year":
        case "Quarter":
        case "Month":
        case "Day":
            filter.Interval = filter.CountBy;
            filter.CountBy = "Date";
            break;
    }
    if (filter.StartDate) {
        filter.StartDate = new Date(filter.StartDate).toISOString();
    }
    if (filter.EndDate) {
        filter.EndDate = new Date(filter.EndDate).toISOString();
    }
    return filter;
}

interface DiskUsageProps {
    filter?: { [id: string]: any };
}

const emptyDiskUsageFilter = {};

export const DiskUsage: React.FunctionComponent<DiskUsageProps> = ({
    filter = emptyDiskUsageFilter
}) => {

    const dfuService = React.useMemo(() => new DFUService({ baseUrl: "" }), []);
    const hasFilter = React.useMemo(() => {
        return Object.entries(filter).some(([key, value]) => {
            if (value === undefined || value === "") return false;
            if (key === "CountBy" && value === "Owner") return false;
            return true;
        });
    }, [filter]);
    const [showFilter, setShowFilter] = React.useState(false);
    const [data, setData] = React.useState<DiskUsageRow[]>([]);
    const { refreshTable } = useFluentStoreState({});
    const abortController = React.useRef<AbortController | null>(null);

    const refresh = React.useCallback(() => {
        if (abortController.current) abortController.current.abort();
        abortController.current = new AbortController();
        const query = { CountBy: "Owner", ...filter };
        const request = formatDiskUsageQuery(query);
        request.abortSignal_ = abortController.current.signal;
        dfuService.DFUSpace(request)
            .then((response: WsDfu.DFUSpaceResponse) => {
                const items: WsDfu.DFUSpaceItem[] = response?.DFUSpaceItems?.DFUSpaceItem ?? [];
                setData(items.map(item => ({
                    ...item,
                    __hpcc_id: item.Name
                })));
            })
            .catch(err => {
                if (err.name === "AbortError") return;
                setData([]);
                logger.error(err);
            });
    }, [dfuService, filter]);

    React.useEffect(() => {
        refresh();
        return () => {
            if (abortController.current) {
                abortController.current.abort();
                abortController.current = null;
            }
        };
    }, [refresh]);

    const columns = React.useMemo((): FluentColumns => ({
        Name: { label: nlsHPCC.Grouping, width: 90 },
        NumOfFiles: { label: nlsHPCC.FileCounts, width: 90, justify: "right" },
        TotalSize: { label: nlsHPCC.TotalSize, width: 125, justify: "right" },
        LargestFile: { label: nlsHPCC.LargestFile },
        LargestSize: { label: nlsHPCC.LargestSize, width: 125, justify: "right" },
        SmallestFile: { label: nlsHPCC.SmallestFile },
        SmallestSize: { label: nlsHPCC.SmallestSize, width: 125, justify: "right" },
        NumOfFilesUnknown: { label: nlsHPCC.FilesWithUnknownSize, width: 160, justify: "right" },
    }), []);

    const filterFields: Fields = React.useMemo(() => {
        const fields: Fields = {};
        for (const field in DiskUsageFilterFields) {
            fields[field] = { ...DiskUsageFilterFields[field], value: filter[field] ?? (DiskUsageFilterFields[field] as { value?: string }).value };
        }
        return fields;
    }, [filter]);

    return <HolyGrail
        header={
            <Toolbar>
                <ToolbarButton appearance="subtle" icon={<ArrowClockwise20Regular />} aria-label={nlsHPCC.Refresh} onClick={() => refresh()}>
                    {nlsHPCC.Refresh}
                </ToolbarButton>
                <ToolbarButton appearance="subtle" icon={hasFilter ? <FilterFilled /> : <Filter20Regular />} aria-label={nlsHPCC.Filter} onClick={() => setShowFilter(true)}>
                    {nlsHPCC.Filter}
                </ToolbarButton>
            </Toolbar>
        }
        main={
            <>
                <SizeMe>{({ size }) =>
                    <div style={{ width: "100%", height: "100%" }}>
                        <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                            <FluentGrid
                                data={data}
                                primaryID="__hpcc_id"
                                sort={{ attribute: "Name", descending: false }}
                                columns={columns}
                                setSelection={() => null}
                                setTotal={() => null}
                                refresh={refreshTable}
                            />
                        </div>
                    </div>
                }</SizeMe>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
            </>
        }
    />;
};

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
