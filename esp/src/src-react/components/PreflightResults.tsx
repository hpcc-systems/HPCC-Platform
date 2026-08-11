import * as React from "react";
import { Spinner, Toolbar, ToolbarButton, makeStyles } from "@fluentui/react-components";
import { ArrowClockwise20Regular } from "@fluentui/react-icons";
import { MachineService, WsMachine } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { getCondition, getState } from "src/ESPPreflight";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { SizeMe } from "../layouts/SizeMe";
import { FluentColumns, FluentGrid, SelectionMode, useFluentStoreState } from "./controls/Grid";

const logger = scopedLogger("src-react/components/PreflightResults.tsx");

const machineService = new MachineService({ baseUrl: "" });

const useStyles = makeStyles({
    loadingContainer: {
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        height: "100%"
    }
});

export interface PreflightQueryParams {
    TargetClusters?: string | string[];
    Addresses?: string | string[];
    GetProcessorInfo?: boolean;
    GetStorageInfo?: boolean;
    LocalFileSystemsOnly?: boolean;
    GetSoftwareInfo?: boolean;
    ApplyProcessFilter?: boolean;
    AddProcessesToFilter?: string;
    AutoRefresh?: number;
    CpuThreshold?: number;
    MemThreshold?: number;
    MemThresholdType?: number;
    DiskThreshold?: number;
    DiskThresholdType?: number;
}

interface PreflightRow {
    __hpcc_id: string;
    Location: string;
    Component: string;
    Condition: string;
    State: string;
    ProcessesDown: string;
    ComputerUpTime: string;
    CPULoad?: number;
    RoxieState?: string;
    [storageKey: string]: string | number | undefined;
}

function buildRows(response: WsMachine.GetTargetClusterInfoResponse): PreflightRow[] {
    const rows: PreflightRow[] = [];
    const clusters = response?.TargetClusterInfoList?.TargetClusterInfo ?? [];
    clusters.forEach(cluster => {
        (cluster.Processes?.MachineInfoEx ?? []).forEach((machine, idx) => {
            const row: PreflightRow = {
                __hpcc_id: `${cluster.Name}_${machine.Address}_${idx}`,
                Location: `${machine.Address ?? ""} ${machine.ComponentPath ?? ""}`.trim(),
                Component: `${machine.DisplayType ?? ""}[${machine.ComponentName ?? ""}]`,
                Condition: getCondition(machine.ComponentInfo?.Condition),
                State: getState(machine.ComponentInfo?.State),
                ProcessesDown: machine.ComponentInfo?.Condition === 2 ? (machine.ComponentName ?? "") : "",
                ComputerUpTime: machine.UpTime ?? "",
            };
            if (machine.RoxieState) {
                row.RoxieState = machine.RoxieState;
            }
            if (machine.Processors?.ProcessorInfo?.length) {
                row.CPULoad = machine.Processors.ProcessorInfo[0].Load;
            }
            if (machine.Storage?.StorageInfo) {
                machine.Storage.StorageInfo.forEach(storage => {
                    const key = (storage.Description ?? "").replace(/([~!@#$%^&*()_+=`{}[\]|\\:;'<>,./? ])+/g, "").replace(/^(-)+|(-)+$/g, "");
                    if (key) {
                        row[key] = storage.PercentAvail;
                    }
                });
            }
            rows.push(row);
        });
    });
    return rows;
}

function buildRowsFromMachineInfo(response: WsMachine.GetMachineInfoResponse): PreflightRow[] {
    const rows: PreflightRow[] = [];
    (response?.Machines?.MachineInfoEx ?? []).forEach((machine, idx) => {
        const row: PreflightRow = {
            __hpcc_id: `${machine.Address}_${idx}`,
            Location: `${machine.Address ?? ""} ${machine.ComponentPath ?? ""}`.trim(),
            Component: `${machine.DisplayType ?? ""}[${machine.ComponentName ?? ""}]`,
            Condition: getCondition(machine.ComponentInfo?.Condition),
            State: getState(machine.ComponentInfo?.State),
            ProcessesDown: machine.ComponentInfo?.Condition === 2 ? (machine.ComponentName ?? "") : "",
            ComputerUpTime: machine.UpTime ?? "",
        };
        if (machine.RoxieState) {
            row.RoxieState = machine.RoxieState;
        }
        if (machine.Processors?.ProcessorInfo?.length) {
            row.CPULoad = machine.Processors.ProcessorInfo[0].Load;
        }
        if (machine.Storage?.StorageInfo) {
            machine.Storage.StorageInfo.forEach(storage => {
                const key = (storage.Description ?? "").replace(/([~!@#$%^&*()_+=`{}[\]|\\:;'<>,./? ])+/g, "").replace(/^(-)+|(-)+$/g, "");
                if (key) {
                    row[key] = storage.PercentAvail;
                }
            });
        }
        rows.push(row);
    });
    return rows;
}

function buildStorageColumns(rows: PreflightRow[], diskThreshold?: number): FluentColumns {
    const staticKeys = new Set(["__hpcc_id", "Location", "Component", "Condition", "State", "ProcessesDown", "ComputerUpTime", "CPULoad", "RoxieState"]);
    const storageKeys = new Set<string>();
    rows.forEach(row => {
        Object.keys(row).forEach(key => {
            if (!staticKeys.has(key)) storageKeys.add(key);
        });
    });
    const cols: FluentColumns = {};
    storageKeys.forEach(key => {
        cols[key] = {
            label: key,
            width: 90,
            justify: "right",
            formatter: (val: number) => {
                if (val === undefined || val === null) return "";
                const isWarning = diskThreshold !== undefined && val < diskThreshold;
                return <span style={isWarning ? { color: "var(--colorPaletteRedForeground1)" } : undefined}>{val}%</span>;
            }
        };
    });
    return cols;
}

interface PreflightResultsProps {
    queryParams?: PreflightQueryParams;
}

export const PreflightResults: React.FunctionComponent<PreflightResultsProps> = ({
    queryParams
}) => {
    const styles = useStyles();
    const [data, setData] = React.useState<PreflightRow[]>([]);
    const [loading, setLoading] = React.useState(true);
    const { refreshTable } = useFluentStoreState({});

    const targetClusters = React.useMemo((): string[] => {
        if (!queryParams?.TargetClusters) return [];
        return Array.isArray(queryParams.TargetClusters)
            ? queryParams.TargetClusters
            : [queryParams.TargetClusters];
    }, [queryParams?.TargetClusters]);

    const addresses = React.useMemo((): string[] => {
        if (!queryParams?.Addresses) return [];
        return Array.isArray(queryParams.Addresses)
            ? queryParams.Addresses
            : [queryParams.Addresses];
    }, [queryParams?.Addresses]);

    const commonRequestFields = React.useMemo(() => ({
        AutoRefresh: queryParams?.AutoRefresh,
        MemThreshold: queryParams?.MemThreshold,
        CpuThreshold: queryParams?.CpuThreshold,
        MemThresholdType: queryParams?.MemThresholdType,
        DiskThreshold: queryParams?.DiskThreshold,
        DiskThresholdType: queryParams?.DiskThresholdType,
        GetProcessorInfo: queryParams?.GetProcessorInfo,
        GetStorageInfo: queryParams?.GetStorageInfo,
        LocalFileSystemsOnly: queryParams?.LocalFileSystemsOnly,
        GetSoftwareInfo: queryParams?.GetSoftwareInfo,
        ApplyProcessFilter: queryParams?.ApplyProcessFilter,
        AddProcessesToFilter: queryParams?.AddProcessesToFilter,
    }), [queryParams]);

    const refresh = React.useCallback(() => {
        if (addresses.length) {
            setLoading(true);
            const request: Partial<WsMachine.GetMachineInfoRequest> = {
                ...commonRequestFields,
                Addresses: { Item: addresses },
            };
            machineService.GetMachineInfo(request)
                .then(response => {
                    setData(buildRowsFromMachineInfo(response));
                })
                .catch(err => {
                    logger.error(err);
                })
                .finally(() => {
                    setLoading(false);
                });
        } else if (targetClusters.length) {
            setLoading(true);
            const request: Partial<WsMachine.GetTargetClusterInfoRequest> = {
                ...commonRequestFields,
                TargetClusters: { Item: targetClusters },
            };
            machineService.GetTargetClusterInfo(request)
                .then(response => {
                    setData(buildRows(response));
                })
                .catch(err => {
                    logger.error(err);
                })
                .finally(() => {
                    setLoading(false);
                });
        } else {
            setData([]);
            setLoading(false);
        }
    }, [addresses, targetClusters, commonRequestFields]);

    React.useEffect(() => {
        refresh();
    }, [refresh]);

    const hasRoxieState = React.useMemo(() => data.some(r => r.RoxieState !== undefined), [data]);
    const hasCPULoad = React.useMemo(() => data.some(r => r.CPULoad !== undefined), [data]);
    const cpuThreshold = queryParams?.CpuThreshold;
    const diskThreshold = queryParams?.DiskThreshold;

    const columns = React.useMemo((): FluentColumns => {
        const cols: FluentColumns = {};
        if (hasRoxieState) {
            cols.RoxieState = {
                label: nlsHPCC.RoxieState,
                width: 140,
                formatter: (val: string) => {
                    const isError = val === "State hash mismatch ..." || val === "Not attached to DALI..." || val === "empty state hash ..." || val === "Node State: not ok ...";
                    return <span style={isError ? { color: "var(--colorPaletteRedForeground1)" } : undefined}>{val ?? "N/A"}</span>;
                }
            };
        }
        cols.Location = { label: nlsHPCC.Location, width: 350 };
        cols.Component = { label: nlsHPCC.Component, width: 275 };
        cols.Condition = {
            label: nlsHPCC.Condition,
            width: 90,
            formatter: (val: string) => {
                const isError = val === "Warning" || val === "Minor" || val === "Major" || val === "Critical" || val === "Fatal" || val === "Unknown";
                return <span style={isError ? { color: "var(--colorPaletteRedForeground1)" } : undefined}>{val}</span>;
            }
        };
        cols.State = {
            label: nlsHPCC.State,
            width: 90,
            formatter: (val: string) => {
                const isError = val === "Unknown" || val === "Starting" || val === "Stopping" || val === "Suspended" || val === "Recycling" || val === "Busy" || val === "NA";
                return <span style={isError ? { color: "var(--colorPaletteRedForeground1)" } : undefined}>{val}</span>;
            }
        };
        cols.ProcessesDown = {
            label: nlsHPCC.ProcessesDown,
            width: 120,
            formatter: (val: string) => {
                return <span style={val ? { color: "var(--colorPaletteRedForeground1)" } : undefined}>{val}</span>;
            }
        };
        if (hasCPULoad) {
            cols.CPULoad = {
                label: nlsHPCC.CPULoad,
                width: 90,
                justify: "right",
                formatter: (val: number) => {
                    const isWarning = cpuThreshold !== undefined && val > cpuThreshold;
                    return <span style={isWarning ? { color: "var(--colorPaletteRedForeground1)" } : undefined}>{val}%</span>;
                }
            };
        }
        cols.ComputerUpTime = { label: nlsHPCC.ComputerUpTime, width: 140 };
        return { ...cols, ...buildStorageColumns(data, diskThreshold) };
    }, [hasRoxieState, hasCPULoad, cpuThreshold, diskThreshold, data]);

    return <HolyGrail
        header={
            <Toolbar>
                <ToolbarButton appearance="subtle" icon={<ArrowClockwise20Regular />} aria-label={nlsHPCC.Refresh} onClick={refresh}>
                    {nlsHPCC.Refresh}
                </ToolbarButton>
            </Toolbar>
        }
        main={
            loading
                ? <div className={styles.loadingContainer}><Spinner label={nlsHPCC.Loading} /></div>
                : <SizeMe>{({ size }) =>
                    <div style={{ position: "relative", width: "100%", height: "100%" }}>
                        <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                            <FluentGrid
                                data={data}
                                primaryID="__hpcc_id"
                                columns={columns}
                                selectionMode={SelectionMode.none}
                                setSelection={() => null}
                                setTotal={() => null}
                                refresh={refreshTable}
                            />
                        </div>
                    </div>
                }</SizeMe>
        }
    />;
};
