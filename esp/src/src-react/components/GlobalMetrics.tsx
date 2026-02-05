import * as React from "react";
import { Badge, Field, Label, makeStyles, ProgressBar, Toolbar, ToolbarButton, ToolbarGroup, tokens } from "@fluentui/react-components";
import { ArrowClockwiseRegular, ArrowRightFilled, ArrowResetFilled } from "@fluentui/react-icons";
import { DatePicker } from "@fluentui/react-datepicker-compat";
import { SelectionMode } from "@fluentui/react";
import { timeFormat, timeParse } from "@hpcc-js/common";
import { SMCService, WsSMC } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { formatCost } from "src/Session";
import { HolyGrail } from "../layouts/HolyGrail";
import { SizeMe } from "../layouts/SizeMe";
import { pushParams } from "../util/history";
import { FluentColumns, FluentGrid, useFluentStoreState } from "./controls/Grid";

const logger = scopedLogger("src-react/components/GlobalMetrics.tsx");

const useStyles = makeStyles({
    root: {
        maxWidth: "200px"
    },
    toolbar: {
        justifyContent: "space-between",
    },
    label: { marginLeft: tokens.spacingHorizontalM, marginRight: tokens.spacingHorizontalXS }
});

const dateFormatter = timeFormat("%Y-%m-%d");
const dateParser = timeParse("%Y-%m-%d");
const bucketDateFormatter = timeFormat("%Y-%m-%d %H:00");

function formatDuration(value: unknown): string {
    if (value === null || value === undefined || value === "") return "";

    const pad2 = (n: number) => n.toString().padStart(2, "0");
    const pad3 = (n: number) => n.toString().padStart(3, "0");

    const asTrimmedString = typeof value === "string" ? value.trim() : undefined;
    const isIntegerString = asTrimmedString !== undefined && /^-?\d+$/.test(asTrimmedString);

    if (typeof value === "bigint" || isIntegerString) {
        const seconds = typeof value === "bigint" ? value : BigInt(asTrimmedString!);
        const sign = seconds < 0n ? "-" : "";
        const absSeconds = seconds < 0n ? -seconds : seconds;
        const totalMs = (absSeconds * 1000n) % 1000n;
        const ms = Number(totalMs);
        const sec = Number(absSeconds % 60n);
        const totalMin = absSeconds / 60n;
        const min = Number(totalMin % 60n);
        const hours = totalMin / 60n;

        return `${sign}${hours.toString()}:${pad2(min)}:${pad2(sec)}.${pad3(ms)}`;
    }

    const secondsNumber = typeof value === "number" ? value : Number(asTrimmedString ?? value);
    if (!Number.isFinite(secondsNumber)) return String(value);

    const sign = secondsNumber < 0 ? "-" : "";
    const absSeconds = Math.abs(secondsNumber);
    const ms = Math.trunc((absSeconds * 1000) % 1000);
    const totalSec = Math.trunc(absSeconds);
    const sec = totalSec % 60;
    const totalMin = Math.trunc(totalSec / 60);
    const min = totalMin % 60;
    const hours = Math.trunc(totalMin / 60);

    return `${sign}${hours}:${pad2(min)}:${pad2(sec)}.${pad3(ms)}`;
}

function compareColumns(a: string, b: string): number {
    if (a.indexOf("🏷️") === 0) {
        if (b.indexOf("🏷️") === 0) {
            return a.localeCompare(b);
        } else if (b.indexOf("📊") === 0) {
            return -1;
        }
    } else if (a.indexOf("📊") === 0) {
        if (b.indexOf("📊") === 0) {
            return a.localeCompare(b);
        } else if (b.indexOf("🏷️") === 0) {
            return 1;
        }
    }
    return 0;
}

const smc = new SMCService({ baseUrl: "" });

async function getGlobalMetrics(request: Partial<WsSMC.GetGlobalMetrics>) {
    return smc.GetNormalisedGlobalMetrics(request).then(response => {
        const columnsSet = new Set<string>();
        columnsSet.add("Category");
        columnsSet.add("Start");
        columnsSet.add("End");
        const data = response.map(metric => {
            const retVal = {
                Category: metric.Category,
                Start: bucketDateFormatter(metric.Start),
                End: bucketDateFormatter(metric.End),
            };
            for (const dimName in metric.dimensions) {
                const name = `🏷️${dimName}`;
                columnsSet.add(name);
                retVal[name] = metric.dimensions[dimName];
            }
            for (const statName in metric.stats) {
                const name = `📊${statName}`;
                columnsSet.add(name);
                if ((statName ?? "").indexOf("Time") === 0) {
                    retVal[name] = formatDuration(metric.stats[statName]);
                } else if ((statName ?? "").indexOf("Cost") === 0) {
                    retVal[name] = formatCost(metric.stats[statName]);
                } else {
                    retVal[name] = metric.stats[statName];
                }
            }
            return retVal;
        }) ?? [];

        return {
            columns: Array.from(columnsSet).sort(compareColumns),
            data
        };
    });
}

export interface GlobalMetricsProps {
    from?: string;
    to?: string;
}

export const GlobalMetrics: React.FunctionComponent<GlobalMetricsProps> = ({
    to = dateFormatter(new Date()),
    from = dateFormatter(new Date(new Date().setDate(new Date().getDate() - 7)))
}) => {
    const styles = useStyles();

    const end = dateParser(to);
    const start = dateParser(from);

    const [columns, setColumns] = React.useState<string[]>([]);
    const [data, setData] = React.useState<{ [key: string]: any }[]>([]);
    const [loading, setLoading] = React.useState(false);

    const {
        setSelection,
        setTotal,
        refreshTable
    } = useFluentStoreState({});

    const rows = React.useMemo(() => {
        return (data ?? []).map((d) => {
            const retVal = {};
            columns.forEach(col => {
                retVal[col] = d[col] ?? "";
            });
            return retVal;
        });
    }, [columns, data]);

    const fluentColumns = React.useMemo((): FluentColumns => {
        const retVal: FluentColumns = {};
        columns.forEach((column) => {
            retVal[column] = {
                label: column,
                sortable: true,
                width: 180
            };
        });
        return retVal;
    }, [columns]);

    const fetchData = React.useCallback(async (start: Date, end: Date) => {
        setLoading(true);
        setColumns([]);
        setData([]);
        try {
            const adjustedEnd = new Date(end);
            adjustedEnd.setDate(adjustedEnd.getDate() + 1);

            const { columns, data } = await getGlobalMetrics({
                DateTimeRange: {
                    Start: start.toISOString(),
                    End: adjustedEnd.toISOString()
                }
            });
            setColumns(columns);
            setData(data);
        } catch (e) {
            logger.error(e);
        } finally {
            setLoading(false);
        }
    }, []);

    React.useEffect(() => {
        fetchData(dateParser(from), dateParser(to));
    }, [fetchData, from, to]);

    const onFromChange = React.useCallback((date: Date | null | undefined) => {
        if (date) {
            pushParams({ from: dateFormatter(date) });
        }
    }, []);

    const onToChange = React.useCallback((date: Date | null | undefined) => {
        if (date) {
            pushParams({ to: dateFormatter(date) });
        }
    }, []);

    return <>
        <HolyGrail
            header={<Toolbar className={styles.toolbar}>
                <ToolbarGroup>
                    <ToolbarButton icon={<ArrowClockwiseRegular />} onClick={() => { fetchData(dateParser(from), dateParser(to)); }}>{nlsHPCC.Refresh}</ToolbarButton>
                    <DatePicker value={start} onSelectDate={onFromChange} placeholder={nlsHPCC.FromDate} className={styles.root} />
                    <ArrowRightFilled />
                    <DatePicker value={end} onSelectDate={onToChange} showCloseButton={true} placeholder={nlsHPCC.ToDate} className={styles.root} />
                    <ToolbarButton icon={<ArrowResetFilled title={nlsHPCC.Reset} />} onClick={() => { pushParams({ from: undefined, to: undefined }); }} title={nlsHPCC.Reset}></ToolbarButton>
                </ToolbarGroup>
                <ToolbarGroup>
                    <Label style={{ color: tokens.colorBrandForeground1 }} className={styles.label}>{nlsHPCC.Total}:</Label><Badge appearance="tint" color="brand">{data.length}</Badge>
                </ToolbarGroup>
            </Toolbar>}
            main={
                <SizeMe>{({ size }) =>
                    <div style={{ width: "100%", height: "100%", overflow: "hidden" }}>
                        {loading &&
                            <Field validationMessage={nlsHPCC.Refresh} validationState="none">
                                <ProgressBar />
                            </Field>
                        }
                        <FluentGrid
                            data={rows}
                            primaryID={"id"}
                            columns={fluentColumns}
                            setSelection={setSelection}
                            setTotal={setTotal}
                            refresh={refreshTable}
                            height={`${Math.max(0, size.height - (loading ? 44 : 0))}px`}
                            selectionMode={SelectionMode.none}
                        />
                    </div>
                }</SizeMe>
            }
        />
    </>;
};
