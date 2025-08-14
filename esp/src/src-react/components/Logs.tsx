import * as React from "react";
import { GetLogsExRequest, LogaccessService, LogType, TargetAudience, WsLogaccess } from "@hpcc-js/comms";
import { Level, scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { formatDateString, logColor, removeAllExcept, timestampToDate, wuidToDate, wuidToDateTime, wuidToTime } from "src/Utility";
import { useLogAccessInfo } from "../hooks/platform";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { Filter } from "./forms/Filter";
import { Fields, DateRange } from "./forms/Fields";
import { LogsHeader } from "./LogsHeader";
import { LogsHeaderWithSuperDatePicker } from "./LogsHeaderWithSuperDatePicker";

export const service = new LogaccessService({ baseUrl: "" });

const logger = scopedLogger("src-react/components/Logs.tsx");

const eightHours = 8 * 60 * 60 * 1000;
const startTimeOffset = 1 * 60 * 60 * 1000;
const endTimeOffset = 23 * 60 * 60 * 1000;
const defaultStartDate = new Date(new Date().getTime() - startTimeOffset);

const FilterFields: Fields = {
    components: { type: "cloud-containername", label: nlsHPCC.ContainerName },
    instance: { type: "cloud-podname", label: nlsHPCC.PodName },
    audience: {
        type: "dropdown", label: nlsHPCC.Audience, options: [
            { key: TargetAudience.Operator, text: "Operator" },
            { key: TargetAudience.User, text: "User" },
            { key: TargetAudience.Programmer, text: "Programmer" },
            { key: TargetAudience.Audit, text: "Audit" },
        ]
    },
    class: {
        type: "dropdown-multi", label: nlsHPCC.Class, options: [
            { key: LogType.Disaster, text: "Disaster" },
            { key: LogType.Error, text: "Error" },
            { key: LogType.Warning, text: "Warning" },
            { key: LogType.Information, text: "Information" },
            { key: LogType.Progress, text: "Progress" },
            { key: LogType.Metric, text: "Metric" },
        ]
    },
    workunits: { type: "string", label: nlsHPCC.JobID },
    processid: { type: "string", label: nlsHPCC.ProcessID },
    threadid: { type: "string", label: nlsHPCC.ThreadID },
    message: { type: "string", label: nlsHPCC.Message },
    LogLineLimit: {
        type: "dropdown", label: nlsHPCC.LogLineLimit, options: [
            { key: 100, text: "100" },
            { key: 250, text: "250" },
            { key: 500, text: "500" },
            { key: 1000, text: "1000" },
        ], value: "100", optional: false
    },
    StartDate: { type: "datetime", label: nlsHPCC.FromDate },
    EndDate: { type: "datetime", label: nlsHPCC.ToDate },
};

function formatQuery(_request: any): Partial<GetLogsExRequest> {
    const request: Partial<GetLogsExRequest> = { ..._request };
    if (_request.StartDate) {
        request.StartDate = new Date(_request.StartDate);
    }
    if (_request.EndDate) {
        request.EndDate = new Date(_request.EndDate);
    }
    if (_request.class) {
        request.class = _request.class.split(",");
    }
    return request;
}

interface LogsProps {
    wuid?: string;
    filter?: Partial<GetLogsExRequest>;
    page?: number;
    setLogCount?: (count: number | string) => void;
    useSuperDatePicker?: boolean; // New prop to control header type
}
export const defaultFilter: Partial<GetLogsExRequest> = { StartDate: defaultStartDate };

const levelMap = (level) => {
    switch (level) {
        case "ERR":
            return Level.error;
        case "WRN":
            return Level.warning;
        case "PRO":
            return Level.debug;
        case "INF":
        default:
            return Level.info;
    }
};

const columnOrder: string[] = [WsLogaccess.LogColumnType.timestamp, WsLogaccess.LogColumnType.message];

export const Logs: React.FunctionComponent<LogsProps> = ({
    wuid,
    filter = defaultFilter,
    page,
    setLogCount,
    useSuperDatePicker = wuid ? false : true
}) => {

    const [showFilter, setShowFilter] = React.useState(false);
    const [data, setData] = React.useState<any[]>([]);

    // Auto-refresh state (only used when useSuperDatePicker is true)
    const [autoRefresh, setAutoRefresh] = React.useState(false);
    const [autoRefreshInterval, setAutoRefreshInterval] = React.useState(30); // 30 seconds default

    const [currentFilter, setCurrentFilter] = React.useState<Partial<GetLogsExRequest>>(() => ({
        ...filter,
        StartDate: filter.StartDate || (wuid ? wuidToDateTime(wuid) : defaultStartDate)
    }));

    const hasFilter = React.useMemo(() => Object.keys(currentFilter).length > 0, [currentFilter]);

    const [headerStartDateInput, setHeaderStartDateInput] = React.useState<Date | string | undefined>(() => {
        return currentFilter.StartDate;
    });
    const [headerEndDateInput, setHeaderEndDateInput] = React.useState<Date | string | undefined>(() => {
        return currentFilter.EndDate;
    });

    const [committedStartDate, setCommittedStartDate] = React.useState<Date | string | undefined>(() => {
        return currentFilter.StartDate;
    });
    const [committedEndDate, setCommittedEndDate] = React.useState<Date | string | undefined>(() => {
        return currentFilter.EndDate;
    });

    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({ page });

    const now = React.useMemo(() => new Date(), []);

    const { logsColumns: logColumns } = useLogAccessInfo();

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        // we've defined the columnOrder array above to ensure specific columns will
        // appear on the left-most side of the grid, eg timestamps and log messages
        const cols = logColumns?.sort((a, b) => {
            const logTypeA = columnOrder.indexOf(a.LogType);
            const logTypeB = columnOrder.indexOf(b.LogType);

            if (logTypeA >= 0) {
                if (logTypeB >= 0) { return logTypeA - logTypeB; }
                return -1;
            } else if (logTypeB >= 0) {
                return 1;
            } else {
                return 0;
            }
        });
        const retVal = {
            timestamp: {
                label: nlsHPCC.TimeStamp, width: 140, sortable: false,
                formatter: ts => {
                    if (ts) {
                        if (ts.indexOf(":") < 0) {
                            const date = timestampToDate(ts);
                            if (!isNaN(date.getTime())) {
                                return date.toISOString();
                            }
                            return ts;
                        }
                        return formatDateString(ts);
                    }
                },
            },
            message: { label: nlsHPCC.Message, width: 600, sortable: false, },
            components: { label: nlsHPCC.ContainerName, width: 150, sortable: false },
            instance: { label: nlsHPCC.PodName, width: 150, sortable: false },
            audience: { label: nlsHPCC.Audience, width: 60, sortable: false, },
            class: {
                label: nlsHPCC.Class, width: 40, sortable: false,
                formatter: level => {
                    const colors = logColor(levelMap(level));
                    const styles = { backgroundColor: colors.background, padding: "2px 6px", color: colors.foreground };
                    return <span style={styles}>{level}</span>;
                }
            },
            workunits: { label: nlsHPCC.JobID, width: 50, sortable: false, hidden: wuid !== undefined, },
            processid: { label: nlsHPCC.ProcessID, width: 75, sortable: false, },
            logid: { label: nlsHPCC.Sequence, width: 70, sortable: false, },
            threadid: { label: nlsHPCC.ThreadID, width: 60, sortable: false, },
        };
        const colTypes = cols?.map(c => c.LogType.toString()) ?? [];
        removeAllExcept(retVal, colTypes);
        return retVal;
    }, [logColumns, wuid]);

    const copyButtons = useCopyButtons(columns, selection, "logaccess");

    const query = React.useMemo(() => {
        const queryFilter = { ...currentFilter };

        if (wuid !== undefined) {
            queryFilter.workunits = wuid;
            if (typeof committedStartDate === "string") {
                queryFilter.StartDate = new Date(committedStartDate + ":00Z");
            } else if (committedStartDate) {
                queryFilter.StartDate = committedStartDate;
            } else {
                queryFilter.StartDate = new Date(`${wuidToDate(wuid)}T${wuidToTime(wuid)}Z`);
            }
        } else {
            if (typeof committedStartDate === "string") {
                queryFilter.StartDate = new Date(committedStartDate + ":00Z");
            } else if (committedStartDate) {
                queryFilter.StartDate = committedStartDate;
            } else {
                //assign a reasonable default start date if one isn't set
                queryFilter.StartDate = new Date(now.getTime() - eightHours);
            }

            if (typeof committedEndDate === "string") {
                queryFilter.EndDate = new Date(committedEndDate + ":00Z");
            } else if (committedEndDate) {
                queryFilter.EndDate = committedEndDate;
            } else {
                queryFilter.EndDate = new Date(now.getTime() + endTimeOffset);
            }
        }
        return formatQuery(queryFilter);
    }, [currentFilter, committedStartDate, committedEndDate, now, wuid]);

    const refreshData = React.useCallback(() => {
        setCommittedStartDate(headerStartDateInput);
        setCommittedEndDate(headerEndDateInput);

        // also update currentFilter so the Filter dialog stays in sync
        setCurrentFilter(prev => ({
            ...prev,
            StartDate: headerStartDateInput ? (typeof headerStartDateInput === "string" ? new Date(headerStartDateInput + ":00Z") : headerStartDateInput) : undefined,
            EndDate: headerEndDateInput ? (typeof headerEndDateInput === "string" ? new Date(headerEndDateInput + ":00Z") : headerEndDateInput) : undefined
        }));
    }, [headerStartDateInput, headerEndDateInput]);

    React.useEffect(() => {
        service.GetLogsEx(query as any).then(response => {
            setData(response.lines);
        }).catch(err => logger.error(err));
    }, [query]);

    const handleStartDateChange = React.useCallback((dateValue: string) => {
        setHeaderStartDateInput(dateValue);
    }, []);

    const handleEndDateChange = React.useCallback((dateValue: string) => {
        setHeaderEndDateInput(dateValue);
    }, []);

    const handleSuperDatePickerChange = React.useCallback((range: DateRange) => {
        setHeaderStartDateInput(range.startDate);
        setHeaderEndDateInput(range.endDate);

        // update current filter so the Filter dialog stays in sync
        setCurrentFilter(prev => ({
            ...prev,
            StartDate: range.startDate ? (typeof range.startDate === "string" ? new Date(range.startDate + ":00Z") : range.startDate) : undefined,
            EndDate: range.endDate ? (typeof range.endDate === "string" ? new Date(range.endDate + ":00Z") : range.endDate) : undefined
        }));

        setCommittedStartDate(range.startDate);
        setCommittedEndDate(range.endDate);
    }, []);

    // initial data load
    React.useEffect(() => {
        refreshData();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    //  Filter  ---
    const filterFields: Fields = React.useMemo(() => {
        const retVal: Fields = {};
        for (const field in FilterFields) {
            retVal[field] = {
                ...FilterFields[field],
                value: currentFilter[field] !== undefined ? currentFilter[field] : FilterFields[field].value
            };
            if (wuid !== undefined) {
                delete currentFilter.workunits;
                delete retVal.jobId;
            }
        }
        const colTypes = logColumns?.map(c => c.LogType.toString()) ?? [];
        removeAllExcept(retVal, [...colTypes, "LogLineLimit", "StartDate", "EndDate"]);
        return retVal;
    }, [currentFilter, logColumns, wuid]);

    const handleFilterApply = React.useCallback((values: any) => {
        setCurrentFilter(prev => ({ ...prev, ...values }));

        if (values.StartDate !== undefined) {
            setHeaderStartDateInput(values.StartDate);
        }
        if (values.EndDate !== undefined) {
            setHeaderEndDateInput(values.EndDate);
        }

        pushParams(values);
    }, []);

    return <HolyGrail
        header={
            useSuperDatePicker ? (
                <LogsHeaderWithSuperDatePicker
                    startDate={headerStartDateInput}
                    endDate={headerEndDateInput}
                    onDateChange={handleSuperDatePickerChange}
                    onRefresh={refreshData}
                    onShowFilter={() => setShowFilter(true)}
                    hasFilter={hasFilter}
                    copyButtons={copyButtons}
                    autoRefresh={autoRefresh}
                    onAutoRefreshChange={setAutoRefresh}
                    autoRefreshInterval={autoRefreshInterval}
                    onAutoRefreshIntervalChange={setAutoRefreshInterval}
                />
            ) : (
                <LogsHeader
                    startDate={headerStartDateInput}
                    endDate={headerEndDateInput}
                    onStartDateChange={handleStartDateChange}
                    onEndDateChange={handleEndDateChange}
                    onRefresh={refreshData}
                    onShowFilter={() => setShowFilter(true)}
                    hasFilter={hasFilter}
                    copyButtons={copyButtons}
                />
            )
        }
        main={
            <div style={{ position: "relative", height: "100%" }}>
                <FluentGrid
                    data={data}
                    primaryID={""}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={(total) => {
                        setTotal(total);
                        if (setLogCount) {
                            setLogCount(total);
                        }
                    }}
                    refresh={refreshTable}
                ></FluentGrid>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={handleFilterApply} />
            </div>
        }
    />;
};