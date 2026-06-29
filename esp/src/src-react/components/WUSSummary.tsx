import * as React from "react";
import { Accordion, AccordionHeader, AccordionItem, AccordionPanel, Badge, Checkbox, Field, makeStyles, ProgressBar, Link, SearchBox, SearchBoxChangeEvent, Text, Toolbar, ToolbarButton, tokens, ToolbarDivider, ToolbarGroup } from "@fluentui/react-components";
import { ArrowClockwiseRegular, ArrowResetFilled, ArrowRightFilled, DismissCircleRegular, FilterRegular, WarningRegular, InfoRegular } from "@fluentui/react-icons";
import { useConst } from "@fluentui/react-hooks";
import { DatePicker } from "@fluentui/react-datepicker-compat";
import { timeFormat, timeParse } from "@hpcc-js/common";
import { Workunit, WsWorkunits } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { userKeyValStore } from "src/KeyValStore";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { throttle, ThrottleQueueItem } from "../util/throttle";
import { SizeMe } from "../layouts/SizeMe";
import { useUserStore } from "../hooks/store";

const logger = scopedLogger("src-react/components/WUSSummary.tsx");

const WUSSUMMARY_SHOWFAILED = "wusummary_showFailed";
const WUSSUMMARY_SHOWCOMPLETED = "wusummary_showCompleted";
const WUSSUMMARY_SHOWOTHER = "wusummary_showOther";
const WUSSUMMARY_SHOWERROR = "wusummary_showError";
const WUSSUMMARY_SHOWWARNING = "wusummary_showWarning";
const WUSSUMMARY_SHOWINFO = "wusummary_showInfo";

export function resetWUSummaryOptions() {
    const store = userKeyValStore();
    return Promise.all([
        store?.delete(WUSSUMMARY_SHOWFAILED),
        store?.delete(WUSSUMMARY_SHOWCOMPLETED),
        store?.delete(WUSSUMMARY_SHOWOTHER),
        store?.delete(WUSSUMMARY_SHOWERROR),
        store?.delete(WUSSUMMARY_SHOWWARNING),
        store?.delete(WUSSUMMARY_SHOWINFO)
    ]);
}

const CANCELLED = "THROTTLE_CANCELLED";

const useStyles = makeStyles({
    toolbar: {
        justifyContent: "space-between",
    },
    datePicker: {
        maxWidth: "200px"
    },
    checkbox: {
        marginRight: "8px"
    },
    searchBox: { flexGrow: 1, maxWidth: "none" }
});

const dateFormatter = timeFormat("%Y-%m-%d");
const dateParser = timeParse("%Y-%m-%d");

function partialStringMatch(strings: string[], placeholder: string = "{unique}") {
    if (strings.length === 0) return "";
    const commonParts = [];
    const firstString = strings[0];
    let currentCommon = "";
    for (let i = 0; i < firstString.length; i++) {
        const char = firstString[i];
        const matchesAll = strings.every(str => str[i] === char);

        if (matchesAll) {
            currentCommon += char;
        } else {
            if (currentCommon) {
                commonParts.push(currentCommon);
                currentCommon = "";
            }
            commonParts.push(placeholder);
        }
    }
    if (currentCommon) {
        commonParts.push(currentCommon);
    }
    return commonParts.join("");
}

function severityIcon(severity: string, colorize: boolean = false) {
    switch (severity) {
        case "Warning":
            return <WarningRegular color={colorize ? severityForegroundColor(severity) : undefined} />;
        case "Error":
            return <DismissCircleRegular color={colorize ? severityForegroundColor(severity) : undefined} />;
    }
    return <InfoRegular />;
}

function severityBackgroundColor(severity: string) {
    switch (severity) {
        case "Warning":
            return tokens.colorStatusWarningBackground1;
        case "Error":
            return tokens.colorStatusDangerBackground1;
    }
    return undefined;
}

function severityBorderColor(severity: string) {
    switch (severity) {
        case "Warning":
            return tokens.colorStatusWarningBorder1;
        case "Error":
            return tokens.colorStatusDangerBorder1;
    }
    return undefined;
}

function severityForegroundColor(severity: string) {
    switch (severity) {
        case "Warning":
            return tokens.colorStatusWarningForeground1;
        case "Error":
            return tokens.colorStatusDangerForeground1;
    }
    return undefined;
}

function severitySortOrder(severity: string) {
    switch (severity) {
        case "Error":
            return 0;
        case "Warning":
            return 1;
        default:
            return 2;
    }
}

interface Error {
    workunit: Workunit;
    severity: string;
    message: string;
}

interface Exceptions {
    [code: string]: {
        severity: string;
        messageSummary: string;
        errors: Error[];
    };
}

interface ExceptionFetchProgress {
    fetched: number;
    total: number;
}

interface UseThrottledExceptionFetchProps {
    failed: Workunit[];
    completed: Workunit[];
    other: Workunit[];
    showFailed: boolean;
    showCompleted: boolean;
    showOther: boolean;
}

function useThrottledExceptionFetch({
    failed,
    completed,
    other,
    showFailed,
    showCompleted,
    showOther
}: UseThrottledExceptionFetchProps): { exceptions: Exceptions; progress: ExceptionFetchProgress } {
    const fetchedCache = useConst(() => new Map<string, Promise<WsWorkunits.ECLException[]>>());
    const [exceptions, setExceptions] = React.useState<Exceptions>({});
    const [fetched, setFetched] = React.useState(0);

    React.useEffect(() => {
        let cancelled = false;

        setFetched(0);

        const throttleQueue: ThrottleQueueItem<WsWorkunits.ECLException[]>[] = [];
        const throttledFetch = throttle(async (wu: Workunit): Promise<WsWorkunits.ECLException[]> => {
            return wu.fetchECLExceptions();
        }, { parallel: 6, timeout: 333, queue: throttleQueue });

        const doFetch = async (wu: Workunit) => {
            if (cancelled) return;
            if (!fetchedCache.has(wu.Wuid)) {
                fetchedCache.set(wu.Wuid, throttledFetch(wu));
            }
            return fetchedCache.get(wu.Wuid).then((fetchedExceptions) => {
                if (cancelled) return;
                setExceptions((prev) => {
                    const retVal = { ...prev };
                    fetchedExceptions.forEach(exception => {
                        if (exception.Message?.startsWith("While expanding macro")) return;
                        const key = `${exception.Severity}|${exception.Code}`;
                        if (!retVal[key]) {
                            retVal[key] = { severity: exception.Severity, messageSummary: exception.Message, errors: [] };
                        } else {
                            retVal[key].messageSummary = partialStringMatch([retVal[key].messageSummary, exception.Message], "?");
                        }
                        retVal[key].errors.push({ severity: exception.Severity, message: exception.Message, workunit: wu });
                    });
                    return retVal;
                });
            }).catch(err => {
                if (cancelled) return;
                if (err === CANCELLED) return;
                logger.error(err);
            }).finally(() => {
                if (cancelled) return;
                setFetched(prev => prev + 1);
            });
        };

        setExceptions({});
        const selectedWorkunits: Workunit[] = [];
        if (showFailed) {
            selectedWorkunits.push(...failed);
        }
        if (showCompleted) {
            selectedWorkunits.push(...completed);
        }
        if (showOther) {
            selectedWorkunits.push(...other);
        }
        selectedWorkunits.forEach(wu => {
            doFetch(wu);
        });

        return () => {
            cancelled = true;
            throttleQueue.forEach(item => {
                item.reject(CANCELLED);
                fetchedCache.delete(item.args[0].Wuid);
            });
            throttleQueue.length = 0;
        };
    }, [failed, completed, other, showFailed, showCompleted, showOther, fetchedCache]);

    const progress = React.useMemo(() => {
        let total = 0;
        if (showFailed) {
            total += failed.length;
        }
        if (showCompleted) {
            total += completed.length;
        }
        if (showOther) {
            total += other.length;
        }
        return {
            fetched: fetched > total ? total : fetched,
            total
        };
    }, [showFailed, showCompleted, showOther, fetched, failed.length, completed.length, other.length]);

    return { exceptions, progress };
}

export interface WUSSummaryProps {
    from?: string;
    to?: string;
    filter?: string;
}

export const WUSSummary: React.FunctionComponent<WUSSummaryProps> = ({
    to = dateFormatter(new Date()),
    from = dateFormatter(new Date(new Date().setDate(new Date().getDate() - 7))),
    filter = ""
}) => {
    const end = dateParser(to);
    const start = dateParser(from);

    const [completed, setCompleted] = React.useState<Workunit[]>([]);
    const [failed, setFailed] = React.useState<Workunit[]>([]);
    const [other, setOther] = React.useState<Workunit[]>([]);
    const [searchBoxValue, setSearchBoxValue] = React.useState(filter);
    const [showFailed, setShowFailed] = useUserStore<boolean>(WUSSUMMARY_SHOWFAILED, true);
    const [showCompleted, setShowCompleted] = useUserStore<boolean>(WUSSUMMARY_SHOWCOMPLETED, false);
    const [showOther, setShowOther] = useUserStore<boolean>(WUSSUMMARY_SHOWOTHER, false);
    const [showError, setShowError] = useUserStore<boolean>(WUSSUMMARY_SHOWERROR, true);
    const [showWarning, setShowWarning] = useUserStore<boolean>(WUSSUMMARY_SHOWWARNING, false);
    const [showInfo, setShowInfo] = useUserStore<boolean>(WUSSUMMARY_SHOWINFO, false);

    React.useEffect(() => {
        setSearchBoxValue(filter);
    }, [filter]);

    const fetchData = React.useCallback((start: Date, end: Date) => {
        let cancelled = false;

        function updateGroups(workunits: Workunit[]) {
            const completed = [];
            const failed = [];
            const other = [];
            workunits.forEach(wu => {
                if (wu.isComplete() && !wu.isFailed()) {
                    completed.push(wu);
                } else if (wu.isFailed()) {
                    failed.push(wu);
                } else {
                    other.push(wu);
                }
            });
            setCompleted(completed);
            setFailed(failed);
            setOther(other);
        }

        setCompleted([]);
        setFailed([]);
        setOther([]);

        const adjustedEnd = new Date(end);
        adjustedEnd.setDate(adjustedEnd.getDate() + 1);
        Workunit.query({ baseUrl: "" }, {
            StartDate: start.toISOString(),
            EndDate: adjustedEnd.toISOString(),
            PageSize: 999999
        }).then(response => {
            if (!cancelled) {
                updateGroups(response);
            }
        }).catch(err => logger.error(err));

        return () => {
            cancelled = true;
        };
    }, []);

    const { exceptions, progress } = useThrottledExceptionFetch({
        failed,
        completed,
        other,
        showFailed,
        showCompleted,
        showOther
    });

    React.useEffect(() => {
        fetchData(dateParser(from), dateParser(to));
    }, [fetchData, from, to]);

    const styles = useStyles();

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

    const filterTimerRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);

    React.useEffect(() => {
        return () => {
            if (filterTimerRef.current) clearTimeout(filterTimerRef.current);
        };
    }, []);

    const onFilterChange = React.useCallback((_evt: SearchBoxChangeEvent, data: { value: string }) => {
        const value = data?.value ?? "";
        setSearchBoxValue(value);
        if (filterTimerRef.current) clearTimeout(filterTimerRef.current);
        filterTimerRef.current = setTimeout(() => {
            pushParams({ filter: value || undefined });
        }, 500);
    }, []);

    const onToggleShowFailed = React.useCallback(() => {
        setShowFailed(!showFailed);
    }, [setShowFailed, showFailed]);

    const onToggleShowCompleted = React.useCallback(() => {
        setShowCompleted(!showCompleted);
    }, [setShowCompleted, showCompleted]);

    const onToggleShowOther = React.useCallback(() => {
        setShowOther(!showOther);
    }, [setShowOther, showOther]);

    const onToggleShowError = React.useCallback(() => {
        setShowError(!showError);
    }, [setShowError, showError]);

    const onToggleShowWarning = React.useCallback(() => {
        setShowWarning(!showWarning);
    }, [setShowWarning, showWarning]);

    const onToggleShowInfo = React.useCallback(() => {
        setShowInfo(!showInfo);
    }, [setShowInfo, showInfo]);

    const severityCounts = React.useMemo(() => {
        const counts = { Error: 0, Warning: 0, Info: 0 };
        Object.values(exceptions).forEach(entry => {
            entry.errors.forEach(err => {
                if (err.severity === "Error") counts.Error++;
                else if (err.severity === "Warning") counts.Warning++;
                else counts.Info++;
            });
        });
        return counts;
    }, [exceptions]);

    const filteredExceptions = React.useMemo(() => {
        const lc = filter.toLowerCase();
        const result: { [key: string]: { severity: string; messageSummary: string; errors: Error[]; filteredErrors: Error[] } } = {};

        Object.entries(exceptions).forEach(([compositeKey, entry]) => {
            const actualCode = compositeKey.split("|")[1];
            const codeMatch = !lc || actualCode.toLowerCase().includes(lc);
            const summaryMatch = !lc || entry.messageSummary.toLowerCase().includes(lc);

            const filteredErrors = entry.errors.filter(e => {
                if (!showFailed && e.workunit.isFailed()) return false;
                if (!showCompleted && e.workunit.isComplete() && !e.workunit.isFailed()) return false;
                if (!showOther && !e.workunit.isComplete() && !e.workunit.isFailed()) return false;
                if (!showError && e.severity === "Error") return false;
                if (!showWarning && e.severity === "Warning") return false;
                if (!showInfo && e.severity !== "Error" && e.severity !== "Warning") return false;
                if (codeMatch || summaryMatch) return true;
                return e.message.toLowerCase().includes(lc) || e.workunit.Wuid?.toLowerCase().includes(lc);
            });

            if (filteredErrors.length > 0) {
                result[compositeKey] = { ...entry, filteredErrors };
            }
        });

        return result;
    }, [exceptions, filter, showFailed, showCompleted, showOther, showError, showWarning, showInfo]);

    const sortedFilteredExceptions = React.useMemo(() => {
        return Object.entries(filteredExceptions).sort(([compositeKeyA, entryA], [compositeKeyB, entryB]) => {
            const severityDelta = severitySortOrder(entryA.severity) - severitySortOrder(entryB.severity);
            if (severityDelta !== 0) {
                return severityDelta;
            }
            const codeA = compositeKeyA.split("|")[1];
            const codeB = compositeKeyB.split("|")[1];
            return codeA.localeCompare(codeB, undefined, { numeric: true, sensitivity: "base" });
        });
    }, [filteredExceptions]);

    return <>
        <HolyGrail
            header={<>
                <Toolbar className={styles.toolbar}>
                    <ToolbarGroup>
                        <ToolbarButton key="refresh" icon={<ArrowClockwiseRegular />} onClick={() => { fetchData(dateParser(from), dateParser(to)); }}>Refresh</ToolbarButton>
                        <DatePicker key="from" value={start} onSelectDate={onFromChange} placeholder={nlsHPCC.FromDate} className={styles.datePicker} />
                        <ArrowRightFilled key="date-range-separator" />
                        <DatePicker key="to" value={end} onSelectDate={onToChange} showCloseButton={true} placeholder={nlsHPCC.ToDate} className={styles.datePicker} />
                        <ToolbarButton key="reset" icon={<ArrowResetFilled title={nlsHPCC.Reset} />} onClick={() => { pushParams({ from: undefined, to: undefined, filter: undefined }); setSearchBoxValue(""); }} title={nlsHPCC.Reset}></ToolbarButton>
                        <ToolbarDivider />
                    </ToolbarGroup>
                    <ToolbarGroup>
                        <Checkbox checked={showFailed} onChange={onToggleShowFailed} className={styles.checkbox} label={<><span style={{ color: tokens.colorStatusDangerForeground1 }}>{nlsHPCC.Failed} </span><Badge appearance="tint" color="danger">{failed.length}</Badge></>} />
                        <Checkbox checked={showCompleted} onChange={onToggleShowCompleted} className={styles.checkbox} label={<><span style={{ color: tokens.colorStatusSuccessForeground1 }}>{nlsHPCC.Completed} </span><Badge appearance="tint" color="success">{completed.length}</Badge></>} />
                        <Checkbox checked={showOther} onChange={onToggleShowOther} className={styles.checkbox} label={<><span>{nlsHPCC.Other} </span><Badge appearance="tint" color="informative">{other.length}</Badge></>} />
                    </ToolbarGroup>
                </Toolbar>
                <Toolbar className={styles.toolbar}>
                    <SearchBox key="filter" value={searchBoxValue} onChange={onFilterChange} placeholder={nlsHPCC.Filter} contentBefore={<FilterRegular />} className={styles.searchBox} />
                    <ToolbarGroup>
                        <ToolbarDivider />
                        <Checkbox checked={showError} onChange={onToggleShowError} className={styles.checkbox} label={<><span style={{ color: tokens.colorStatusDangerForeground1 }}>{nlsHPCC.Error} </span><Badge appearance="tint" color="danger">{severityCounts.Error}</Badge></>} />
                        <Checkbox checked={showWarning} onChange={onToggleShowWarning} className={styles.checkbox} label={<><span style={{ color: tokens.colorStatusWarningForeground1 }}>{nlsHPCC.Warning} </span><Badge appearance="tint" color="warning">{severityCounts.Warning}</Badge></>} />
                        <Checkbox checked={showInfo} onChange={onToggleShowInfo} className={styles.checkbox} label={<><span>{nlsHPCC.Info} </span><Badge appearance="tint" color="informative">{severityCounts.Info}</Badge></>} />
                    </ToolbarGroup>
                </Toolbar>
            </>}
            main={<SizeMe>{({ size }) =>
                <div style={{ width: "100%", height: "100%" }}>
                    <div style={{ position: "absolute", width: "100%", height: `${size.height}px`, overflowY: "scroll" }}>
                        <Field validationMessage={`${nlsHPCC.FetchingErrorDetails}: ${progress.fetched} ${nlsHPCC.of} ${progress.total}.`} validationState={progress.fetched < progress.total ? "none" : "success"}>
                            <ProgressBar max={progress.total + 1} value={progress.fetched + 1} />
                        </Field>
                        <Accordion collapsible multiple>
                            {sortedFilteredExceptions.map(([compositeKey, exceptionEntry]) => {
                                const code = compositeKey.split("|")[1];
                                return <AccordionItem key={`accordion-item-${compositeKey}`} value={compositeKey}>
                                    <AccordionHeader icon={severityIcon(exceptionEntry.severity)} style={{ margin: 4, borderStyle: "solid", borderWidth: 1, borderColor: severityBorderColor(exceptionEntry.severity), background: severityBackgroundColor(exceptionEntry.severity) }}>{code} ({exceptionEntry.filteredErrors.length}):  {exceptionEntry.messageSummary}</AccordionHeader>
                                    <AccordionPanel>
                                        {exceptionEntry.filteredErrors.map((err, idx) => {
                                            return <div key={idx}>
                                                <Link href={`#/workunits/${err.workunit.Wuid}`}>{err.workunit.Wuid}</Link> - {severityIcon(err.severity, true)}
                                                <Text style={{ color: severityForegroundColor(err.severity) }}>
                                                    <code style={{ whiteSpace: "pre-wrap", display: "inline" }}>{err.message}</code>
                                                </Text>
                                                <br />
                                            </div>;
                                        })}
                                    </AccordionPanel>
                                </AccordionItem>;
                            })}
                        </Accordion>
                    </div>
                </div>
            }</SizeMe>}
        /></>;
};