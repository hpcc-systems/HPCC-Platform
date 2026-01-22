import * as React from "react";
import { Accordion, AccordionHeader, AccordionItem, AccordionPanel, Badge, Field, makeStyles, ProgressBar, Label, Link, Text, Toolbar, ToolbarButton, tokens, ToolbarGroup } from "@fluentui/react-components";
import { ArrowClockwiseRegular, ArrowRightFilled, ArrowResetFilled, DismissCircleRegular, WarningRegular, InfoRegular } from "@fluentui/react-icons";
import { DatePicker } from "@fluentui/react-datepicker-compat";
import { timeFormat, timeParse } from "@hpcc-js/common";
import { Workunit } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { throttle } from "../util/throttle";
import { SizeMe } from "../layouts/SizeMe";

const logger = scopedLogger("src-react/components/WUSSummary.tsx");

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

export interface WUSSummaryProps {
    from?: string;
    to?: string;
}

export const WUSSummary: React.FunctionComponent<WUSSummaryProps> = ({
    to = dateFormatter(new Date()),
    from = dateFormatter(new Date(new Date().setDate(new Date().getDate() - 7)))
}) => {
    const end = dateParser(to);
    const start = dateParser(from);

    const [workunits, setWorkunits] = React.useState<Workunit[]>([]);
    const [completed, setCompleted] = React.useState<Workunit[]>([]);
    const [failed, setFailed] = React.useState<Workunit[]>([]);
    const [other, setOther] = React.useState<Workunit[]>([]);
    const [exceptions, setExceptions] = React.useState<Exceptions>({});
    const [fetched, setFetched] = React.useState(0);

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

        setWorkunits([]);
        setCompleted([]);
        setFailed([]);
        setOther([]);
        setExceptions({});

        const adjustedEnd = new Date(end);
        adjustedEnd.setDate(adjustedEnd.getDate() + 1);
        Workunit.query({ baseUrl: "" }, {
            StartDate: start.toISOString(),
            EndDate: adjustedEnd.toISOString(),
            PageSize: 999999
        }).then(response => {
            if (!cancelled) {
                setWorkunits(response);
                updateGroups(response);
            }
        }).catch(err => logger.error(err));

        return () => {
            cancelled = true;
        };
    }, []);

    React.useEffect(() => {
        let cancelled = false;

        setFetched(0);
        const throttledFetch = throttle(async (wu: Workunit) => {
            if (!cancelled) {
                return wu.fetchECLExceptions().then((exceptions) => {
                    if (!cancelled) {
                        setExceptions((prev) => {
                            const retVal = { ...prev };
                            exceptions.forEach(exception => {
                                if (!retVal[exception.Code]) {
                                    retVal[exception.Code] = { severity: exception.Severity, messageSummary: exception.Message, errors: [] };
                                } else {
                                    if (retVal[exception.Code].severity !== exception.Severity) {
                                        switch (retVal[exception.Code].severity) {
                                            case "Info":
                                                retVal[exception.Code].severity = exception.Severity;
                                                break;
                                            case "Warning":
                                                if (exception.Severity === "Error") {
                                                    retVal[exception.Code].severity = "Error";
                                                }
                                                break;
                                        }
                                    }
                                    retVal[exception.Code].messageSummary = partialStringMatch([retVal[exception.Code].messageSummary, exception.Message], "?");
                                }
                                retVal[exception.Code].errors.push({ severity: exception.Severity, message: exception.Message, workunit: wu });
                            });
                            return retVal;
                        });
                    }
                }).catch(err => {
                    logger.error(err);
                }).finally(() => {
                    if (!cancelled) {
                        setFetched(prev => prev + 1);
                    }
                });
            }
        }, 6, 333);

        failed.forEach(wu => {
            throttledFetch(wu);
        });

        return () => {
            cancelled = true;
        };
    }, [failed]);

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

    return <>
        <HolyGrail
            header={<Toolbar className={styles.toolbar}>
                <ToolbarGroup>
                    <ToolbarButton icon={<ArrowClockwiseRegular />} onClick={() => { fetchData(dateParser(from), dateParser(to)); }}>Refresh</ToolbarButton>
                    <DatePicker value={start} onSelectDate={onFromChange} placeholder={nlsHPCC.FromDate} className={styles.root} />
                    <ArrowRightFilled />
                    <DatePicker value={end} onSelectDate={onToChange} showCloseButton={true} placeholder={nlsHPCC.ToDate} className={styles.root} />
                    <ToolbarButton icon={<ArrowResetFilled title={nlsHPCC.Reset} />} onClick={() => { pushParams({ from: undefined, to: undefined }); }} title={nlsHPCC.Reset}></ToolbarButton>
                </ToolbarGroup>
                <ToolbarGroup>
                    <Label style={{ color: tokens.colorStatusSuccessForeground1 }} className={styles.label}>{nlsHPCC.Completed}:</Label><Badge appearance="tint" color="success">{completed.length}</Badge>
                    <Label style={{ color: tokens.colorStatusDangerForeground1 }} className={styles.label}>{nlsHPCC.Failed}:</Label><Badge appearance="tint" color="danger">{failed.length}</Badge>
                    <Label style={{ color: tokens.colorBrandForeground1 }} className={styles.label}>{nlsHPCC.Total}:</Label><Badge appearance="tint" color="brand">{workunits.length}</Badge>
                    <Label className={styles.label}>{nlsHPCC.Other}:</Label><Badge appearance="tint" color="informative">{other.length}</Badge>
                </ToolbarGroup>
            </Toolbar>}
            main={
                <SizeMe>{({ size }) =>
                    <div style={{ width: "100%", height: "100%" }}>
                        <div style={{ position: "absolute", width: "100%", height: `${size.height}px`, overflowY: "scroll" }}>
                            <Field validationMessage={`${nlsHPCC.FetchingErrorDetails}: ${fetched} ${nlsHPCC.of} ${failed.length}.`} validationState={fetched < failed.length ? "none" : "success"}>
                                <ProgressBar max={failed.length + 1} value={fetched + 1} />
                            </Field>
                            <Accordion multiple>
                                {Object.keys(exceptions).map(code => {
                                    return <AccordionItem key={code} value={code}>
                                        <AccordionHeader icon={severityIcon(exceptions[code].severity)} style={{ margin: 4, borderStyle: "solid", borderWidth: 1, borderColor: severityBorderColor(exceptions[code].severity), background: severityBackgroundColor(exceptions[code].severity) }}>{code} ({exceptions[code].errors.length}):  {exceptions[code].messageSummary}</AccordionHeader>
                                        <AccordionPanel>
                                            {exceptions[code].errors.map((err, idx) => {
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
                }</SizeMe >
            }
            footer={<></>}
            footerStyles={{}}
        /></ >;
};