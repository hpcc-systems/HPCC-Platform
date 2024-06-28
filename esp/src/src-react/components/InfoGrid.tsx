import * as React from "react";
import { Checkbox, CommandBar, ICommandBarItemProps, Link, SelectionMode } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { formatCost, formatTwoDigits } from "src/Session";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitExceptions } from "../hooks/workunit";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { pivotItemStyle } from "../layouts/pivot";

function extractGraphInfo(msg) {
    const regex = /^([a-zA-Z0-9 :]+: )(graph graph(\d+)\[(\d+)\], )(([a-zA-Z]+)\[(\d+)\]: )?(.*)$/gmi;
    const matches = [...msg.matchAll(regex)];
    const retVal: { prefix?: string, graphID?: string, subgraphID?: string, activityID?: string, activityName?: string, message?: string } = {};
    if (matches.length > 0) {
        retVal.prefix = matches[0][1];
        retVal.graphID = matches[0][3];
        retVal.subgraphID = matches[0][4];
        retVal.activityName = matches[0][6];
        retVal.activityID = matches[0][7];
        retVal.message = matches[0][8];
    }
    return retVal;
}

interface FilterCounts {
    cost: number,
    penalty: number,
    error: number,
    warning: number,
    info: number,
    other: number
}

interface InfoGridProps {
    wuid?: string;
    syntaxErrors?: any[];
}

export const InfoGrid: React.FunctionComponent<InfoGridProps> = ({
    wuid = null,
    syntaxErrors = []
}) => {

    const [costChecked, setCostChecked] = React.useState(true);
    const [errorChecked, setErrorChecked] = React.useState(true);
    const [warningChecked, setWarningChecked] = React.useState(true);
    const [infoChecked, setInfoChecked] = React.useState(true);
    const [otherChecked, setOtherChecked] = React.useState(true);
    const [filterCounts, setFilterCounts] = React.useState<FilterCounts>({ cost: 0, penalty: 0, error: 0, warning: 0, info: 0, other: 0 });
    const [exceptions] = useWorkunitExceptions(wuid);
    const [errors, setErrors] = React.useState<any[]>([]);
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        { key: "errors", onRender: () => <Checkbox defaultChecked label={`${filterCounts.error || 0} ${nlsHPCC.Errors}`} onChange={(ev, value) => setErrorChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "costs", onRender: () => <Checkbox defaultChecked label={`${filterCounts.cost || 0} ${nlsHPCC.Costs}`} onChange={(ev, value) => setCostChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "warnings", onRender: () => <Checkbox defaultChecked label={`${filterCounts.warning || 0} ${nlsHPCC.Warnings}`} onChange={(ev, value) => setWarningChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "infos", onRender: () => <Checkbox defaultChecked label={`${filterCounts.info || 0} ${nlsHPCC.Infos}`} onChange={(ev, value) => setInfoChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "others", onRender: () => <Checkbox defaultChecked label={`${filterCounts.other || 0} ${nlsHPCC.Others}`} onChange={(ev, value) => setOtherChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> }
    ], [filterCounts.cost, filterCounts.error, filterCounts.info, filterCounts.other, filterCounts.warning]);

    React.useEffect(() => {
        if (syntaxErrors.length) {
            setErrors(syntaxErrors);
        } else {
            setErrors(exceptions);
        }
    }, [syntaxErrors, exceptions]);

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            Severity: {
                label: nlsHPCC.Severity, width: 72, sortable: false,
                className: (value, row) => {
                    switch (value) {
                        case "Error":
                            return "ErrorCell";
                        case "Alert":
                            return "AlertCell";
                        case "Cost":
                            return "CostCell";
                        case "Warning":
                            return "WarningCell";
                    }
                    return "";
                }
            },
            Cost: {
                label: `${nlsHPCC.Source} / ${nlsHPCC.Cost}`, width: 144,
                formatter: (Source, row) => {
                    if (Source === "Cost Optimizer") {
                        return formatCost(+row.Cost);
                    }
                    return Source;
                }
            },
            Priority: {
                label: `${nlsHPCC.Priority} / ${nlsHPCC.TimePenalty}`, width: 144, sortable: false,
                formatter: (Priority, row) => {
                    if (row.Source === "Cost Optimizer") {
                        return `${formatTwoDigits(+row.Priority / 1000)} (${nlsHPCC.Seconds})`;
                    }
                    return Priority;
                }
            },
            Code: { label: nlsHPCC.Code, width: 45 },
            Message: {
                label: nlsHPCC.Message,
                sortable: true,
                formatter: (Message, idx) => {
                    const info = extractGraphInfo(Message);
                    if (info.graphID && info.subgraphID) {
                        let txt = `Graph ${info.graphID}[${info.subgraphID}]`;
                        if (info.activityName && info.activityID) {
                            txt = `Graph ${info.graphID}[${info.subgraphID}], ${info.activityName} [${info.activityID}]`;
                        }
                        return <><span>{info?.prefix}<Link style={{ marginRight: 3 }} href={`#/workunits/${wuid}/metrics/sg${info.subgraphID}`}>{txt}</Link>{info?.message}</span></>;
                    }
                    return Message;
                },
                fluentColumn: {
                    flexGrow: 1,
                    minWidth: 320,
                    isResizable: true
                }
            },
            Column: { label: nlsHPCC.Col, width: 36 },
            LineNo: { label: nlsHPCC.Line, width: 36 },
            Activity: {
                label: nlsHPCC.Activity, width: 56,
                formatter: (activityId, row) => {
                    return activityId ? <Link href={`#/workunits/${wuid}/metrics/a${activityId}`}>a{activityId}</Link> : "";
                }
            },
            FileName: {
                label: nlsHPCC.FileName,
                fluentColumn: {
                    flexGrow: 2,
                    minWidth: 320,
                    isResizable: true
                }
            }
        };
    }, [wuid]);

    const copyButtons = useCopyButtons(columns, selection, "errorwarnings");

    React.useEffect(() => {
        const filterCounts: FilterCounts = {
            cost: 0,
            penalty: 0,
            error: 0,
            warning: 0,
            info: 0,
            other: 0
        };
        const filteredExceptions = errors?.map((row, idx) => {
            if (row.Source === "Cost Optimizer") {
                row.Severity = "Cost";
            }
            switch (row.Severity) {
                case "Cost":
                    filterCounts.cost++;
                    break;
                case "Error":
                    filterCounts.error++;
                    break;
                case "Warning":
                    filterCounts.warning++;
                    break;
                case "Info":
                    filterCounts.info++;
                    break;
                default:
                    filterCounts.other++;
                    break;
            }
            return {
                id: idx,
                ...row
            };
        }).filter(row => {
            if (!costChecked && row.Severity === "Cost") {
                return false;
            } else if (!errorChecked && row.Severity === "Error") {
                return false;
            } else if (!warningChecked && row.Severity === "Warning") {
                return false;
            } else if (!infoChecked && row.Severity === "Info") {
                return false;
            } else if (!otherChecked && row.Severity !== "Cost" && row.Severity !== "Error" && row.Severity !== "Warning" && row.Severity !== "Info") {
                return false;
            }
            return true;
        }).sort((l, r) => {
            if (l.Severity === r.Severity) {
                return 0;
            } else if (l.Severity === "Error") {
                return -1;
            } else if (r.Severity === "Error") {
                return 1;
            } else if (l.Severity === "Alert") {
                return -1;
            } else if (r.Severity === "Alert") {
                return 1;
            } else if (l.Severity === "Cost") {
                return -1;
            } else if (r.Severity === "Cost") {
                return 1;
            } else if (l.Severity === "Warning") {
                return -1;
            } else if (r.Severity === "Warning") {
                return 1;
            }
            return l.Severity.localeCompare(r.Severity);
        });
        setData(filteredExceptions);
        setFilterCounts(filterCounts);
        setSelection(filteredExceptions);
    }, [costChecked, errorChecked, errors, infoChecked, otherChecked, setSelection, warningChecked]);

    React.useEffect(() => {
        if (data.length) {
            const header = document.querySelector(".ms-DetailsList-headerWrapper");
            const viewport = document.querySelector(".ms-Viewport");
            if (header && viewport) {
                header.remove();
                header["style"].top = "-4px";
                viewport.prepend(header);
            }
        }
    }, [data.length]);

    return <div style={{ height: "100%", overflowY: "hidden" }}>
        <CommandBar items={buttons} farItems={copyButtons} />
        <SizeMe monitorHeight >{({ size }) =>
            <div style={{ height: "100%", overflowY: "hidden" }}>
                <div style={{ ...pivotItemStyle(size), overflowY: "hidden" }}>
                    <FluentGrid
                        data={data}
                        primaryID={"id"}
                        columns={columns}
                        setSelection={_ => { }}
                        setTotal={setTotal}
                        refresh={refreshTable}
                        height={`${size.height - (44 + 8 + 45 + 12)}px`}
                        selectionMode={SelectionMode.none}
                    ></FluentGrid>
                </div>
            </div>
        }</SizeMe>
    </div>;
};
