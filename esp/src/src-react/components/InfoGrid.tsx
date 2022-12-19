import * as React from "react";
import { Checkbox, CommandBar, ICommandBarItemProps, Link } from "@fluentui/react";
import * as domClass from "dojo/dom-class";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { useWorkunitExceptions } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";

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

interface InfoGridProps {
    wuid: string;
}

export const InfoGrid: React.FunctionComponent<InfoGridProps> = ({
    wuid
}) => {

    const [errorChecked, setErrorChecked] = React.useState(true);
    const [warningChecked, setWarningChecked] = React.useState(true);
    const [infoChecked, setInfoChecked] = React.useState(true);
    const [otherChecked, setOtherChecked] = React.useState(true);
    const [filterCounts, setFilterCounts] = React.useState<any>({});
    const [exceptions] = useWorkunitExceptions(wuid);
    const [data, setData] = React.useState<any[]>([]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        { key: "errors", onRender: () => <Checkbox defaultChecked label={`${filterCounts.error || 0} ${nlsHPCC.Errors}`} onChange={(ev, value) => setErrorChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "warnings", onRender: () => <Checkbox defaultChecked label={`${filterCounts.warning || 0} ${nlsHPCC.Warnings}`} onChange={(ev, value) => setWarningChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "infos", onRender: () => <Checkbox defaultChecked label={`${filterCounts.info || 0} ${nlsHPCC.Infos}`} onChange={(ev, value) => setInfoChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "others", onRender: () => <Checkbox defaultChecked label={`${filterCounts.other || 0} ${nlsHPCC.Others}`} onChange={(ev, value) => setOtherChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> }
    ], [filterCounts.error, filterCounts.info, filterCounts.other, filterCounts.warning]);

    //  Grid ---
    const { Grid, copyButtons } = useFluentGrid({
        data,
        primaryID: "id",
        filename: "errorwarnings",
        columns: {
            Severity: {
                label: nlsHPCC.Severity, field: "", width: 72, sortable: false,
                renderCell: React.useCallback(function (object, value, node, options) {
                    switch (value) {
                        case "Error":
                            domClass.add(node, "ErrorCell");
                            break;
                        case "Alert":
                            domClass.add(node, "AlertCell");
                            break;
                        case "Warning":
                            domClass.add(node, "WarningCell");
                            break;
                    }
                    node.innerText = value;
                }, [])
            },
            Source: { label: nlsHPCC.Source, field: "", width: 144, sortable: false },
            Code: { label: nlsHPCC.Code, field: "", width: 45, sortable: false },
            Message: {
                label: nlsHPCC.Message, field: "",
                sortable: false,
                formatter: React.useCallback(function (Message, idx) {
                    const info = extractGraphInfo(Message);
                    if (info.graphID && info.subgraphID) {
                        let txt = `Graph ${info.graphID}[${info.subgraphID}]`;
                        if (info.activityName && info.activityID) {
                            txt = `Graph ${info.graphID}[${info.subgraphID}], ${info.activityName} [${info.activityID}]`;
                        }
                        return <><span>{info?.prefix}<Link style={{ marginRight: 3 }} href={`#/workunits/${wuid}/metrics/sg${info.subgraphID}`}>{txt}</Link>{info?.message}</span></>;
                    } else {
                        Message = Utility.xmlEncode2(Message);
                    }
                    return Message;
                }, [wuid])
            },
            Column: { label: nlsHPCC.Col, field: "", width: 36, sortable: false },
            LineNo: { label: nlsHPCC.Line, field: "", width: 36, sortable: false },
            FileName: { label: nlsHPCC.FileName, field: "", width: 360, sortable: false }
        }
    });

    React.useEffect(() => {
        const filterCounts = {
            error: 0,
            warning: 0,
            info: 0,
            other: 0
        };
        const filteredExceptions = exceptions.map((row, idx) => {
            switch (row.Severity) {
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
            if (!errorChecked && row.Severity === "Error") {
                return false;
            } else if (!warningChecked && row.Severity === "Warning") {
                return false;
            } else if (!infoChecked && row.Severity === "Info") {
                return false;
            } else if (!otherChecked && row.Severity !== "Error" && row.Severity !== "Warning" && row.Severity !== "Info") {
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
            } else if (l.Severity === "Warning") {
                return -1;
            } else if (r.Severity === "Warning") {
                return 1;
            }
            return l.Severity.localeCompare(r.Severity);
        });
        setData(filteredExceptions);
        setFilterCounts(filterCounts);
    }, [errorChecked, exceptions, infoChecked, otherChecked, warningChecked]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <Grid />
        }
    />;
};
