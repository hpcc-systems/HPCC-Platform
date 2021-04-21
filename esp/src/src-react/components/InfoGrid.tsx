import * as React from "react";
import { Checkbox, CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as domClass from "dojo/dom-class";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitExceptions } from "../hooks/Workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoGrid } from "./DojoGrid";
import { createCopyDownloadSelection } from "./Common";

function extractGraphInfo(msg) {
    const retVal: { graphID?: string, subgraphID?: string, activityID?: string, activityName?: string } = {};
    const parts = msg.split("Graph graph");
    if (parts.length > 1) {
        const parts1 = parts[1].split("[");
        if (parts1.length > 1) {
            retVal.graphID = "graph" + parts1[0];
            parts1.shift();
            const parts2 = parts1.join("[").split("], ");
            retVal.subgraphID = parts2[0];
            if (parts2.length > 1) {
                const parts3 = parts2[1].split("[");
                retVal.activityName = parts3[0];
                if (parts3.length > 1) {
                    const parts4 = parts3[1].split("]");
                    retVal.activityID = parts4[0];
                }
            }
        }
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
    const [grid, setGrid] = React.useState<any>(undefined);
    const [exceptions] = useWorkunitExceptions(wuid);
    const [selection, setSelection] = React.useState([]);

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
        { key: "errors", onRender: () => <Checkbox defaultChecked label={`${filterCounts.error || 0} ${nlsHPCC.Errors}`} onChange={(ev, value) => setErrorChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "warnings", onRender: () => <Checkbox defaultChecked label={`${filterCounts.warning || 0} ${nlsHPCC.Warnings}`} onChange={(ev, value) => setWarningChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "infos", onRender: () => <Checkbox defaultChecked label={`${filterCounts.info || 0} ${nlsHPCC.Infos}`} onChange={(ev, value) => setInfoChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "others", onRender: () => <Checkbox defaultChecked label={`${filterCounts.other || 0} ${nlsHPCC.Others}`} onChange={(ev, value) => setOtherChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> }
    ];

    const rightButtons: ICommandBarItemProps[] = [
        ...createCopyDownloadSelection(grid, selection, "errorwarnings.csv")
    ];

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("id")));
    const gridColumns = useConst({
        Severity: {
            label: nlsHPCC.Severity, field: "", width: 72, sortable: false,
            renderCell: function (object, value, node, options) {
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
            }
        },
        Source: { label: nlsHPCC.Source, field: "", width: 144, sortable: false },
        Code: { label: nlsHPCC.Code, field: "", width: 45, sortable: false },
        Message: {
            label: nlsHPCC.Message, field: "",
            sortable: false,
            formatter: function (Message, idx) {
                const info = extractGraphInfo(Message);
                if (info.graphID && info.subgraphID && info.activityID) {
                    const txt = "Graph " + info.graphID + "[" + info.subgraphID + "], " + info.activityName + "[" + info.activityID + "]";
                    Message = Message.replace(txt, "<a href='#' onClick='return false;' class='dgrid-row-url'>" + txt + "</a>");
                } else if (info.graphID && info.subgraphID) {
                    const txt = "Graph " + info.graphID + "[" + info.subgraphID + "]";
                    Message = Message.replace(txt, "<a href='#' onClick='return false;' class='dgrid-row-url'>" + txt + "</a>");
                } else {
                    Message = Utility.xmlEncode2(Message);
                }
                return Message;
            }
        },
        Column: { label: nlsHPCC.Col, field: "", width: 36, sortable: false },
        LineNo: { label: nlsHPCC.Line, field: "", width: 36, sortable: false },
        FileName: { label: nlsHPCC.FileName, field: "", width: 360, sortable: false }
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", {});
        if (clearSelection) {
            grid?.clearSelection();
        }
    };

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
        gridStore.setData(filteredExceptions);
        refreshTable();
        setFilterCounts(filterCounts);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, exceptions, errorChecked, warningChecked, infoChecked, otherChecked]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <DojoGrid type={"SimpleGrid"} store={gridStore} query={{}} sort={{}} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
