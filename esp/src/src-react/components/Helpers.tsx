import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link, ScrollablePane, Sticky } from "@fluentui/react";
import * as domClass from "dojo/dom-class";
import * as ESPRequest from "src/ESPRequest";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { HelperRow, useWorkunitHelpers } from "../hooks/workunit";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";

function canShowContent(type: string) {
    switch (type) {
        case "dll":
            return false;
    }
    return true;
}

function getURL(item: HelperRow, option) {
    let params = "";

    const uriEncodedParams: { [key: string]: any } = {
        "Description": encodeURIComponent(item.Orig.Description),
        "IPAddress": encodeURIComponent(item.Orig.IPAddress),
        "LogDate": encodeURIComponent(item.Orig.LogDate),
        "Name": encodeURIComponent(item.Orig.Name),
        "PID": encodeURIComponent(item.Orig.PID),
        "ProcessName": encodeURIComponent(item.Orig.ProcessName),
        "SlaveNumber": encodeURIComponent(item.Orig.SlaveNumber),
        "Type": encodeURIComponent(item.Orig.Type),
        "Wuid": encodeURIComponent(item.workunit.Wuid),
    };

    switch (item.Type) {
        case "dll":
            const parts = item.Orig.Name.split("/");
            if (parts.length) {
                const leaf = parts[parts.length - 1];
                params = `/WUFile/${leaf}?Wuid=${uriEncodedParams.Wuid}&Name=${uriEncodedParams.Name}&Type=${uriEncodedParams.Type}`;
            }
            break;
        case "res":
            params = `/WUFile/res.txt?Wuid=${uriEncodedParams.Wuid}&Type=${uriEncodedParams.Type}`;
            break;
        case "ComponentLog":
            params = `/WUFile/${item.Type}?Wuid=${uriEncodedParams.Wuid}&Name=${uriEncodedParams.Name}&Type=${uriEncodedParams.Type}`;
            break;
        case "postmortem":
            params = `/WUFile/${item.Type}?Wuid=${uriEncodedParams.Wuid}&Name=${uriEncodedParams.Name}&Type=${uriEncodedParams.Type}`;
            break;
        case "postmortem":
            params = "/WUFile/" + item.Type + "?Wuid=" + encodeURIComponent(item.workunit.Wuid) + "&Name=" + encodeURIComponent(item.Orig.Name) + "&Type=" + encodeURIComponent(item.Orig.Type);
            break;
        case "EclAgentLog":
            params = `/WUFile/${item.Type}?Wuid=${uriEncodedParams.Wuid}&Process=${uriEncodedParams.PID}&Name=${uriEncodedParams.Name}&Type=${uriEncodedParams.Type}`;
            break;
        case "ThorSlaveLog":
            params = `/WUFile?Wuid=${uriEncodedParams.Wuid}&Process=${uriEncodedParams.ProcessName}&ClusterGroup=${uriEncodedParams.ProcessName}&LogDate=${uriEncodedParams.LogDate}&SlaveNumber=${uriEncodedParams.SlaveNumber}&Type=${uriEncodedParams.Type}`;
            break;
        case "Archive Query":
            params = `/WUFile/ArchiveQuery?Wuid=${uriEncodedParams.Wuid}&Name=ArchiveQuery&Type=ArchiveQuery`;
            break;
        case "ECL":
            params = `/WUFile?Wuid=${uriEncodedParams.Wuid}&Type=WUECL`;
            break;
        case "Workunit XML":
            params = `/WUFile?Wuid=${uriEncodedParams.Wuid}&Type=XML`;
            break;
        case "log":
        case "cpp":
        case "hpp":
            params = `/WUFile?Wuid=${uriEncodedParams.Wuid}&Name=${uriEncodedParams.Name}&IPAddress=${uriEncodedParams.IPAddress}&Description=${uriEncodedParams.Description}&Type=${uriEncodedParams.Type}`;
            break;
        case "xml":
            if (option !== undefined)
                params = `/WUFile?Wuid=${uriEncodedParams.Wuid}&Name=${uriEncodedParams.Name}&IPAddress=${uriEncodedParams.IPAddress}&Description=${uriEncodedParams.Description}&Type=${uriEncodedParams.Type}`;
            break;
        default:
            if (item.Type.indexOf("ThorLog") === 0)
                params = `/WUFile/${item.Type}?Wuid=${uriEncodedParams.Wuid}&Process=${uriEncodedParams.PID}&Name=${uriEncodedParams.Name}&Type=${uriEncodedParams.Type}`;
            break;
    }

    return ESPRequest.getBaseURL() + params + (option ? `&Option=${encodeURIComponent(option)}` : "&Option=1");
}

function getTarget(id, row: HelperRow) {
    if (canShowContent(row.Type)) {
        let sourceMode = "text";
        switch (row.Type) {
            case "ECL":
                sourceMode = "ecl";
                break;
            case "Workunit XML":
            case "Archive Query":
            case "xml":
                sourceMode = "xml";
                break;
        }
        return {
            sourceMode,
            url: getURL(row, id)
        };
    }
    return null;
}

const defaultUIState = {
    hasSelection: false,
    canShowContent: false
};

interface HelpersProps {
    wuid: string;
}

export const Helpers: React.FunctionComponent<HelpersProps> = ({
    wuid
}) => {

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [helpers, refreshData] = useWorkunitHelpers(wuid);
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const { Grid, selection, copyButtons } = useFluentGrid({
        data,
        primaryID: "id",
        filename: "helpers",
        columns: {
            sel: selector({
                width: 27,
                selectorType: "checkbox"
            }),
            Type: {
                label: nlsHPCC.Type,
                width: 160,
                formatter: function (Type, row) {
                    const target = getTarget(row.id, row);
                    if (target) {
                        return <Link href={`#/workunits/${row?.workunit?.Wuid}/helpers/${row.Type}?mode=${encodeURIComponent(target.sourceMode)}&src=${encodeURIComponent(target.url)}`}>{Type + (row?.Orig?.Description ? " (" + row.Orig.Description + ")" : "")}</Link>;
                    }
                    return Type;
                }
            },
            Description: {
                label: nlsHPCC.Description
            },
            FileSize: {
                label: nlsHPCC.FileSize,
                width: 90,
                renderCell: function (object, value, node, options) {
                    domClass.add(node, "justify-right");
                    node.innerText = Utility.valueCleanUp(value);
                },
            }
        }
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.canShowContent, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    const target = getTarget(selection[0].id, selection[0]);
                    if (target) {
                        window.location.href = `#/text?mode=${target.sourceMode}&src=${encodeURIComponent(target.url)}`;
                    }
                } else {
                    for (let i = 0; i < selection.length; ++i) {
                        const target = getTarget(selection[i].id, selection[i]);
                        if (target) {
                            window.open(`#/text?mode=${target.sourceMode}&src=${encodeURIComponent(target.url)}`, "_blank");
                        }
                    }
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "file", text: nlsHPCC.File, disabled: !uiState.hasSelection, iconProps: { iconName: "Download" },
            onClick: () => {
                selection.forEach(item => {
                    window.open(getURL(item, 1));
                });
            }
        },
        {
            key: "zip", text: nlsHPCC.Zip, disabled: !uiState.hasSelection, iconProps: { iconName: "Download" },
            onClick: () => {
                selection.forEach(item => {
                    window.open(getURL(item, 2));
                });
            }
        },
        {
            key: "gzip", text: nlsHPCC.GZip, disabled: !uiState.hasSelection, iconProps: { iconName: "Download" },
            onClick: () => {
                selection.forEach(item => {
                    window.open(getURL(item, 3));
                });
            }
        }

    ], [refreshData, selection, uiState.canShowContent, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        selection.forEach(row => {
            state.hasSelection = true;
            if (canShowContent(row.Type)) {
                state.canShowContent = true;
            }
        });
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        setData(helpers);
    }, [helpers]);

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <Grid />
    </ScrollablePane>;
};
