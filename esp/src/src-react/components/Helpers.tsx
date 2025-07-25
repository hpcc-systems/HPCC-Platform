import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { TreeItemValue } from "@fluentui/react-components";
import * as ESPRequest from "src/ESPRequest";
import { convertedSize } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HelperRow, useWorkunitHelpersTree } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { FlatTreeItem, FlatTreeEx } from "./controls/FlatTreeEx";
import { FetchEditor } from "./SourceEditor";

function getURL(wuid: string, item: HelperRow, option?: number) {
    let params = "";

    const uriEncodedParams: { [key: string]: any } = {
        "Description": encodeURIComponent(item.Orig?.Description ?? ""),
        "IPAddress": encodeURIComponent(item.Orig?.IPAddress ?? ""),
        "LogDate": encodeURIComponent(item.Orig?.LogDate ?? ""),
        "Name": encodeURIComponent(item.Orig?.Name ?? ""),
        "PID": encodeURIComponent(item.Orig?.PID ?? ""),
        "ProcessName": encodeURIComponent(item.Orig?.ProcessName ?? ""),
        "SlaveNumber": encodeURIComponent(item.Orig?.SlaveNumber ?? ""),
        "Type": encodeURIComponent(item.Type ?? ""),
        "Wuid": encodeURIComponent(wuid),
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
            params = `/WUFile/${item.Type}?Wuid=${uriEncodedParams.Wuid}&Name=${uriEncodedParams.Name}&Type=${uriEncodedParams.Type}&LogFormat=2`;
            break;
        case "postmortem":
            params = `/WUFile/${item.Type}?Wuid=${uriEncodedParams.Wuid}&Name=${uriEncodedParams.Name}&Type=${uriEncodedParams.Type}`;
            break;
        case "EclAgentLog":
            params = `/WUFile/${item.Type}?Wuid=${uriEncodedParams.Wuid}&Process=${uriEncodedParams.PID}&Name=${uriEncodedParams.Name}&Type=${uriEncodedParams.Type}`;
            break;
        case "ThorSlaveLog":
            params = `/WUFile?Wuid=${uriEncodedParams.Wuid}&Type=${uriEncodedParams.Type}&Process=${uriEncodedParams.ProcessName}&ClusterGroup=${uriEncodedParams.ProcessName}&LogDate=${uriEncodedParams.LogDate}&SlaveNumber=${uriEncodedParams.SlaveNumber}`;
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
        case "xml":
            params = `/WUFile?Wuid=${uriEncodedParams.Wuid}&Name=${uriEncodedParams.Name}&IPAddress=${uriEncodedParams.IPAddress}&Description=${uriEncodedParams.Description}&Type=${uriEncodedParams.Type}`;
            break;
        default:
            if (item.Type.indexOf("ThorLog") === 0)
                params = `/WUFile/${item.Type}?Wuid=${uriEncodedParams.Wuid}&Process=${uriEncodedParams.PID}&Name=${uriEncodedParams.Name}&Type=${uriEncodedParams.Type}`;
            break;
    }

    return ESPRequest.getBaseURL() + params + (option ? `&Option=${encodeURIComponent(option)}` : "&Option=1");
}

interface FlatTreeItemEx extends FlatTreeItem {
    type: string;
    url: string;
}

interface HelpersProps {
    wuid: string;
    parentUrl?: string;
    selectedTreeValue?: string;
}

export const Helpers: React.FunctionComponent<HelpersProps> = ({
    wuid,
    parentUrl = `/workunits/${wuid}/helpers`,
    selectedTreeValue
}) => {
    selectedTreeValue = selectedTreeValue?.split("::").join("/");

    const [fullscreen, setFullscreen] = React.useState<boolean>(false);
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [helpers, refreshData] = useWorkunitHelpersTree(wuid);
    const [checkedRows, setCheckedRows] = React.useState([]);
    const [checkedItems, setCheckedItems] = React.useState<TreeItemValue[]>([]);
    const [treeItems, setTreeItems] = React.useState<FlatTreeItemEx[]>([]);
    const [openItems, setOpenItems] = React.useState<Iterable<TreeItemValue>>([]);
    const [selectedTreeItem, setSelectedTreeItem] = React.useState<FlatTreeItemEx>();

    const setSelectedItem = React.useCallback((treeItemValue: string | number) => {
        pushUrl(`${parentUrl}/${("" + treeItemValue).split("/").join("::")}`);
    }, [parentUrl]);

    React.useEffect(() => {
        const item = treeItems.find(i => i.value === selectedTreeValue);
        setSelectedTreeItem(item);
    }, [selectedTreeValue, treeItems]);

    React.useEffect(() => {
        const rows = [];
        checkedItems.forEach(value => {
            const found = helpers.find(row => row.id === value || row?.Name?.indexOf("" + value) > -1);
            if (found && treeItems.find(item => item.value === value)?.url) {
                rows.push(found);
            }
        });
        setCheckedRows(rows);
    }, [checkedItems, helpers, treeItems]);

    React.useEffect(() => {
        const flatTreeItems: FlatTreeItemEx[] = [];
        helpers.forEach(helper => {
            flatTreeItems.push({
                ...helper,
                value: helper.id,
                parentValue: helper.parentId,
                content: helper.Description ?? helper.Name ?? helper.Type,
                content_parens: (helper.Type !== "folder" && helper.FileSize) ? convertedSize(helper.FileSize) : "",
                type: helper.Type,
                url: helper.Type === "folder" ? "" : getURL(wuid, helper)
            });
        });
        setTreeItems(flatTreeItems);
        setOpenItems(flatTreeItems.map(item => item.value));
    }, [helpers, wuid]);

    const treeItemLeafNodes = React.useMemo(() => {
        return treeItems.filter(item => item.url !== "");
    }, [treeItems]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refreshData();
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "selectAll", text: checkedItems.length === treeItemLeafNodes.length ? nlsHPCC.DeselectAll : nlsHPCC.SelectAll, iconProps: { iconName: checkedItems.length === treeItemLeafNodes.length ? "Checkbox" : "CheckboxComposite" },
            onClick: () => {
                if (checkedItems.length < treeItemLeafNodes.length) {
                    setCheckedItems(treeItemLeafNodes.map(i => i.value));
                } else {
                    setCheckedItems([]);
                }
            }
        },
        {
            key: "file", text: nlsHPCC.File, disabled: checkedRows.filter(item => item.url !== "").length === 0, iconProps: { iconName: "Download" },
            onClick: () => {
                checkedRows.forEach(item => {
                    window.open(getURL(wuid, item, 1));
                });
            }
        },
        {
            key: "zip", text: nlsHPCC.Zip, disabled: checkedRows.filter(item => item.url !== "").length === 0, iconProps: { iconName: "Download" },
            onClick: () => {
                checkedRows.forEach(item => {
                    window.open(getURL(wuid, item, 2));
                });
            }
        },
        {
            key: "gzip", text: nlsHPCC.GZip, disabled: checkedRows.filter(item => item.url !== "").length === 0, iconProps: { iconName: "Download" },
            onClick: () => {
                checkedRows.forEach(item => {
                    window.open(getURL(wuid, item, 3));
                });
            }
        }
    ], [checkedItems.length, checkedRows, refreshData, treeItemLeafNodes, wuid]);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "fullscreen", title: nlsHPCC.MaximizeRestore, iconProps: { iconName: fullscreen ? "ChromeRestore" : "FullScreen" },
            onClick: () => setFullscreen(!fullscreen)
        }
    ], [fullscreen]);

    React.useEffect(() => {
        if (dockpanel) {
            //  Should only happen once on startup  ---
            const layout: any = dockpanel.layout();
            if (Array.isArray(layout?.main?.sizes) && layout.main.sizes.length === 2) {
                layout.main.sizes = [0.3, 0.7];
                dockpanel.layout(layout).lazyRender();
            }
        }
    }, [dockpanel]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={rightButtons} />}
        main={
            <DockPanel hideSingleTabs onCreate={setDockpanel}>
                <DockPanelItem key="helpersTable" title="Helpers">
                    <FlatTreeEx treeItems={treeItems} openTreeValues={openItems} setOpenTreeValues={setOpenItems} checkedTreeValues={checkedItems} setCheckedTreeValues={setCheckedItems} setSelectedTreeValue={setSelectedItem} selectedTreeValue={selectedTreeValue} />
                </DockPanelItem>
                <DockPanelItem key="helperEditor" title="Helper" padding={4} location="split-right" relativeTo="helpersTable">
                    <FetchEditor url={selectedTreeItem?.type === "dll" ? "" : selectedTreeItem?.url} noDataMsg={selectedTreeItem?.type === "dll" ? nlsHPCC.CannotDisplayBinaryData : ""}></FetchEditor>
                </DockPanelItem>
            </DockPanel>
        }
    />;
};
