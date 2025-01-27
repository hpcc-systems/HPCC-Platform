import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { TreeItemValue } from "@fluentui/react-components";
import * as ESPRequest from "src/ESPRequest";
import nlsHPCC from "src/nlsHPCC";
import { HelperRow, useWorkunitHelpers } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { FlatItem, HelpersTree } from "./HelpersTree";
import { FetchEditor } from "./SourceEditor";

function getURL(item: HelperRow, option?) {
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

interface HelpersProps {
    wuid: string;
    mode?: "ecl" | "xml" | "text" | "yaml";
    url?: string;
    parentUrl?: string;
}

export const Helpers: React.FunctionComponent<HelpersProps> = ({
    wuid,
    mode,
    url,
    parentUrl = `/workunits/${wuid}/helpers`
}) => {

    const [fullscreen, setFullscreen] = React.useState<boolean>(false);
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [helpers, refreshData] = useWorkunitHelpers(wuid);
    const [noDataMsg, setNoDataMsg] = React.useState("");
    const [checkedRows, setCheckedRows] = React.useState([]);
    const [checkedItems, setCheckedItems] = React.useState([]);
    const [selection, setSelection] = React.useState<HelperRow>();
    const [treeItems, setTreeItems] = React.useState<FlatItem[]>([]);
    const [openItems, setOpenItems] = React.useState<Iterable<TreeItemValue>>([]);

    React.useEffect(() => {
        helpers.forEach(helper => {
            helper.Orig = {
                url: getURL(helper),
                ...helper.Orig
            };
        });
    }, [helpers]);

    React.useEffect(() => {
        setSelection(helpers.filter(item => url === getURL(item))[0]);
    }, [helpers, url]);

    React.useEffect(() => {
        if (helpers.length && selection !== undefined) {
            setNoDataMsg(selection?.Type === "dll" ? nlsHPCC.CannotDisplayBinaryData : "");
        }
    }, [helpers, selection]);

    React.useEffect(() => {
        const rows = [];
        checkedItems.forEach(value => {
            const filtered = helpers.filter(row => row.id === value || row?.Name?.indexOf(value) > -1)[0];
            if (treeItems.filter(item => item.value === value)[0].url && filtered) {
                rows.push(filtered);
            }
        });
        setCheckedRows(rows);
    }, [checkedItems, helpers, treeItems]);

    React.useEffect(() => {
        const flatTreeItems: FlatItem[] = [];
        helpers.forEach(helper => {
            let parentValue;
            const helperPath = helper.Path?.replace("/var/lib/HPCCSystems/", "");
            const fileName = helper.Name?.split("/").pop();
            if (helperPath) {
                const pathDirs = helperPath.split("/");
                let parentFolder;
                let folderName;
                for (let i = 0; i < pathDirs.length; i++) {
                    folderName = parentFolder ? parentFolder + "/" + pathDirs[i] : pathDirs[i];
                    if (flatTreeItems.filter(item => item.value === folderName).length === 0) {
                        flatTreeItems.push({
                            value: folderName,
                            content: pathDirs[i],
                            parentValue: parentFolder,
                            url: ""
                        });
                    }
                    if (!parentFolder) {
                        parentFolder = pathDirs[i];
                    } else {
                        parentFolder += "/" + pathDirs[i];
                    }
                }
                flatTreeItems.push({
                    value: helperPath + "/" + fileName,
                    content: fileName,
                    fileSize: helper.FileSize,
                    parentValue: parentFolder,
                    url: helper.Orig.url
                });
            } else {
                flatTreeItems.push({
                    value: helper.id,
                    parentValue: parentValue ?? undefined,
                    content: helper.Description ?? helper.Name ?? helper.Type,
                    fileSize: helper.FileSize,
                    url: helper.Orig.url
                });
            }
        });
        setTreeItems(flatTreeItems.sort((a, b) => {
            if (a.parentValue === undefined && b.parentValue === undefined) return 0;
            if (a.parentValue === undefined) return -1;
            if (b.parentValue === undefined) return 1;
            return a.parentValue?.toString().localeCompare(b.parentValue?.toString(), undefined, { ignorePunctuation: false });
        }));
        setOpenItems(flatTreeItems.map(item => item.value));
    }, [helpers]);

    const treeItemLeafNodes = React.useMemo(() => {
        return treeItems.filter(item => item.url !== "");
    }, [treeItems]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refreshData();
                pushUrl(`${parentUrl}`);
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
                    window.open(getURL(item, 1));
                });
            }
        },
        {
            key: "zip", text: nlsHPCC.Zip, disabled: checkedRows.filter(item => item.url !== "").length === 0, iconProps: { iconName: "Download" },
            onClick: () => {
                checkedRows.forEach(item => {
                    window.open(getURL(item, 2));
                });
            }
        },
        {
            key: "gzip", text: nlsHPCC.GZip, disabled: checkedRows.filter(item => item.url !== "").length === 0, iconProps: { iconName: "Download" },
            onClick: () => {
                checkedRows.forEach(item => {
                    window.open(getURL(item, 3));
                });
            }
        }
    ], [checkedItems, checkedRows, parentUrl, refreshData, treeItemLeafNodes]);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "fullscreen", title: nlsHPCC.MaximizeRestore, iconProps: { iconName: fullscreen ? "ChromeRestore" : "FullScreen" },
            onClick: () => setFullscreen(!fullscreen)
        }
    ], [fullscreen]);

    const setSelectedItem = React.useCallback((selId: string) => {
        pushUrl(`${parentUrl}/${selId}`);
    }, [parentUrl]);

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
                    {   //  Only render after archive is loaded (to ensure it "defaults to open") ---
                        helpers?.length &&
                        <HelpersTree treeItems={treeItems} openItems={openItems} setOpenItems={setOpenItems} checkedItems={checkedItems} setCheckedItems={setCheckedItems} setSelectedItem={setSelectedItem} selectedUrl={url} />
                    }
                </DockPanelItem>
                <DockPanelItem key="helperEditor" title="Helper" padding={4} location="split-right" relativeTo="helpersTable">
                    <FetchEditor url={helpers && selection?.Type && selection?.Type !== "dll" ? url : null} mode={mode} noDataMsg={noDataMsg}></FetchEditor>
                </DockPanelItem>
            </DockPanel>
        }
    />;
};
