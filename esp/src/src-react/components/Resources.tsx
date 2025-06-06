import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { hashHistory, pushSearch, updateParam } from "../util/history";
import { SearchParams } from "../util/hashUrl";
import { useWorkunitResources } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { ShortVerticalDivider } from "./Common";
import { IFrame } from "./IFrame";
import { ResourcesList } from "./ResourcesList";
import { FetchEditor, ModeT } from "./SourceEditor";

interface ResourcesProps {
    wuid: string;
    preview?: boolean;
}

export const Resources: React.FunctionComponent<ResourcesProps> = ({
    wuid,
    preview = true
}) => {

    const [resources, , , refreshData] = useWorkunitResources(wuid);
    const [selectedResource, setSelectedResource] = React.useState("");
    const [mode, setMode] = React.useState<ModeT>("xml");
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" }, onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "preview", text: nlsHPCC.Preview, canCheck: true, checked: preview ?? true, iconProps: { iconName: "FileHTML" },
            onClick: () => {
                updateParam("preview", !preview);
            }
        }
    ], [preview, refreshData]);

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

    React.useEffect(() => {
        const ext = selectedResource.substring(selectedResource.lastIndexOf(".") + 1).toLowerCase();
        switch (ext) {
            case "js":
            case "css":
            case "html":
                setMode(ext);
                break;
            case "xml":
            default:
                setMode("xml");
        }
        if (selectedResource) {
            const searchParams = new SearchParams(hashHistory.location.search);
            searchParams.param("url", selectedResource);
            searchParams.param("preview", preview);
            pushSearch(searchParams.params);
        }
    }, [preview, selectedResource]);

    React.useEffect(() => {
        if (resources.length) {
            const searchParams = new SearchParams(hashHistory.location.search);
            if (searchParams.param("url")) {
                setSelectedResource(searchParams.param("url")?.toString() ?? "");
            } else {
                setSelectedResource(resources[0]);
            }
        }
    }, [resources]);

    return <HolyGrail
        header={<CommandBar items={buttons} />}
        main={
            <DockPanel hideSingleTabs onCreate={setDockpanel}>
                <DockPanelItem key="helpersTable" title="Helpers">
                    <ResourcesList resources={resources} setSelection={setSelectedResource} selectedResource={selectedResource} />
                </DockPanelItem>
                <DockPanelItem key="helperEditor" title="Helper" padding={4} location="split-right" relativeTo="helpersTable">
                    {preview && selectedResource.indexOf(".html") > -1 ?
                        <IFrame src={`/WsWorkunits/${selectedResource}`} /> :
                        <FetchEditor url={resources && selectedResource ? `/WsWorkunits/${selectedResource}` : null} mode={mode}></FetchEditor>
                    }
                </DockPanelItem>
            </DockPanel>
        }
    />;
};
