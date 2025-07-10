import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { pushUrl } from "../util/history";
import { useWorkunitResources } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { ResourcesList } from "./ResourcesList";
import { FetchEditor, ModeT } from "./SourceEditor";

interface ResourcesProps {
    wuid: string;
    parentUrl?: string;
    selectedResource?: string;
}

export const Resources: React.FunctionComponent<ResourcesProps> = ({
    wuid,
    parentUrl = `/workunits/${wuid}/resources`,
    selectedResource
}) => {
    selectedResource = selectedResource?.split("::").join("/");

    const [resources, , , refreshData] = useWorkunitResources(wuid);
    const [mode, setMode] = React.useState<ModeT>("xml");
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();

    const setSelectedItem = React.useCallback((resource: string) => {
        pushUrl(`${parentUrl}/${resource.split("/").join("::")}`);
    }, [parentUrl]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" }, onClick: () => refreshData()
        },

    ], [refreshData]);

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
        const ext = selectedResource?.substring(selectedResource?.lastIndexOf(".") + 1).toLowerCase();
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
    }, [selectedResource]);

    return <HolyGrail
        header={<CommandBar items={buttons} />}
        main={
            <DockPanel hideSingleTabs onCreate={setDockpanel}>
                <DockPanelItem key="helpersTable" title="Resources">
                    <ResourcesList resources={resources} setSelection={setSelectedItem} selectedResource={selectedResource} />
                </DockPanelItem>
                <DockPanelItem key="helperEditor" title="Resource" padding={4} location="split-right" relativeTo="helpersTable">
                    <FetchEditor url={selectedResource ? `/WsWorkunits/${selectedResource}` : ""} mode={mode} noDataMsg={""}></FetchEditor>
                </DockPanelItem>
            </DockPanel>
        }
    />;
};
