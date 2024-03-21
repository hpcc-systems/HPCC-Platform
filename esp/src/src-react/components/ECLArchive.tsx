import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { Workunit, WUDetails, IScope } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitArchive } from "../hooks/workunit";
import { useWorkunitMetrics } from "../hooks/metrics";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { ECLArchiveTree } from "./ECLArchiveTree";
import { ECLArchiveEditor } from "./ECLArchiveEditor";
import { MetricsPropertiesTables } from "./MetricsPropertiesTables";

const logger = scopedLogger("src-react/components/ECLArchive.tsx");

const scopeFilterDefault: WUDetails.RequestNS.ScopeFilter = {
    MaxDepth: 999999,
    ScopeTypes: ["graph"]
};

const nestedFilterDefault: WUDetails.RequestNS.NestedFilter = {
    Depth: 999999,
    ScopeTypes: ["activity"]
};

interface ECLArchiveProps {
    wuid: string;
    parentUrl?: string;
    selection?: string;
}

export const ECLArchive: React.FunctionComponent<ECLArchiveProps> = ({
    wuid,
    parentUrl = `/workunits/${wuid}/eclsummary`,
    selection
}) => {
    const [fullscreen, setFullscreen] = React.useState<boolean>(false);
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [_archiveXmlStr, _workunit2, _state2, archive, refreshArchive] = useWorkunitArchive(wuid);
    const [metrics, _columns, _activities, _properties, _measures, _scopeTypes, _fetchStatus, refreshMetrics] = useWorkunitMetrics(wuid, scopeFilterDefault, nestedFilterDefault);
    const [markers, setMarkers] = React.useState<{ lineNum: number, label: string }[]>([]);
    const [selectionText, setSelectionText] = React.useState<string>("");
    const [selectedMetrics, setSelectedMetrics] = React.useState<IScope[]>([]);

    selection = selection ?? archive?.queryId();

    React.useEffect(() => {
        if (archive) {
            archive?.updateMetrics(metrics);
        }
    }, [archive, metrics]);

    React.useEffect(() => {
        if (metrics.length) {
            setSelectionText(archive?.content(selection) ?? "");
            setMarkers(archive?.markers(selection) ?? []);
            setSelectedMetrics(archive?.metrics(selection) ?? []);
        } else {
            if (archive && !archive.build) {
                const wu = Workunit.attach({ baseUrl: "" }, wuid);
                wu.fetchQuery().then(function (query) {
                    setSelectionText(query?.Text ?? "");
                }).catch(err => logger.error(err));
            }
        }
    }, [archive, metrics.length, selection, wuid]);

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

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refreshArchive();
                refreshMetrics();
                pushUrl(`${parentUrl}`);
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [parentUrl, refreshArchive, refreshMetrics]);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "copy", text: nlsHPCC.CopyToClipboard, disabled: !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
            onClick: () => {
                navigator?.clipboard?.writeText(selectionText);
            }
        }, {
            key: "fullscreen", title: nlsHPCC.MaximizeRestore, iconProps: { iconName: fullscreen ? "ChromeRestore" : "FullScreen" },
            onClick: () => setFullscreen(!fullscreen)
        }
    ], [selectionText, fullscreen]);

    return <HolyGrail fullscreen={fullscreen}
        header={<CommandBar items={buttons} farItems={rightButtons} />}
        main={
            <DockPanel hideSingleTabs onDockPanelCreate={setDockpanel}>
                <DockPanelItem key="scopesTable" title="Files" >
                    {   //  Only render after archive is loaded (to ensure it "defaults to open") ---
                        archive?.modAttrs.length &&
                        <ECLArchiveTree archive={archive} selectedAttrIDs={[selection]} setSelectedItem={setSelectedItem} />
                    }
                </DockPanelItem>
                <DockPanelItem key="eclEditor" title="ECL" padding={4} location="split-right" relativeTo="scopesTable">
                    <ECLArchiveEditor ecl={selectionText} markers={markers}></ECLArchiveEditor>
                </DockPanelItem>
                <DockPanelItem key="properties" title="Properties" location="split-bottom" relativeTo="scopesTable" >
                    <MetricsPropertiesTables scopes={selectedMetrics}></MetricsPropertiesTables>
                </DockPanelItem>
            </DockPanel>
        }
    />;
};
