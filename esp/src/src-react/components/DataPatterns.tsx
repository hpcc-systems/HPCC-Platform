import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, mergeStyleSets, ScrollablePane, ScrollbarVisibility, Sticky, StickyPositionType } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { DPWorkunit } from "src/DataPatterns/DPWorkunit";
import { Report } from "src/DataPatterns/Report";
import { useFile } from "../hooks/file";
import { useWorkunit } from "../hooks/workunit";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { Optimize } from "./forms/Optimize";
import { ShortVerticalDivider } from "./Common";
import { TargetClusterTextField } from "./forms/Fields";
import { WUStatus } from "src/react/wuStatus";

const dpStyles = mergeStyleSets({
    inlineDropdown: {
        marginTop: "6px",
        ".ms-Dropdown": {
            minWidth: "100px"
        }
    }
});

interface DataPatternsProps {
    cluster: string;
    logicalFile: string;
}

export const DataPatterns: React.FunctionComponent<DataPatternsProps> = ({
    cluster,
    logicalFile
}) => {

    const [file] = useFile(cluster, logicalFile);
    const dpWu = React.useMemo(() => {
        if (file) {
            return new DPWorkunit(cluster, logicalFile, file?.Modified);
        }
        return undefined;
    }, [cluster, file, logicalFile]);

    const [showOptimize, setShowOptimize] = React.useState(false);
    const [targetCluster, setTargetCluster] = React.useState<string>();
    const [wuid, setWuid] = React.useState<string>();
    const [wu, , , isComplete] = useWorkunit(wuid, true);

    const refreshData = React.useCallback((full: boolean = false) => {
        if (full) {
            dpWu?.clearCache();
        }
        dpWu?.resolveWU().then(wu => {
            setWuid(wu?.Wuid);
        });
    }, [dpWu]);

    React.useEffect(() => refreshData(), [refreshData]);

    const dpReport = React.useMemo(() => {
        if (wu && isComplete) {
            return new Report().wu(dpWu);
        }
    }, [wu, isComplete, dpWu]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refreshData(true);
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "analyze", text: nlsHPCC.Analyze, iconProps: { iconName: "AnalyticsView" },
            disabled: !!wu,
            onClick: () => {
                dpWu?.create(targetCluster).then(() => {
                    refreshData();
                });
            }
        },
        {
            key: "targetCluster", itemType: ContextualMenuItemType.Normal,
            commandBarButtonAs: () => <TargetClusterTextField
                key="targetClusterField"
                disabled={!!wu}
                placeholder={nlsHPCC.Target}
                className={dpStyles.inlineDropdown}
                optional={false}
                selectedKey={targetCluster}
                onChange={(ev, row) => {
                    setTargetCluster(row.key as string);
                }}
            />
        }, {
            key: "delete", text: nlsHPCC.Delete, iconProps: { iconName: "Delete" },
            disabled: !isComplete,
            onClick: () => {
                dpWu?.delete().then(() => {
                    setWuid("");
                    refreshData();
                });
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "optimize", text: nlsHPCC.Optimize,
            disabled: !isComplete,
            onClick: () => setShowOptimize(true)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "wuid", text: wuid,
            href: `#/workunits/${wuid}`,
        }
    ].filter(row => row.key !== "wuid" || !!wuid), [dpWu, isComplete, refreshData, targetCluster, wu, wuid]);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
    ], []);

    return <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
        <Sticky stickyPosition={StickyPositionType.Header}>
            <CommandBar items={buttons} farItems={rightButtons} />
        </Sticky>
        {wu?.isComplete() ?
            <AutosizeHpccJSComponent widget={dpReport} /> :
            wuid ? <div style={{ width: "512px", height: "64px", float: "right" }}>
                <WUStatus wuid={wuid}></WUStatus>
            </div> :
                <h3>{nlsHPCC.DataPatternsNotStarted}</h3>
        }
        <Optimize dpWu={dpWu} targetCluster={targetCluster} logicalFile={logicalFile} showForm={showOptimize} setShowForm={setShowOptimize} />
    </ScrollablePane>;

};
