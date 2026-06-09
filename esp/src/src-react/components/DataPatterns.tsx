import * as React from "react";
import { ScrollablePane, ScrollbarVisibility, Sticky, StickyPositionType } from "./controls/ScrollablePane";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { makeStyles } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";
import { DPWorkunit } from "src/DataPatterns/DPWorkunit";
import { Report } from "src/DataPatterns/Report";
import { useFile } from "../hooks/file";
import { useWorkunit } from "../hooks/workunit";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { Optimize } from "./forms/Optimize";
import { TargetClusterTextField } from "./forms/Fields";
import { WUStatus } from "src/react/wuStatus";

const useStyles = makeStyles({
    inlineDropdown: {
        minWidth: "100px",
        marginTop: "6px",
    },
});

interface DataPatternsProps {
    cluster: string;
    logicalFile: string;
}

export const DataPatterns: React.FunctionComponent<DataPatternsProps> = ({
    cluster,
    logicalFile
}) => {
    const styles = useStyles();

    const { file } = useFile(cluster, logicalFile);
    const dpWu = React.useMemo(() => {
        if (file) {
            return new DPWorkunit(cluster, logicalFile, file?.Modified);
        }
        return undefined;
    }, [cluster, file, logicalFile]);

    const [showOptimize, setShowOptimize] = React.useState(false);
    const [targetCluster, setTargetCluster] = React.useState<string>();
    const [wuid, setWuid] = React.useState<string>();
    const { workunit: wu, isComplete } = useWorkunit(wuid, true);

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
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "analyze", text: nlsHPCC.Analyze, iconProps: { iconName: "AnalyticsView" },
            disabled: !!wu || !file?.Ecl,
            onClick: () => {
                dpWu?.create(targetCluster).then(() => {
                    refreshData();
                });
            }
        },
        {
            key: "targetCluster", itemType: ContextualMenuItemType.Normal,
            onRender: () => <TargetClusterTextField
                key="targetClusterField"
                disabled={!!wu}
                placeholder={nlsHPCC.Target}
                className={styles.inlineDropdown}
                required={true}
                selectedKey={targetCluster}
                onChange={(ev, option) => {
                    if (option && !Array.isArray(option)) setTargetCluster(option.key as string);
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
        { key: "divider_2", itemType: ContextualMenuItemType.Divider },
        {
            key: "optimize", text: nlsHPCC.Optimize,
            disabled: !isComplete,
            onClick: () => setShowOptimize(true)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider },
        {
            key: "wuid", text: wuid,
            href: `#/workunits/${wuid}`,
        }
    ].filter(row => row.key !== "wuid" || !!wuid), [dpWu, file, isComplete, refreshData, styles.inlineDropdown, targetCluster, wu, wuid]);

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
                file?.Ecl ?
                    <h3>{nlsHPCC.DataPatternsNotStarted}</h3> :
                    <h3>{nlsHPCC.DataPatternsDefnNotFound}</h3>

        }
        <Optimize dpWu={dpWu} targetCluster={targetCluster} logicalFile={logicalFile} showForm={showOptimize} setShowForm={setShowOptimize} />
    </ScrollablePane>;

};
