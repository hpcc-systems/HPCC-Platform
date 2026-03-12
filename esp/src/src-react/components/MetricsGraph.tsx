import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { Label, Spinner } from "@fluentui/react-components";
import { typographyStyles } from "@fluentui/react-theme";
import { useConst } from "@fluentui/react-hooks";
import { bundleIcon, Folder20Filled, Folder20Regular, FolderOpen20Filled, FolderOpen20Regular } from "@fluentui/react-icons";
import { IScope } from "@hpcc-js/comms";
import nlsHPCC from "src/nlsHPCC";
import { FetchStatus, METRICS_GRAPH_TRACK_SELECTION, MetricsView, useMetricsGraphLayout } from "../hooks/metrics";
import { useUserStore } from "../hooks/store";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeComponent, AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { isLayoutComplete, LayoutStatus, MetricGraph, MetricGraphWidget } from "../util/metricGraph";
import { ShortVerticalDivider } from "./Common";
import { BreadcrumbInfo, OverflowBreadcrumb } from "./controls/OverflowBreadcrumb";

const LineageIcon = bundleIcon(Folder20Filled, Folder20Regular);
const SelectedLineageIcon = bundleIcon(FolderOpen20Filled, FolderOpen20Regular);
const TRANSITION_DURATION = 0;

export function idsToScopes(metrics: IScope[], ids?: string[]): IScope[] {
    if (!ids?.length) {
        return [];
    }

    const selectionSet = new Set(ids);
    return metrics.filter(item => selectionSet.has(item.id));
}

export function calcLineage(metricGraph: MetricGraph, selection?: IScope[], lsName?: string): { lineage: IScope[], lineageSelectionScope: IScope | undefined } {
    const lineage: IScope[] = [];

    if (selection?.length) {
        const lineages = selection.map(item => metricGraph.lineage(item));
        const minLen = lineages.reduce((min, l) => Math.min(min, l.length), Number.MAX_SAFE_INTEGER);

        for (let i = 0; i < minLen; ++i) {
            const item = lineages[0][i];
            if (!lineages.every(l => l[i] === item)) break;
            if (item.id && item.type !== "child" && metricGraph.isSubgraph(item) && !metricGraph.isVertex(item)) {
                lineage.push(item);
            }
        }
    }

    const lineageSelectionScope = (lsName && lineage.find(item => item.name === lsName)) || lineage[lineage.length - 1];

    return { lineage, lineageSelectionScope };
}

export interface MetricGraphData {
    metricGraph: MetricGraph;
    selectedMetrics: IScope[];
    lineage: IScope[];
    lineageSelectionScope: IScope | undefined;
    dot: string;
    svg: string;
    layoutStatus: LayoutStatus;
}

export function useMetricsGraphData(metrics: IScope[], view: MetricsView, lineageSelectionName?: string, selection?: string[]): MetricGraphData {
    const [dot, setDot] = React.useState<string>("");
    const { svg, layoutStatus } = useMetricsGraphLayout(dot);
    const [lineage, setLineage] = React.useState<IScope[]>([]);
    const [lineageSelectionScope, setLineageSelectionScope] = React.useState<IScope | undefined>(undefined);

    const metricGraph = useConst(() => new MetricGraph());

    const selectedMetrics = React.useMemo<IScope[]>(() => {
        return idsToScopes(metrics, selection);
    }, [metrics, selection]);

    React.useEffect(() => {
        if (metrics.length > 0) {
            metricGraph.load(metrics);
            setDot(metricGraph.graphTpl(lineageSelectionScope?.name ? [lineageSelectionScope.name] : [], view));
        } else {
            metricGraph.clear();
            setDot("");
        }
    }, [metricGraph, metrics, lineageSelectionScope, view]);

    React.useEffect(() => {
        const { lineage, lineageSelectionScope } = calcLineage(metricGraph, selectedMetrics, lineageSelectionName);
        setLineage(lineage);
        setLineageSelectionScope(lineageSelectionScope);
    }, [metricGraph, selectedMetrics, lineageSelectionName]);

    return { metricGraph, selectedMetrics, lineage, lineageSelectionScope, dot, svg, layoutStatus };
}

export interface MetricsGraphProps {
    metrics: IScope[],
    metricGraphData: MetricGraphData;
    selection?: string[];
    selectedMetricsSource: string;
    status: FetchStatus;
    onLineageSelectionChange: (selection?: string, replace?: boolean) => void;
    onSelectionChange: (selection?: string[]) => void;
}

export const MetricsGraph: React.FunctionComponent<MetricsGraphProps> = ({
    metrics,
    metricGraphData: { metricGraph, selectedMetrics, lineage, lineageSelectionScope, svg, layoutStatus },
    selection,
    selectedMetricsSource,
    status,
    onLineageSelectionChange,
    onSelectionChange
}) => {
    const [selectedMetricsPtr, setSelectedMetricsPtr] = React.useState<number>(-1);
    const [trackSelection, setTrackSelection] = useUserStore(METRICS_GRAPH_TRACK_SELECTION, true);
    const [isRenderComplete, setIsRenderComplete] = React.useState<boolean>(false);
    const [metricGraphWidgetReady, setMetricGraphWidgetReady] = React.useState<boolean>(false);

    // Data ---
    React.useEffect(() => {
        if (isLayoutComplete(layoutStatus) && lineage.length && lineageSelectionScope && lineage.find(item => item.name === lineageSelectionScope.name) === undefined) {
            onLineageSelectionChange(lineage[lineage.length - 1]?.name, true);
        }
    }, [layoutStatus, lineage, lineageSelectionScope, onLineageSelectionChange]);

    // Widget  ---
    const metricGraphWidget = useConst(() => new MetricGraphWidget()
        .zoomToFitLimit(1)
    );

    React.useEffect(() => {
        metricGraphWidget
            .on("selectionChanged", () => {
                const selection = metricGraphWidget.selection().filter(id => metricGraph.item(id)).map(id => metricGraph.item(id).id);
                onSelectionChange(selection);
            }, true)
            ;
    }, [metricGraph, metricGraphWidget, onSelectionChange]);

    React.useEffect(() => {
        let cancelled = false;
        if (metricGraphWidgetReady && isLayoutComplete(layoutStatus)) {
            const currentSVG = metricGraphWidget.svg();
            const sameSVG = currentSVG === svg;
            const currentSelection = metricGraphWidget.selection().sort();
            const newSelection = metrics.filter(m => selection?.indexOf(m.id) >= 0).map(m => m.name).filter(sel => !!sel).sort();
            const sameSelection = !sameSVG ? false : currentSelection.join() === newSelection.join();
            if (sameSVG && sameSelection) {
                setIsRenderComplete(sameSVG);
                return;
            }
            if (!sameSVG) {
                metricGraphWidget.svg(svg);
            }
            metricGraphWidget
                .renderPromise()
                .then(() => {
                    if (!cancelled && !sameSelection) {
                        metricGraphWidget
                            .selection(newSelection)
                            ;
                        if (trackSelection && selectedMetricsSource !== "metricGraphWidget") {
                            if (newSelection.length) {
                                if (sameSVG) {
                                    metricGraphWidget.centerOnSelection(TRANSITION_DURATION);
                                } else {
                                    metricGraphWidget.zoomToSelection(TRANSITION_DURATION);
                                }
                            } else {
                                metricGraphWidget.zoomToFit(TRANSITION_DURATION);
                            }
                        }
                        metricGraphWidget.lazyRender();
                    }
                })
                .finally(() => {
                    setIsRenderComplete(true);
                })
                ;
        }
        return () => {
            cancelled = true;
        };
    }, [layoutStatus, metricGraphWidget, metricGraphWidgetReady, metrics, selectedMetrics, selectedMetricsSource, selection, svg, trackSelection]);

    const onReady = React.useCallback(() => {
        setMetricGraphWidgetReady(true);
    }, []);
    // --- ---

    const graphButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "selPrev", title: nlsHPCC.PreviousSelection, iconProps: { iconName: "NavigateBack" },
            disabled: selection === undefined || selectedMetricsPtr < 1 || selectedMetricsPtr >= selection.length,
            onClick: () => {
                metricGraphWidget.centerOnItem(selection[selectedMetricsPtr - 1]);
                setSelectedMetricsPtr(selectedMetricsPtr - 1);
            }
        },
        {
            key: "selNext", title: nlsHPCC.NextSelection, iconProps: { iconName: "NavigateBackMirrored" },
            disabled: selection === undefined || selectedMetricsPtr < 0 || selectedMetricsPtr >= selection.length - 1,
            onClick: () => {
                metricGraphWidget.centerOnItem(selection[selectedMetricsPtr + 1]);
                setSelectedMetricsPtr(selectedMetricsPtr + 1);
            }
        }
    ], [metricGraphWidget, selection, selectedMetricsPtr]);

    const graphRightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "toSel", title: nlsHPCC.ZoomSelection,
            disabled: selection === undefined || selection.length <= 0,
            iconProps: { iconName: "FitPage" },
            canCheck: true,
            checked: trackSelection,
            onClick: () => {
                if (trackSelection) {
                    setTrackSelection(false);
                } else {
                    setTrackSelection(true);
                    metricGraphWidget.zoomToSelection();
                }
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "tofit", title: nlsHPCC.ZoomAll, iconProps: { iconName: "ScaleVolume" },
            onClick: () => metricGraphWidget.zoomToFit()
        }, {
            key: "tofitWidth", title: nlsHPCC.ZoomWidth, iconProps: { iconName: "FitWidth" },
            onClick: () => metricGraphWidget.zoomToWidth()
        }, {
            key: "100%", title: nlsHPCC.Zoom100Pct, iconProps: { iconName: "ZoomToFit" },
            onClick: () => metricGraphWidget.zoomToScale(1)
        }, {
            key: "plus", title: nlsHPCC.ZoomPlus, iconProps: { iconName: "ZoomIn" },
            onClick: () => metricGraphWidget.zoomPlus()
        }, {
            key: "minus", title: nlsHPCC.ZoomMinus, iconProps: { iconName: "ZoomOut" },
            onClick: () => metricGraphWidget.zoomMinus()
        },
    ], [metricGraphWidget, selection, setTrackSelection, trackSelection]);

    const spinnerLabel: string = React.useMemo((): string => {
        if (status === FetchStatus.STARTED) {
            return nlsHPCC.FetchingData;
        } else if (status === FetchStatus.COMPLETE && selectedMetrics.length === 0) {
            // fetch completed but an error occurred or no data available?
            return "";
        } else if (!isLayoutComplete(layoutStatus)) {
            switch (layoutStatus) {
                case LayoutStatus.LONG_RUNNING:
                    return nlsHPCC.PerformingLayoutLongRunning;
                case LayoutStatus.STARTED:
                default:
                    return nlsHPCC.PerformingLayout;
            }
        } else if (layoutStatus === LayoutStatus.FAILED) {
            return nlsHPCC.PerformingLayoutFailed;
        } else if (!isRenderComplete) {
            return nlsHPCC.RenderSVG;
        }
        return "";
    }, [status, selectedMetrics.length, layoutStatus, isRenderComplete]);

    const breadcrumbs = React.useMemo<BreadcrumbInfo[]>(() => {
        return lineage.map(item => {
            return {
                id: item.name,
                label: `${item.id} (${metricGraph.childCount(item.name)})`,
                props: {
                    icon: lineageSelectionScope?.name === item.name ? <SelectedLineageIcon /> : <LineageIcon />
                }
            };
        });
    }, [lineage, lineageSelectionScope, metricGraph]);

    return <HolyGrail
        header={<>
            <CommandBar items={graphButtons} farItems={graphRightButtons} />
            <OverflowBreadcrumb breadcrumbs={breadcrumbs} selected={lineageSelectionScope?.name} onSelect={(item => onLineageSelectionChange(item.id, false))} />
        </>}
        main={<>
            <AutosizeComponent hidden={!spinnerLabel}>
                {
                    layoutStatus === LayoutStatus.FAILED ?
                        <Label style={{ ...typographyStyles.subtitle2 }}>{spinnerLabel}</Label> :
                        <Spinner size="extra-large" label={spinnerLabel} labelPosition="below"></Spinner>
                }
            </AutosizeComponent>
            <AutosizeComponent hidden={!!spinnerLabel || selection?.length > 0}>
                <Label style={{ ...typographyStyles.subtitle2 }}>{nlsHPCC.NoContentPleaseSelectItem}</Label>
            </AutosizeComponent>
            <AutosizeHpccJSComponent widget={metricGraphWidget} onReady={onReady}>
            </AutosizeHpccJSComponent>
        </>
        }
    />;
};
