import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { Label, Spinner } from "@fluentui/react-components";
import { typographyStyles } from "@fluentui/react-theme";
import { useConst } from "@fluentui/react-hooks";
import { bundleIcon, Folder20Filled, Folder20Regular, FolderOpen20Filled, FolderOpen20Regular } from "@fluentui/react-icons";
import { IScope } from "@hpcc-js/comms";
import nlsHPCC from "src/nlsHPCC";
import { FetchStatus, MetricsView, useMetricsGraphLayout } from "../hooks/metrics";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeComponent, AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { isLayoutComplete, LayoutStatus, MetricGraph, MetricGraphWidget } from "../util/metricGraph";
import { ShortVerticalDivider } from "./Common";
import { BreadcrumbInfo, OverflowBreadcrumb } from "./controls/OverflowBreadcrumb";

const LineageIcon = bundleIcon(Folder20Filled, Folder20Regular);
const SelectedLineageIcon = bundleIcon(FolderOpen20Filled, FolderOpen20Regular);

export interface MetricGraphData {
    metricGraph: MetricGraph;
    selectedMetrics: IScope[];
    lineage: IScope[];
    dot: string;
    svg: string;
    layoutStatus: LayoutStatus;
}

export function useMetricsGraphData(metrics: IScope[], view: MetricsView, lineageSelection?: string, selection?: string[]): MetricGraphData {
    const [selectedMetrics, setSelectedMetrics] = React.useState<IScope[]>([]);
    const [dot, setDot] = React.useState<string>("");
    const { svg, layoutStatus } = useMetricsGraphLayout(dot);
    const [lineage, setLineage] = React.useState<IScope[]>([]);

    const metricGraph = useConst(() => new MetricGraph());

    const updateSelectedMetrics = React.useCallback(() => {
        if (!selection?.length) {
            setSelectedMetrics([]);
            return;
        }

        const selectionSet = new Set(selection);
        setSelectedMetrics(metrics.filter(item => selectionSet.has(item.id)));
    }, [metrics, selection]);

    const updateSvg = React.useCallback((lineageSelection?: string) => {
        const dot = metricGraph.graphTpl(lineageSelection ? [lineageSelection] : [], view);
        setDot(dot);
    }, [metricGraph, view]);

    const updateLineage = React.useCallback((selection?: IScope[]) => {
        const newLineage: IScope[] = [];

        if (!selection?.length) {
            setLineage(newLineage);
            return;
        }

        let minLen = Number.MAX_SAFE_INTEGER;
        const lineages = selection.map(item => {
            const retVal = metricGraph.lineage(item);
            minLen = Math.min(minLen, retVal.length);
            return retVal;
        });

        if (lineages.length && minLen > 0) {
            for (let i = 0; i < minLen; ++i) {
                const item = lineages[0][i];
                if (lineages.every(lineage => lineage[i] === item)) {
                    if (item.id && item.type !== "child" && metricGraph.isSubgraph(item) && !metricGraph.isVertex(item)) {
                        newLineage.push(item);
                    }
                } else {
                    break;
                }
            }
        }

        setLineage(newLineage);
    }, [metricGraph]);

    React.useEffect(() => {
        metricGraph.load(metrics);
    }, [metricGraph, metrics]);

    React.useEffect(() => {
        updateSelectedMetrics();
    }, [updateSelectedMetrics]);

    React.useEffect(() => {
        updateLineage(selectedMetrics);
    }, [selectedMetrics, updateLineage]);

    React.useEffect(() => {
        if (metrics?.length > 0) {
            updateSvg(lineageSelection);
        } else {
            setDot("");
        }
    }, [updateSvg, metrics, lineageSelection]);

    return { metricGraph, selectedMetrics, lineage, dot, svg, layoutStatus };
}

export interface MetricsGraphProps {
    metricGraphData: MetricGraphData;
    lineageSelection?: string;
    selection?: string[];
    selectedMetricsSource: string;
    status: FetchStatus;
    onLineageSelectionChange: (selection?: string) => void;
    onSelectionChange: (selection?: string[]) => void;
}

export const MetricsGraph: React.FunctionComponent<MetricsGraphProps> = ({
    metricGraphData: { metricGraph, selectedMetrics, lineage, svg, layoutStatus },
    lineageSelection,
    selection,
    selectedMetricsSource,
    status,
    onLineageSelectionChange,
    onSelectionChange
}) => {
    const [selectedMetricsPtr, setSelectedMetricsPtr] = React.useState<number>(-1);
    const [trackSelection, setTrackSelection] = React.useState<boolean>(true);
    const [isRenderComplete, setIsRenderComplete] = React.useState<boolean>(false);
    const [metricGraphWidgetReady, setMetricGraphWidgetReady] = React.useState<boolean>(false);

    // Data ---
    React.useEffect(() => {
        if (isLayoutComplete(layoutStatus) && lineage.find(item => item.name === lineageSelection) === undefined) {
            onLineageSelectionChange(lineage[lineage.length - 1]?.name);
        }
    }, [layoutStatus, lineage, lineageSelection, onLineageSelectionChange]);

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
        if (metricGraphWidgetReady) {
            const sameSVG = metricGraphWidget.svg() === svg;
            setIsRenderComplete(sameSVG);
            metricGraphWidget
                .svg(svg)
                .renderPromise()
                .then(() => {
                    if (!cancelled) {
                        const newSel = selectedMetrics.filter(m => selection?.indexOf(m.id) >= 0).map(m => m.name).filter(sel => !!sel);
                        metricGraphWidget
                            .selection(newSel)
                            ;
                        if (trackSelection && selectedMetricsSource !== "metricGraphWidget") {
                            if (newSel.length) {
                                if (sameSVG) {
                                    metricGraphWidget.centerOnSelection();
                                } else {
                                    metricGraphWidget.zoomToSelection(0);
                                }
                            } else {
                                metricGraphWidget.zoomToFit(0);
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
    }, [metricGraphWidget, metricGraphWidgetReady, selectedMetrics, selectedMetricsSource, selection, svg, trackSelection]);

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
    ], [metricGraphWidget, selection, trackSelection]);

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
                    icon: lineageSelection === item.name ? <SelectedLineageIcon /> : <LineageIcon />
                }
            };
        });
    }, [lineage, lineageSelection, metricGraph]);

    return <HolyGrail
        header={<>
            <CommandBar items={graphButtons} farItems={graphRightButtons} />
            <OverflowBreadcrumb breadcrumbs={breadcrumbs} selected={lineageSelection} onSelect={(item => onLineageSelectionChange(item.id))} />
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
