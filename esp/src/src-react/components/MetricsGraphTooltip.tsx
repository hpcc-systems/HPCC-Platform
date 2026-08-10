import * as React from "react";
import { createPortal } from "react-dom";
import { Tooltip, makeStyles } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";
import { METRIC_GRAPH_HOVER_EVENT, MetricGraphHoverDetail, MetricGraphWidget, MetricGraph } from "../util/metricGraph";

const TOOLTIP_ROW_ORDER = ["id", "source(s)", "target(s)"];

function isOrderedMetricPrefix(key: string): boolean {
    const lower = key.toLowerCase();
    return lower.startsWith("time") || lower.startsWith("size") || lower.startsWith("cost") || lower.startsWith("num");
}

function metricPrefixRank(key: string): number {
    const lower = key.toLowerCase();
    if (lower.startsWith("time")) {
        return 0;
    }
    if (lower.startsWith("size")) {
        return 1;
    }
    if (lower.startsWith("cost")) {
        return 2;
    }
    if (lower.startsWith("num")) {
        return 3;
    }
    return 4;
}

interface TooltipPropertyRow {
    key: string;
    label?: string;
    value: React.ReactNode;
}

interface MetricException {
    Severity?: string;
    Source?: string;
    Code?: string | number;
    Message?: string;
    FileName?: string;
    LineNo?: number;
    Column?: number;
}

function renderTooltipValue(value: React.ReactNode): React.ReactNode {
    if (typeof value !== "string") {
        return value;
    }
    if (!value.includes("|")) {
        return value;
    }
    return value.split("|").map((part, idx) => (
        <div key={idx}>{part.trim()}</div>
    ));
}

function metricLabel(v): string {
    return v.Label ? `${v.id} (${v.Label})` : `${v.id}`;
}

function propertyDisplayValue(row): string {
    const value = row?.Value;
    if (value !== undefined && value !== null && value !== "") {
        return `${value}`;
    }
    const parts: string[] = [];
    if (row?.Avg !== undefined) parts.push(`Avg=${row.Avg}`);
    if (row?.Min !== undefined) parts.push(`Min=${row.Min}`);
    if (row?.Max !== undefined) parts.push(`Max=${row.Max}`);
    if (row?.Delta !== undefined) parts.push(`Delta=${row.Delta}`);
    if (row?.StdDev !== undefined) parts.push(`StdDev=${row.StdDev}`);
    if (row?.SkewMin !== undefined) parts.push(`SkewMin=${row.SkewMin}`);
    if (row?.SkewMax !== undefined) parts.push(`SkewMax=${row.SkewMax}`);
    return parts.join(" | ");
}

function exceptionSummary(ex: MetricException): string {
    const parts: string[] = [];
    if (ex.Source) {
        parts.push(ex.Source);
    }
    if (ex.Code !== undefined && ex.Code !== null && `${ex.Code}`.length > 0) {
        parts.push(`${ex.Code}`);
    }
    if (ex.FileName) {
        const lineCol = ex.LineNo !== undefined && ex.Column !== undefined ? `:${ex.LineNo}:${ex.Column}` : "";
        parts.push(`${ex.FileName}${lineCol}`);
    }
    if (ex.Message) {
        parts.push(ex.Message);
    }
    return parts.filter(part => !!part).join(" | ");
}

function appendExceptionRows(rows: TooltipPropertyRow[], item: { __exceptions?: MetricException[] }): void {
    if (!item.__exceptions?.length) {
        return;
    }

    const errors = item.__exceptions.filter(ex => ex.Severity === "Error");
    const warnings = item.__exceptions.filter(ex => ex.Severity === "Warning");

    if (errors.length) {
        rows.push({
            key: "errors",
            label: nlsHPCC.Errors,
            value: errors.map((ex, idx) => <div key={`error-${idx}`}>{exceptionSummary(ex)}</div>)
        });
    }
    if (warnings.length) {
        rows.push({
            key: "warnings",
            label: nlsHPCC.Warnings,
            value: warnings.map((ex, idx) => <div key={`warning-${idx}`}>{exceptionSummary(ex)}</div>)
        });
    }
}

function metricUrl(metricName: string): string | undefined {
    if (typeof window === "undefined" || !window.location?.hash) {
        return undefined;
    }
    const [path, query] = window.location.hash.split("?");
    if (!path || !path.includes("/metrics/")) {
        return undefined;
    }

    const segments = path.split("/");
    if (segments.length < 2) {
        return undefined;
    }
    const splitName = metricName.split(":");
    const lastNode = splitName.pop();
    if (!lastNode) {
        return undefined;
    }
    const parentName = splitName.join(":");

    segments[segments.length - 2] = encodeURIComponent(parentName);
    segments[segments.length - 1] = encodeURIComponent(lastNode);

    return query ? `${segments.join("/")}?${query}` : segments.join("/");
}

function renderMetricLinks(activities): React.ReactNode {
    return activities.map((activity, idx) => {
        const href = metricUrl(activity.name);
        return <div key={`${activity.name}-${idx}`}>
            {
                href ?
                    <a href={href}>{metricLabel(activity)}</a> :
                    metricLabel(activity)
            }
        </div>;
    });
}

function buildTooltipRows(item, metricGraph: MetricGraph): TooltipPropertyRow[] {
    if (!item) {
        return [];
    }

    const rows: TooltipPropertyRow[] = [];

    rows.push({ key: "id", value: item.id ?? "" });

    for (const groupedKey in item.__groupedProps ?? {}) {
        const row = item.__groupedProps[groupedKey];
        const rowKey = `${row.Key ?? groupedKey}`;
        if (isOrderedMetricPrefix(rowKey)) {
            rows.push({ key: rowKey, value: propertyDisplayValue(row) });
        }
    }

    appendExceptionRows(rows, item);

    if (item.type === "edge") {
        const sourceActivity = metricGraph.activityByID(item.IdSource);
        const targetActivity = metricGraph.activityByID(item.IdTarget);
        if (sourceActivity) {
            rows.push({ key: "source(s)", value: renderMetricLinks([sourceActivity]) });
        }
        if (targetActivity) {
            rows.push({ key: "target(s)", value: renderMetricLinks([targetActivity]) });
        }
    } else if (metricGraph.subgraphExists(item.name)) {
        const inActivities = metricGraph.inSubgraphActivities(item);
        const outActivities = metricGraph.outSubgraphActivities(item);
        if (inActivities.length) {
            rows.push({ key: "source(s)", value: renderMetricLinks(inActivities) });
        }
        if (outActivities.length) {
            rows.push({ key: "target(s)", value: renderMetricLinks(outActivities) });
        }
    } else {
        const inActivities = metricGraph.inActivities(item);
        const outActivities = metricGraph.outActivities(item);
        if (inActivities.length) {
            rows.push({ key: "source(s)", value: renderMetricLinks(inActivities) });
        }
        if (outActivities.length) {
            rows.push({ key: "target(s)", value: renderMetricLinks(outActivities) });
        }
    }

    rows.sort((l, r) => {
        const lKey = l.key.toLowerCase();
        const rKey = r.key.toLowerCase();
        const lIdx = TOOLTIP_ROW_ORDER.indexOf(lKey);
        const rIdx = TOOLTIP_ROW_ORDER.indexOf(rKey);
        if (lIdx >= 0 && rIdx >= 0) {
            return lIdx <= rIdx ? -1 : 1;
        }
        if (lIdx >= 0) {
            return -1;
        }
        if (rIdx >= 0) {
            return 1;
        }
        const lPrefixRank = metricPrefixRank(lKey);
        const rPrefixRank = metricPrefixRank(rKey);
        if (lPrefixRank !== rPrefixRank) {
            return lPrefixRank - rPrefixRank;
        }
        if (lPrefixRank < 3) {
            return l.key.localeCompare(r.key);
        }
        return l.key.localeCompare(r.key);
    });

    return rows;
}

const useStyles = makeStyles({
    hoverAnchor: {
        position: "fixed",
        width: "1px",
        height: "1px",
        pointerEvents: "none",
        zIndex: 10000
    },
    tooltipContent: {
        display: "grid",
        rowGap: "8px",
        maxHeight: "320px",
        maxWidth: "520px",
        overflowY: "auto",
        overflowX: "hidden"
    },
    tooltipId: {
        fontWeight: "700",
        fontSize: "13px",
        lineHeight: "18px",
        wordBreak: "break-word"
    },
    tooltipLabel: {
        fontWeight: "400",
        fontSize: "12px",
        lineHeight: "16px",
        opacity: 0.95,
        wordBreak: "break-word"
    },
    tooltipBlock: {
        display: "grid",
        rowGap: "2px"
    },
    tooltipTitle: {
        fontWeight: "700",
        fontSize: "11px",
        opacity: 0.8
    },
    tooltipValue: {
        wordBreak: "break-word",
        "& a": {
            color: "var(--colorBrandForeground1)",
            textDecoration: "none"
        }
    }
});


export interface MetricsGraphTooltipProps {
    metricGraph?: MetricGraph;
    metricGraphWidget: MetricGraphWidget;
    selection?: string[];
    showTooltips: boolean;
}

export const MetricsGraphTooltip: React.FunctionComponent<MetricsGraphTooltipProps> = ({ metricGraph, metricGraphWidget, selection, showTooltips }) => {
    const styles = useStyles();
    const [hoverDetail, setHoverDetail] = React.useState<MetricGraphHoverDetail | undefined>(undefined);
    const hoverHideTimer = React.useRef<number | undefined>(undefined);
    const isTooltipHovered = React.useRef<boolean>(false);
    const isTooltipTransitioning = React.useRef<boolean>(false);
    const isMouseDown = React.useRef<boolean>(false);
    const pendingHoverDetail = React.useRef<MetricGraphHoverDetail | undefined>(undefined);

    const clearHoverHideTimer = React.useCallback(() => {
        if (hoverHideTimer.current !== undefined) {
            window.clearTimeout(hoverHideTimer.current);
            hoverHideTimer.current = undefined;
        }
    }, []);

    const scheduleHoverHide = React.useCallback((delay = 120) => {
        clearHoverHideTimer();
        hoverHideTimer.current = window.setTimeout(() => {
            if (!isTooltipHovered.current) {
                setHoverDetail(pendingHoverDetail.current);
            }
            isTooltipTransitioning.current = false;
            pendingHoverDetail.current = undefined;
            hoverHideTimer.current = undefined;
        }, delay);
    }, [clearHoverHideTimer]);

    const hoverItem = React.useMemo(() => {
        if (!hoverDetail?.id || !metricGraph) {
            return undefined;
        }
        return metricGraph.item(hoverDetail.id);
    }, [hoverDetail?.id, metricGraph]);

    const hoverRows = React.useMemo(() => {
        if (!metricGraph) {
            return [];
        }
        return buildTooltipRows(hoverItem, metricGraph);
    }, [hoverItem, metricGraph]);

    const hoverContent = React.useMemo(() => {
        if (!hoverDetail?.id) {
            return null;
        }
        const idRow = hoverRows.find(row => row.key.toLowerCase() === "id");
        const labelRow = hoverItem?.Label ? `${hoverItem.Label}` : "";
        const detailRows = hoverRows.filter(row => row.key.toLowerCase() !== "id");

        return <div
            className={styles.tooltipContent}
            data-metric-tooltip="true"
            onMouseEnter={() => {
                isTooltipHovered.current = true;
                isTooltipTransitioning.current = false;
                pendingHoverDetail.current = undefined;
                clearHoverHideTimer();
            }}
            onMouseLeave={() => {
                isTooltipHovered.current = false;
                isTooltipTransitioning.current = false;
                pendingHoverDetail.current = undefined;
                scheduleHoverHide(0);
            }}
        >
            <div className={styles.tooltipId}>{renderTooltipValue(idRow?.value ?? hoverDetail.id)}</div>
            {labelRow ? <div className={styles.tooltipLabel}>{renderTooltipValue(labelRow)}</div> : null}
            {detailRows.map((row, idx) =>
                <div key={`${row.key}-${idx}`} className={styles.tooltipBlock}>
                    <div className={styles.tooltipTitle}>{row.label ?? row.key}</div>
                    <div className={styles.tooltipValue}>{renderTooltipValue(row.value)}</div>
                </div>
            )}
        </div>;
    }, [clearHoverHideTimer, hoverDetail?.id, hoverItem?.Label, hoverRows, scheduleHoverHide, styles.tooltipBlock, styles.tooltipContent, styles.tooltipId, styles.tooltipLabel, styles.tooltipTitle, styles.tooltipValue]);

    const hoverAnchor = React.useMemo(() => {
        if (!hoverDetail) {
            return undefined;
        }
        if (hoverDetail.clientX !== undefined && hoverDetail.clientY !== undefined) {
            return {
                left: hoverDetail.clientX,
                top: hoverDetail.clientY
            };
        }
        if (hoverDetail.anchorLeft !== undefined && hoverDetail.anchorTop !== undefined) {
            return {
                left: hoverDetail.anchorLeft + (hoverDetail.anchorWidth ?? 0) / 2,
                top: hoverDetail.anchorTop + (hoverDetail.anchorHeight ?? 0) / 2
            };
        }
        return undefined;
    }, [hoverDetail]);

    React.useEffect(() => {
        const onMouseDown = () => { isMouseDown.current = true; };
        const onMouseUp = () => { isMouseDown.current = false; };
        window.addEventListener("mousedown", onMouseDown, { capture: true });
        window.addEventListener("mouseup", onMouseUp, { capture: true });
        return () => {
            window.removeEventListener("mousedown", onMouseDown, { capture: true });
            window.removeEventListener("mouseup", onMouseUp, { capture: true });
        };
    }, []);

    React.useEffect(() => {
        const onMetricGraphHover = (evt: Event) => {
            const customEvent = evt as CustomEvent<MetricGraphHoverDetail>;
            const detail = customEvent.detail;
            if (!detail || detail.widgetID !== metricGraphWidget.id()) {
                return;
            }
            if (detail.id) {
                if (isMouseDown.current || isTooltipTransitioning.current) {
                    pendingHoverDetail.current = isMouseDown.current ? undefined : detail;
                    return;
                }
                clearHoverHideTimer();
                setHoverDetail(detail);
            } else if (detail.relatedTarget instanceof Element && detail.relatedTarget.closest("[data-metric-tooltip='true']")) {
                clearHoverHideTimer();
            } else {
                isTooltipTransitioning.current = true;
                pendingHoverDetail.current = undefined;
                scheduleHoverHide(800);
            }
        };
        document.addEventListener(METRIC_GRAPH_HOVER_EVENT, onMetricGraphHover);
        return () => {
            clearHoverHideTimer();
            document.removeEventListener(METRIC_GRAPH_HOVER_EVENT, onMetricGraphHover);
        };
    }, [clearHoverHideTimer, metricGraphWidget, scheduleHoverHide]);

    React.useEffect(() => {
        if (!hoverDetail?.id) {
            return;
        }
        const dismiss = (evt: KeyboardEvent | MouseEvent) => {
            if (evt instanceof KeyboardEvent) {
                if (evt.key !== "Escape") return;
                evt.preventDefault();
            } else if (evt.target instanceof Element && evt.target.closest("[data-metric-tooltip='true']")) {
                return;
            }
            isTooltipHovered.current = false;
            isTooltipTransitioning.current = false;
            pendingHoverDetail.current = undefined;
            clearHoverHideTimer();
            setHoverDetail(undefined);
        };
        window.addEventListener("keydown", dismiss);
        window.addEventListener("mousedown", dismiss, { capture: true });
        return () => {
            window.removeEventListener("keydown", dismiss);
            window.removeEventListener("mousedown", dismiss, { capture: true });
        };
    }, [clearHoverHideTimer, hoverDetail?.id]);

    React.useEffect(() => {
        setHoverDetail(undefined);
    }, [selection]);

    if (!showTooltips || hoverDetail?.id === undefined || hoverAnchor === undefined || typeof document === "undefined" || !metricGraph) {
        return null;
    }

    return createPortal(
        <Tooltip content={hoverContent} relationship="label" visible withArrow positioning="above-start">
            <div className={styles.hoverAnchor} style={{ left: hoverAnchor.left, top: hoverAnchor.top }} />
        </Tooltip>,
        document.body
    );
};
