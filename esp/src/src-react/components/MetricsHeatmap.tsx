import * as React from "react";
import { makeStyles, tokens, Dropdown, Option, Field } from "@fluentui/react-components";
import { hierarchy, treemap } from "d3-hierarchy";
import { scaleLinear } from "d3-scale";
import { Palette } from "@hpcc-js/common";
import nlsHPCC from "src/nlsHPCC";
import { IScope } from "@hpcc-js/comms";
import { useUserTheme } from "../hooks/theme";
import { brandVariants } from "../themes";
import { MetricGraph } from "../util/metricGraph";
import { MetricsTooltip } from "./MetricsGraphTooltip";
import { SizeMe, Size } from "../layouts/SizeMe";
import { CommandBar, ICommandBarItemProps } from "./CommandBarV9";

// light: 160 (lightest) → 10 (darkest) for low→high heat; dark: 10 (darkest) → 110 (brightest) for low→high
const HEAT_RAMP_LIGHT = [160, 160, 140, 120, 90, 60, 60] as const;
const HEAT_RAMP_DARK = [10, 10, 30, 50, 80, 110, 110] as const;

const useStyles = makeStyles({
    root: {
        display: "flex",
        flexDirection: "column",
        height: "100%",
        width: "100%",
    },
    field: {
        width: "300px",
        maxWidth: "100%",
        minWidth: "0",
    },
    dropdown: {
        width: "100%",
    },
    content: {
        flex: 1,
        overflow: "hidden",
        position: "relative",
    },
    svg: {
        width: "100%",
        height: "100%",
    },
    cell: {
        cursor: "pointer",
        outlineOffset: "-2px",
        transition: "all 100ms",
        "&:hover": {
            opacity: 0.8,
        },
        "&:focus": {
            outline: `2px solid ${tokens.colorBrandStroke1}`,
            outlineOffset: "2px",
        },
    },
    emptyState: {
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100%",
        fontSize: "14px",
        color: tokens.colorNeutralForeground2,
    },
});

export interface MetricsHeatmapProps {
    scopes: IScope[];
    columns: Record<string, MetricColumn>;
    preferredViewProperties?: string[];
    selectedActivityIds?: string[];
    metricGraph?: MetricGraph;
    onActivitySelected?: (ids: string[]) => void;
}

interface MetricColumn {
    Measure?: unknown;
    measure?: unknown;
}

interface MetricChoice {
    key: string;
    value: string;
    label: string;
    hitCount: number;
}

interface HierarchyNode {
    name: string;
    id?: string;
    scopeName?: string;
    activity?: IScope;
    value: number;
    metricValue?: number;
    formattedValue: string;
    isLeaf: boolean;
    children?: HierarchyNode[];
}

type TreemapNode = ReturnType<typeof hierarchy<HierarchyNode>> & {
    x0: number;
    x1: number;
    y0: number;
    y1: number;
};

const NUMERIC_MEASURES = ["s", "ns", "sz", "cnt", "cost", "node", "skw", "cpu", "ppm", "cy", "en"];
const LEGACY_METRIC_CANDIDATES = ["TimeMaxLocalExecute", "TimeLocalExecute"];
const NON_METRIC_COLUMNS = new Set(["Kind"]);
const MIN_LAYOUT_WEIGHT = 0.001;

function normalizeMetricValue(value: unknown): number | undefined {
    if (value === null || value === undefined || (typeof value === "string" && value.trim() === "")) {
        return undefined;
    }
    const normalized = Number(value);
    return Number.isFinite(normalized) ? normalized : undefined;
}

function isActivitySelected(activity: HierarchyNode, selectedIds: readonly string[]): boolean {
    if (selectedIds.includes(activity.id ?? "") || selectedIds.includes(activity.scopeName ?? "")) {
        return true;
    }
    return activity.scopeName?.split(":").some(scopePart => selectedIds.includes(scopePart)) ?? false;
}

export const MetricsHeatmap: React.FunctionComponent<MetricsHeatmapProps> = ({
    scopes,
    columns,
    preferredViewProperties = [],
    selectedActivityIds = [],
    metricGraph,
    onActivitySelected = () => { },
}) => {
    const styles = useStyles();
    const { themeV9, isDark } = useUserTheme();
    const clipPathPrefix = React.useId().replace(/:/g, "");
    const [selectedMetric, setSelectedMetric] = React.useState<string>("");
    const [tooltip, setTooltip] = React.useState<{ x: number; y: number; activity: IScope } | null>(null);
    const [metrics, setMetrics] = React.useState<MetricChoice[]>([]);
    const selectedMetricLabel = metrics.find(metric => metric.key === selectedMetric)?.label ?? selectedMetric;
    const palette = React.useMemo(() => {
        const shades = isDark ? HEAT_RAMP_DARK : HEAT_RAMP_LIGHT;
        const colors = shades.map(s => brandVariants[s]);
        const name = isDark ? "HeatRampDark" : "HeatRamp";
        Palette.rainbow(name, colors);
        return Palette.rainbow(name);
    }, [isDark]);

    const showTooltip = React.useCallback((event: React.MouseEvent<SVGRectElement>, activity: IScope | undefined) => {
        if (!activity) return;
        setTooltip({ x: event.clientX, y: event.clientY, activity });
    }, []);

    const hideTimerRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);

    const clearHideTimer = React.useCallback(() => {
        if (hideTimerRef.current !== null) {
            clearTimeout(hideTimerRef.current);
            hideTimerRef.current = null;
        }
    }, []);

    const scheduleHide = React.useCallback((delay = 800) => {
        clearHideTimer();
        hideTimerRef.current = setTimeout(() => setTooltip(null), delay);
    }, [clearHideTimer]);

    React.useEffect(() => () => { clearHideTimer(); }, [clearHideTimer]);

    // Filter to activities only
    const activities = React.useMemo(() => {
        return scopes.filter(scope => scope.type === "activity");
    }, [scopes]);

    // Build metric choices from numeric columns
    const buildMetricChoices = React.useCallback(() => {
        if (!columns || typeof columns !== "object") return [];

        const choices: MetricChoice[] = [];
        const columnIds = new Set(Object.keys(columns));
        activities.forEach(activity => {
            Object.keys(activity).forEach(key => {
                if (!key.startsWith("__") && !NON_METRIC_COLUMNS.has(key)) {
                    columnIds.add(key);
                }
            });
        });

        for (const colId of columnIds) {
            if (NON_METRIC_COLUMNS.has(colId)) continue;
            const col = columns[colId];

            // Prefer the server's measure metadata, but tolerate it being absent on initial load.
            const measureValue = col?.Measure ?? col?.measure;
            const measure = typeof measureValue === "string" ? measureValue : "";
            const hitCount = activities.reduce((count, activity) =>
                normalizeMetricValue(activity[colId]) !== undefined ? count + 1 : count, 0);
            if (hitCount === 0 || (measure && !NUMERIC_MEASURES.includes(measure))) continue;

            choices.push({
                key: colId,
                value: colId,
                label: `${colId} (${hitCount})`,
                hitCount,
            });
        }

        return choices.sort((l, r) => r.hitCount - l.hitCount || l.label.localeCompare(r.label));
    }, [columns, activities]);

    // Determine initial metric selection
    const determineInitialMetric = React.useCallback((choices: MetricChoice[]) => {
        if (choices.length === 0) return "";

        // First: try preferred view properties
        for (const prop of preferredViewProperties) {
            if (choices.some(c => c.key === prop)) {
                return prop;
            }
        }

        // Second: try legacy candidates
        for (const candidate of LEGACY_METRIC_CANDIDATES) {
            if (choices.some(c => c.key === candidate)) {
                return candidate;
            }
        }

        // Third: use first available
        return choices[0]?.key || "";
    }, [preferredViewProperties]);

    // Build metrics and select initial
    React.useEffect(() => {
        const choices = buildMetricChoices();
        setMetrics(choices);

        if (selectedMetric && choices.some(c => c.key === selectedMetric)) {
            // Keep current selection if still valid
            return;
        }

        // Otherwise select initial
        const initial = determineInitialMetric(choices);
        setSelectedMetric(initial);
    }, [scopes, columns, buildMetricChoices, determineInitialMetric, selectedMetric]);

    // Build treemap data
    const treemapData = React.useMemo(() => {
        if (!selectedMetric || activities.length === 0) return null;

        const children = activities.map(activity => {
            const rawValue = activity[selectedMetric];
            const metricValue = normalizeMetricValue(rawValue);
            const formatted = activity.__formattedProps?.[selectedMetric]?.toString() ??
                (rawValue === null || rawValue === undefined ? nlsHPCC.NotAvailable : String(rawValue));
            const label = activity.Label || activity.id || activity.name || "unknown";

            return {
                name: label,
                id: activity.id || activity.name,
                scopeName: activity.name,
                activity,
                value: metricValue === undefined ? MIN_LAYOUT_WEIGHT : Math.max(metricValue, MIN_LAYOUT_WEIGHT),
                metricValue,
                formattedValue: formatted,
                isLeaf: true,
            };
        });

        const root: HierarchyNode = {
            name: "activities",
            value: 0,
            formattedValue: "",
            isLeaf: false,
            children,
        };

        return hierarchy(root, d => d.children)
            .sum(d => d.isLeaf ? d.value : 0) as TreemapNode;
    }, [selectedMetric, activities]);

    // Compute treemap layout
    const layout = React.useCallback((size: Size) => {
        if (!treemapData) {
            return {
                nodes: [] as TreemapNode[],
                colorScale: undefined as ((value: number) => string) | undefined,
            };
        }

        const tm = treemap<HierarchyNode>()
            .size([size.width, size.height])
            .paddingTop(0)
            .paddingRight(1)
            .paddingBottom(1)
            .paddingLeft(1);

        const laid = tm(treemapData) as TreemapNode;
        const nodes = laid.leaves() as TreemapNode[];

        const values = nodes
            .map(n => n.data?.metricValue)
            .filter((v): v is number => v !== undefined);
        const minValue = Math.min(...values, 0);
        const maxValue = Math.max(...values, 1);

        const valueScale = scaleLinear<number>()
            .domain([minValue, maxValue])
            .range([0, 1]);

        return { nodes, colorScale: (v: number) => palette(valueScale(v), 0, 1) };
    }, [palette, treemapData]);

    const commandBarItems = React.useMemo((): ICommandBarItemProps[] => [{
        key: "metric",
        onRender: () => <Field className={styles.field}>
            <Dropdown
                className={styles.dropdown}
                value={selectedMetricLabel}
                selectedOptions={selectedMetric ? [selectedMetric] : []}
                onOptionSelect={(_event, data) => {
                    setSelectedMetric(data.optionValue ?? "");
                }}
            >
                {metrics.map(metric => <Option key={metric.key} value={metric.value}>
                    {metric.label}
                </Option>)}
            </Dropdown>
        </Field>
    }], [metrics, selectedMetric, selectedMetricLabel, styles.dropdown, styles.field]);

    return (
        <div className={styles.root}>
            <CommandBar items={commandBarItems} />

            {metrics.length === 0 ? (
                <div className={styles.emptyState}>
                    {activities.length === 0
                        ? nlsHPCC.NoActivities
                        : nlsHPCC.NoNumericMetrics}
                </div>
            ) : (
                <div className={styles.content}>
                    <SizeMe>
                        {({ size }) => {
                            const { nodes, colorScale } = layout(size);

                            return (
                                <svg className={styles.svg}>
                                    <defs>
                                        {nodes.map((node, idx) => {
                                            const x0 = node.x0 || 0;
                                            const y0 = node.y0 || 0;
                                            return <clipPath id={`${clipPathPrefix}-${idx}`} key={idx}>
                                                <rect
                                                    x={x0}
                                                    y={y0}
                                                    width={(node.x1 || 0) - x0}
                                                    height={(node.y1 || 0) - y0}
                                                />
                                            </clipPath>;
                                        })}
                                    </defs>
                                    {nodes
                                        .map((node, idx) => [node, idx] as const)
                                        // selected cells last so their border renders on top
                                        .sort(([a], [b]) => (isActivitySelected(a.data, selectedActivityIds) ? 1 : 0) - (isActivitySelected(b.data, selectedActivityIds) ? 1 : 0))
                                        .map(([node, idx]) => {
                                            const x0 = node.x0 || 0;
                                            const y0 = node.y0 || 0;
                                            const width = (node.x1 || 0) - x0;
                                            const height = (node.y1 || 0) - y0;
                                            const metricValue = node.data?.metricValue;
                                            const formatted = node.data?.formattedValue ?? nlsHPCC.NotAvailable;
                                            const id = node.data?.id ?? `activity-${idx}`;
                                            const isSelected = isActivitySelected(node.data, selectedActivityIds);
                                            const color =
                                                colorScale && metricValue !== undefined
                                                    ? colorScale(metricValue)
                                                    : themeV9.colorNeutralBackground3;
                                            const rotateLabel = height > width;
                                            const centerX = x0 + width / 2;
                                            const centerY = y0 + height / 2;

                                            return (
                                                <g key={idx}>
                                                    <rect
                                                        className={styles.cell}
                                                        x={x0}
                                                        y={y0}
                                                        width={width}
                                                        height={height}
                                                        fill={color}
                                                        stroke={
                                                            isSelected
                                                                ? themeV9.colorStatusWarningBorder2
                                                                : themeV9.colorBrandStroke2
                                                        }
                                                        strokeWidth={
                                                            isSelected ? 2 : 1
                                                        }
                                                        tabIndex={0}
                                                        role="button"
                                                        aria-label={`${node.data?.name}: ${formatted}`}
                                                        onMouseEnter={event => { clearHideTimer(); showTooltip(event, node.data.activity); }}
                                                        onMouseLeave={() => scheduleHide()}
                                                        onClick={() => {
                                                            onActivitySelected([id]);
                                                        }}
                                                        onKeyDown={e => {
                                                            if (e.key === "Enter" || e.key === " ") {
                                                                e.preventDefault();
                                                                onActivitySelected([id]);
                                                            }
                                                        }}
                                                    />
                                                    {Math.max(width, height) > 30 &&
                                                        Math.min(width, height) > 20 && (
                                                            <g clipPath={`url(#${clipPathPrefix}-${idx})`}>
                                                                <text
                                                                    x={centerX}
                                                                    y={centerY}
                                                                    textAnchor="middle"
                                                                    dominantBaseline="middle"
                                                                    fontSize="12"
                                                                    fill={Palette.textColor(color)}
                                                                    transform={rotateLabel ? `rotate(-90 ${centerX} ${centerY})` : undefined}
                                                                    pointerEvents="none"
                                                                >
                                                                    {node.data
                                                                        ?.name}
                                                                </text>
                                                            </g>
                                                        )}
                                                </g>
                                            );
                                        })}
                                </svg>
                            );
                        }}
                    </SizeMe>

                    {tooltip && <MetricsTooltip
                        item={tooltip.activity}
                        metricGraph={metricGraph}
                        anchor={{ left: tooltip.x, top: tooltip.y }}
                        onMouseEnter={clearHideTimer}
                        onMouseLeave={() => scheduleHide(0)}
                    />}
                </div>
            )}
        </div>
    );
};
