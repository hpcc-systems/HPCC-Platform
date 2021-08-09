import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { DockPanel } from "@hpcc-js/phosphor";
import { Table } from "@hpcc-js/dgrid";
import nlsHPCC from "src/nlsHPCC";
import { WUTimelinePatched } from "src/Timings";
import { useMetricsOptions, useWorkunitMetrics } from "../hooks/metrics";
import { useFavorite } from "../hooks/favorite";
import { useUserStore } from "../hooks/store";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { IScope, MetricGraph, MetricGraphWidget } from "../util/metricGraph";
import { ShortVerticalDivider } from "./Common";
import { MetricsOptions } from "./MetricsOptions";

const defaultUIState = {
    hasSelection: false
};

interface MetricsProps {
    wuid: string;
    filter?: object;
}

const emptyFilter = {};

export const Metrics: React.FunctionComponent<MetricsProps> = ({
    wuid,
    filter = emptyFilter
}) => {
    const [_uiState, _setUIState] = React.useState({ ...defaultUIState });
    const [timelineFilter, setTimelineFilter] = React.useState("");
    const [metrics, _columns, _activities, _properties, _measures, _scopeTypes] = useWorkunitMetrics(wuid);
    const [showMetricOptions, setShowMetricOptions] = React.useState(false);
    const [options] = useMetricsOptions();
    const [isFavorite, addFavorite, removeFavorite] = useFavorite(window.location.hash);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => { }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "options", text: nlsHPCC.Options, iconProps: { iconName: "Settings" },
            onClick: () => {
                setShowMetricOptions(true);
            }
        }
    ], []);

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "star", iconProps: { iconName: isFavorite ? "FavoriteStarFill" : "FavoriteStar" },
            onClick: () => {
                if (isFavorite) {
                    removeFavorite();
                } else {
                    addFavorite();
                }
            }
        },
    ], [addFavorite, isFavorite, removeFavorite]);

    //  Timeline ---
    const timeline = useConst(() => new WUTimelinePatched()
        .maxZoom(Number.MAX_SAFE_INTEGER)
        .baseUrl("")
        .request({
            ScopeFilter: {
                MaxDepth: 3,
                ScopeTypes: []
            },
            NestedFilter: {
                Depth: 0,
                ScopeTypes: []
            },
            PropertiesToReturn: {
                AllProperties: false,
                AllStatistics: true,
                AllHints: false,
                Properties: ["WhenStarted", "TimeElapsed"]
            },
            ScopeOptions: {
                IncludeId: true,
                IncludeScope: true,
                IncludeScopeType: true
            },
            PropertyOptions: {
                IncludeName: true,
                IncludeRawValue: true,
                IncludeFormatted: true,
                IncludeMeasure: true,
                IncludeCreator: true,
                IncludeCreatorType: false
            }
        })
        .on("click", (row, col, sel) => {
            setTimelineFilter(sel ? row[7].ScopeName : "");
        })
    );

    React.useEffect(() => {
        timeline.wuid(wuid);
    }, [timeline, wuid]);

    //  Scopes Table  ---
    const hasFilter = Object.keys(filter).length > 0;

    const scopesTable = useConst(() => new Table()
        .id("scopesTable")
        .multiSelect(true)
        .columns(["##", nlsHPCC.Type, nlsHPCC.Scope, ...options.properties])
        .sortable(true)
        .on("click", (row, col, sel) => {
            const rows = scopesTable.selection().map(row => row.__lparam);
            if (rows.length) {
                updatePropsTable(rows);
                updatePropsTable2(rows);
                updateMetricGraph(rows);
            }
        })
    );

    React.useEffect(() => {
        scopesTable
            .columns(["##", nlsHPCC.Type, nlsHPCC.Scope, ...options.properties])
            .data(metrics.filter(row => {
                return (timelineFilter === "" || row.name?.indexOf(timelineFilter) === 0) &&
                    (options.scopeTypes.indexOf(row.type) >= 0);
            }).map((row, idx) => {
                row.__hpcc_id = row.name;
                return [idx, row.type, row.name, ...options.properties.map(p => row[p] !== undefined ? row[p] : ""), row];
            }))
            .lazyRender()
            ;
    }, [hasFilter, metrics, scopesTable, timelineFilter, filter, options.properties, options.scopeTypes]);

    //  Graph  ---
    const metricGraph = useConst(() => new MetricGraph());
    const metricGraphWidget = useConst(() => new MetricGraphWidget()
        .id("metricGraph")
        .zoomToFitLimit(1)
        .on("selectionChanged", () => {
            const items = metricGraphWidget.selection().map(id => {
                return metricGraph.item(id);
            });
            // const item = graph.item(row.id);
            updatePropsTable(items);
            updatePropsTable2(items);
            const tableItems = scopesTable.data().filter(tableRow => items.indexOf(tableRow[tableRow.length - 1]) >= 0);
            scopesTable.selection(tableItems);
        })
    );

    React.useEffect(() => {
        metricGraph.load(metrics);
    }, [metrics, metricGraph]);

    const updateMetricGraph = React.useCallback((selection: IScope[]) => {
        if (selection.length) {
            metricGraphWidget
                .dot(metricGraph.graphTpl(selection, options))
                .resize()
                .render(() => {
                    metricGraphWidget.selection(selection.map(s => s.name));
                })
                ;
        }
    }, [metricGraph, metricGraphWidget, options]);

    //  Props Table  ---
    const propsTable = useConst(() => new Table()
        .id("propsTable")
        .columns([nlsHPCC.Property, nlsHPCC.Value])
        .columnWidth("none")
    );

    const updatePropsTable = React.useCallback(items => {
        const props = [];
        items.forEach((item, idx) => {
            for (const key in item) {
                if (key.indexOf("__") !== 0) {
                    props.push([key, item[key]]);
                }
            }
            if (idx < items.length - 1) {
                props.push(["------------------------------", "------------------------------"]);
            }
        });
        propsTable
            ?.data(props)
            ?.lazyRender()
            ;
    }, [propsTable]);

    const propsTable2 = useConst(() => new Table()
        .id("propsTable2")
        .columns([nlsHPCC.Property, nlsHPCC.Value])
        .columnWidth("auto")
    );

    const updatePropsTable2 = React.useCallback(items => {
        const columns = [];
        const props = [];
        items.forEach(item => {
            for (const key in item) {
                if (key.indexOf("__") !== 0 && columns.indexOf(key) < 0) {
                    columns.push(key);
                }
            }
        });
        items.forEach(item => {
            const row = [];
            columns.forEach(column => {
                row.push(item[column]);
            });
            props.push(row);
        });
        propsTable2
            ?.columns(columns)
            ?.data(props)
            ?.lazyRender()
            ;
    }, [propsTable2]);

    //  DockPanel ---
    const dockPanel = useConst(() => {
        return new DockPanel()
            .addWidget(scopesTable, nlsHPCC.Metrics)
            .addWidget(metricGraphWidget, nlsHPCC.Graph, "split-right", scopesTable)
            .addWidget(propsTable, nlsHPCC.Properties, "split-bottom", scopesTable)
            .addWidget(propsTable2, nlsHPCC.CrossTab, "tab-after", propsTable)
            ;
    });

    const [layout, setLayout] = useUserStore("metrics-layout", "");
    const [ready, setReady] = React.useState(false);

    React.useEffect(() => {
        if (layout !== undefined) {
            try {
                const obj = JSON.parse(layout);
                dockPanel.layout(obj);
            } catch (e) {

            }
            setReady(true);
        }
    }, [dockPanel, layout]);

    React.useEffect(() => {
        return () => {
            setLayout(JSON.stringify(dockPanel?.layout()));
        };
    }, [dockPanel, setLayout]);

    return <HolyGrail
        header={<>
            <CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />
            <AutosizeHpccJSComponent widget={timeline} fixedHeight={"160px"} padding={4} />
        </>}
        main={
            <>
                {ready && <AutosizeHpccJSComponent widget={dockPanel} padding={4} debounce={false} />}
                <MetricsOptions show={showMetricOptions} setShow={setShowMetricOptions} layout={dockPanel?.layout()} />
            </>}
    />;
};
