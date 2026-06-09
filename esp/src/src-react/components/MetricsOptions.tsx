import * as React from "react";
import { SelectionMode, Selection } from "./controls/Grid";
import { useConst, useForceUpdate } from "@fluentui/react-hooks";
import { Button, Checkbox, Dropdown, Field, Input, Option, SelectTabData, SelectTabEvent, Tab, TabList, Textarea, makeStyles } from "@fluentui/react-components";
import { BookmarkAddRegular, DeleteRegular, RenameRegular } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";
import { MetricsView, clone, useMetricMeta, useMetricsViews } from "../hooks/metrics";
import { MessageBox } from "../layouts/MessageBox";
import { JSONSourceEditor, SourceEditor } from "./SourceEditor";
import { FluentColumns, FluentGrid, useFluentStoreState } from "./controls/Grid";
import { DockPanelLayout } from "../layouts/DockPanel";

const width = 640;
const innerHeight = 400;

const useStyles = makeStyles({
    metricsPanel: {
        overflow: "auto"
    },
    sqlPanel: {},
    graphPanel: {
        overflow: "auto"
    },
    layoutPanel: {},
    allPanel: {}
});

interface GridOptionsProps {
    label: string;
    strArray: string[];
    strSelection: string[];
    setSelection: (_: string[]) => void;
}

const GridOptions: React.FunctionComponent<GridOptionsProps> = ({
    label,
    strArray,
    strSelection,
    setSelection
}) => {
    const [data, setData] = React.useState<{ id: string }[]>([]);
    const { setTotal, refreshTable } = useFluentStoreState({});

    const setSelectionRef = React.useRef(setSelection);
    const strSelectionRef = React.useRef(strSelection);

    const isSyncingRef = React.useRef(false);

    React.useEffect(() => {
        setSelectionRef.current = setSelection;
    }, [setSelection]);

    React.useEffect(() => {
        strSelectionRef.current = strSelection;
    }, [strSelection]);

    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: {
                width: 27,
                selectorType: "checkbox"
            },
            id: {
                label,
                width: 200,
                sortable: true
            }
        };
    }, [label]);

    React.useEffect(() => {
        setData(strArray.map(str => ({ id: str, key: str })));
    }, [strArray]);

    const handlerRef = React.useRef<Selection | null>(null);
    const selectionHandler = useConst(() => {
        handlerRef.current = new Selection({
            getKey: (item: { id: string; key: string }) => item.key,
            onSelectionChanged: () => {
                if (!isSyncingRef.current) {
                    setSelectionRef.current(handlerRef.current!.getSelection().map((item: any) => item.id));
                }
            },
            onItemsChanged: () => {
                isSyncingRef.current = true;
                handlerRef.current!.setAllSelected(false);
                for (const str of strSelectionRef.current) {
                    handlerRef.current!.setKeySelected(str, true, false);
                }
                isSyncingRef.current = false;
            }
        });
        return handlerRef.current;
    });

    React.useEffect(() => {
        isSyncingRef.current = true;
        selectionHandler.setChangeEvents(false);
        selectionHandler.setAllSelected(false);
        for (const str of strSelection) {
            selectionHandler.setKeySelected(str, true, false);
        }
        selectionHandler.setChangeEvents(true);
        isSyncingRef.current = false;
    }, [selectionHandler, strSelection]);

    return <div style={{ position: "relative", height: 400 }}>
        <FluentGrid
            data={data}
            primaryID={"id"}
            columns={columns}
            selectionMode={SelectionMode.multiple}
            setSelection={selectionHandler}
            setTotal={setTotal}
            refresh={refreshTable}
            height={`${innerHeight}px`}
        ></FluentGrid>
    </div>;
};

interface AddLabelProps {
    show: boolean;
    setShow: (_: boolean) => void;
    defaultLabel?: string;
    title?: string;
    onOk: (label: string) => void
}

export const AddLabel: React.FunctionComponent<AddLabelProps> = ({
    show,
    setShow,
    defaultLabel = "",
    title = nlsHPCC.Add,
    onOk
}) => {
    const [label, setLabel] = React.useState(defaultLabel);

    React.useEffect(() => {
        if (show) {
            setLabel(defaultLabel);
        }
    }, [defaultLabel, show]);

    const onChangeAddLabel = React.useCallback((_: unknown, data: { value: string }) => {
        setLabel(data.value ?? "");
    }, [],);

    return <MessageBox title={title} show={show} setShow={setShow} minWidth={width}
        footer={<>
            <Button appearance="primary" disabled={!label} onClick={() => {
                onOk(label);
                setShow(false);
            }}
            >{nlsHPCC.OK}</Button>
            <Button
                onClick={() => {
                    setLabel("");
                    setShow(false);
                }}
            >{nlsHPCC.Cancel}</Button>
        </>}>
        <Field label={nlsHPCC.Label}>
            <Input value={label} onChange={onChangeAddLabel} />
        </Field>
    </MessageBox>;

};

interface MetricsOptionsProps {
    show: boolean;
    setShow: (_: boolean) => void;
    logicalGraph: boolean;
}

export const MetricsOptions: React.FunctionComponent<MetricsOptionsProps> = ({
    show,
    setShow,
    logicalGraph
}) => {
    const [globalScopeTypes, globalProperties] = useMetricMeta();
    const { viewIds, viewId, setViewId, view, addView, deleteView, renameView, isDefaultView, updateView, resetView, save } = useMetricsViews(logicalGraph);
    const [dirtyView, setDirtyView] = React.useState<MetricsView>(clone(view));
    const [showAdd, setShowAdd] = React.useState(false);
    const [showRename, setShowRename] = React.useState(false);
    const [showDeleteConfirm, setShowDeleteConfirm] = React.useState(false);
    const [selectedTab, setSelectedTab] = React.useState("metrics");
    const forceRefresh = useForceUpdate();

    React.useEffect(() => {
        setDirtyView(clone(view));
    }, [view]);

    const closeOptions = React.useCallback(() => {
        setShow(false);
    }, [setShow]);

    const onDropdownChange = React.useCallback((event, data) => {
        updateView({ ...view, ...dirtyView });
        setViewId(data.optionValue as string, true);
        save();
    }, [dirtyView, save, setViewId, updateView, view]);

    const onAddLabel = React.useCallback((label: string) => {
        if (label) {
            addView(label, { ...view, ...dirtyView });
        }
    }, [addView, dirtyView, view]);

    const onRenameLabel = React.useCallback((newLabel: string) => {
        if (newLabel && newLabel !== viewId) {
            renameView(viewId, newLabel);
        }
    }, [renameView, viewId]);

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        setSelectedTab(data.value as string);
    }, []);

    const styles = useStyles();

    return <>
        <MessageBox title={nlsHPCC.Options} show={show && !showAdd && !showRename && !showDeleteConfirm} setShow={setShow} minWidth={width}
            footer={<>
                <Button
                    appearance="primary"
                    onClick={() => {
                        updateView(dirtyView);
                        save();
                        closeOptions();
                    }}
                >{nlsHPCC.OK}</Button>
                <Button
                    onClick={() => {
                        setDirtyView(clone(view));
                        closeOptions();
                    }}
                >{nlsHPCC.Cancel}</Button>
                <Button
                    onClick={() => {
                        resetView(true);
                    }}
                >{nlsHPCC.Reset}</Button>
            </>}>
            <>
                <div style={{ display: "flex", flexDirection: "row" }}>
                    <div style={{ flexGrow: 1 }}>
                        <Dropdown value={viewId} selectedOptions={[viewId]} onOptionSelect={onDropdownChange}>
                            {viewIds.map(id => <Option key={id} value={id}>{id}</Option>)}
                        </Dropdown>
                    </div>
                    <Button appearance="subtle" icon={<BookmarkAddRegular />} title={nlsHPCC.Add} onClick={() => {
                        setShowAdd(true);
                    }} />
                    <Button appearance="subtle" icon={<RenameRegular />} title={nlsHPCC.Rename} disabled={isDefaultView(viewId)} onClick={() => {
                        setShowRename(true);
                    }} />
                    <Button appearance="subtle" icon={<DeleteRegular />} title={nlsHPCC.Delete} disabled={isDefaultView(viewId)} onClick={() => {
                        setShowDeleteConfirm(true);
                    }} />
                </div>
                <TabList selectedValue={selectedTab} onTabSelect={onTabSelect} size="medium">
                    <Tab value="metrics">{nlsHPCC.Metrics}</Tab>
                    <Tab value="sql">{nlsHPCC.SQL}</Tab>
                    <Tab value="graph">{nlsHPCC.Graph}</Tab>
                    <Tab value="layout">{nlsHPCC.Layout}</Tab>
                    <Tab value="all">{nlsHPCC.All}</Tab>
                </TabList>
                {selectedTab === "metrics" &&
                    <div className={styles.metricsPanel} style={{ height: innerHeight }}>
                        <div style={{ display: "flex", flexDirection: "row" }}>
                            <div style={{ flexGrow: 1 }}>
                                <GridOptions
                                    label={nlsHPCC.ScopeTypes}
                                    strArray={globalScopeTypes}
                                    strSelection={dirtyView.scopeTypes}
                                    setSelection={scopeTypes => {
                                        setDirtyView(prev => ({ ...prev, scopeTypes: [...scopeTypes] }));
                                    }}
                                ></GridOptions>
                            </div>
                            <div style={{ flexGrow: 1 }}>
                                <GridOptions
                                    label={nlsHPCC.ScopeColumns}
                                    strArray={globalProperties}
                                    strSelection={dirtyView.properties}
                                    setSelection={properties => {
                                        setDirtyView(prev => ({ ...prev, properties: [...properties] }));
                                    }}
                                ></GridOptions>
                            </div>
                        </div>
                    </div>
                }
                {selectedTab === "sql" &&
                    <div className={styles.sqlPanel} style={{ height: innerHeight }}>
                        <SourceEditor mode="sql" text={dirtyView.sql} toolbar={false} onTextChange={sql => {
                            setDirtyView(prev => ({ ...prev, sql }));
                        }} />
                    </div>
                }
                {selectedTab === "graph" &&
                    <div className={styles.graphPanel} style={{ height: innerHeight }}>
                        <Checkbox label={nlsHPCC.IgnoreGlobalStoreOutEdges} checked={dirtyView.ignoreGlobalStoreOutEdges} onChange={(_, data) => {
                            setDirtyView(prev => ({ ...prev, ignoreGlobalStoreOutEdges: !!data.checked }));
                        }} />
                        <Field label={nlsHPCC.SubgraphLabel}>
                            <Textarea value={dirtyView.subgraphTpl} onChange={(_, data) => {
                                setDirtyView(prev => ({ ...prev, subgraphTpl: data.value }));
                            }} />
                        </Field>
                        <Field label={nlsHPCC.ActivityLabel}>
                            <Textarea value={dirtyView.activityTpl} onChange={(_, data) => {
                                setDirtyView(prev => ({ ...prev, activityTpl: data.value }));
                            }} />
                        </Field>
                        <Field label={nlsHPCC.EdgeLabel}>
                            <Textarea value={dirtyView.edgeTpl} onChange={(_, data) => {
                                setDirtyView(prev => ({ ...prev, edgeTpl: data.value }));
                            }} />
                        </Field>
                    </div>
                }
                {selectedTab === "layout" &&
                    <div className={styles.layoutPanel} style={{ height: innerHeight }}>
                        <Checkbox label={nlsHPCC.Timeline} checked={dirtyView.showTimeline} onChange={(_, data) => {
                            setDirtyView(prev => ({ ...prev, showTimeline: !!data.checked }));
                        }} />
                        <JSONSourceEditor json={dirtyView.layout} toolbar={false} onChange={obj => {
                            if (obj) {
                                setDirtyView(prev => ({ ...prev, layout: obj as DockPanelLayout }));
                            }
                        }} />
                    </div>
                }
                {selectedTab === "all" &&
                    <div className={styles.allPanel} style={{ height: innerHeight }}>
                        <JSONSourceEditor json={dirtyView} toolbar={false} onChange={(obj?: MetricsView) => {
                            if (obj) {
                                setDirtyView(obj);
                                forceRefresh();
                            }
                        }} />
                    </div>
                }
            </>
        </MessageBox>
        <AddLabel show={showAdd} setShow={setShowAdd} defaultLabel={`${viewId} copy`} onOk={onAddLabel} />
        <AddLabel show={showRename} setShow={setShowRename} defaultLabel={viewId} title={nlsHPCC.Rename} onOk={onRenameLabel} />
        <MessageBox title={nlsHPCC.Delete} show={showDeleteConfirm} setShow={setShowDeleteConfirm} minWidth={width}
            footer={<>
                <Button appearance="primary" onClick={() => {
                    deleteView(viewId);
                    setShowDeleteConfirm(false);
                }}>{nlsHPCC.Delete}</Button>
                <Button onClick={() => {
                    setShowDeleteConfirm(false);
                }}>{nlsHPCC.Cancel}</Button>
            </>}>
            {nlsHPCC.ConfirmRemoval}
        </MessageBox>
    </>;
};