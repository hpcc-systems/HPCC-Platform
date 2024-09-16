import * as React from "react";
import { DefaultButton, Dropdown, PrimaryButton, Checkbox, Pivot, PivotItem, TextField, IDropdownOption, Stack, SelectionMode, Selection } from "@fluentui/react";
import { useConst, useForceUpdate } from "@fluentui/react-hooks";
import { Button } from "@fluentui/react-components";
import { BookmarkAddRegular, DeleteRegular } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";
import { MetricsView, clone, useMetricMeta, useMetricsViews } from "../hooks/metrics";
import { MessageBox } from "../layouts/MessageBox";
import { JSONSourceEditor, SourceEditor } from "./SourceEditor";
import { FluentColumns, FluentGrid, useFluentStoreState } from "./controls/Grid";
import { DockPanelLayout } from "../layouts/DockPanel";

const width = 640;
const innerHeight = 400;

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
        setData(strArray.map(str => ({ id: str })));
    }, [strArray]);

    const selectionHandler = useConst(() => {
        return new Selection({
            getKey: (item: { id: string }) => item.id,
            onSelectionChanged: () => setSelection(selectionHandler.getSelection().map(item => item.id)),
            onItemsChanged: () => {
                selectionHandler.setAllSelected(false);
                for (const str of strSelection) {
                    selectionHandler.setKeySelected(str, true, false);
                }
            }
        });
    });

    React.useEffect(() => {
        selectionHandler.setAllSelected(false);
        for (const str of strSelection) {
            selectionHandler.setKeySelected(str, true, false);
        }
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
    onOk: (label: string) => void
}

export const AddLabel: React.FunctionComponent<AddLabelProps> = ({
    show,
    setShow,
    onOk
}) => {
    const [label, setLabel] = React.useState("");
    const onChangeAddLabel = React.useCallback((event: React.FormEvent<HTMLInputElement | HTMLTextAreaElement>, newValue?: string) => {
        setLabel(newValue || "");
    }, [],);

    return <MessageBox title={nlsHPCC.Add} show={show} setShow={setShow} minWidth={width}
        footer={<>
            <PrimaryButton text={nlsHPCC.OK} disabled={!label} onClick={() => {
                onOk(label);
                setShow(false);
            }}
            />
            <DefaultButton
                text={nlsHPCC.Cancel}
                onClick={() => {
                    setLabel("");
                    setShow(false);
                }}
            />
        </>}>
        <TextField label={nlsHPCC.Label} value={label}
            onChange={onChangeAddLabel}
        />
    </MessageBox>;

};

interface MetricsOptionsProps {
    show: boolean;
    setShow: (_: boolean) => void;
}

export const MetricsOptions: React.FunctionComponent<MetricsOptionsProps> = ({
    show,
    setShow,
}) => {
    const [globalScopeTypes, globalProperties] = useMetricMeta();
    const { viewIds, viewId, setViewId, view, addView, updateView } = useMetricsViews();
    const [dirtyView, setDirtyView] = React.useState<MetricsView>(clone(view));
    const [showAdd, setShowAdd] = React.useState(false);
    const forceRefresh = useForceUpdate();

    const options = React.useMemo(() => {
        return viewIds.map(id => ({ key: id, text: id }));
    }, [viewIds]);

    React.useEffect(() => {
        setDirtyView(clone(view));
    }, [view]);

    const closeOptions = React.useCallback(() => {
        setShow(false);
    }, [setShow]);

    const onDropdownChange = React.useCallback((event: React.FormEvent<HTMLDivElement>, item?: IDropdownOption) => {
        updateView({ ...view, ...dirtyView });
        setViewId(item.key as string, true);
    }, [dirtyView, setViewId, updateView, view]);

    const onAddLabel = React.useCallback((label: string) => {
        if (label) {
            addView(label, { ...view, ...dirtyView });
        }
    }, [addView, dirtyView, view]);

    console.log("dirtyView.scopeTypes", viewId, view.scopeTypes);

    return <>
        <MessageBox title={nlsHPCC.Options} show={show && !showAdd} setShow={setShow} minWidth={width}
            footer={<>
                <PrimaryButton
                    text={nlsHPCC.OK}
                    onClick={() => {
                        updateView(dirtyView);
                        closeOptions();
                    }}
                />
                <DefaultButton
                    text={nlsHPCC.Cancel}
                    onClick={() => {
                        setDirtyView(clone(view));
                        closeOptions();
                    }}
                />
                <DefaultButton
                    text={nlsHPCC.Reset}
                    onClick={() => {
                        setDirtyView(clone(view));
                        forceRefresh();
                    }}
                />
            </>}>
            <>
                <Stack horizontal>
                    <Stack.Item grow>
                        <Dropdown selectedKey={viewId} onChange={onDropdownChange} options={options} />
                    </Stack.Item>
                    <Button appearance="subtle" icon={<BookmarkAddRegular />} title={nlsHPCC.Add} disabled hidden onClick={() => {
                        setShowAdd(true);
                    }} />
                    <Button appearance="subtle" icon={<DeleteRegular />} title={nlsHPCC.Delete} disabled hidden onClick={() => {
                    }} />
                </Stack>
                <Pivot>
                    <PivotItem key="metrics" headerText={nlsHPCC.Metrics}>
                        <div style={{ height: innerHeight, overflow: "auto" }}>
                            <Stack horizontal>
                                <Stack.Item grow={1}>
                                    <GridOptions
                                        label={nlsHPCC.ScopeTypes}
                                        strArray={globalScopeTypes}
                                        strSelection={dirtyView.scopeTypes}
                                        setSelection={scopeTypes => {
                                            dirtyView.scopeTypes = [...scopeTypes];
                                        }}
                                    ></GridOptions>
                                </Stack.Item>
                                <Stack.Item grow={1}>
                                    <GridOptions
                                        label={nlsHPCC.ScopeColumns}
                                        strArray={globalProperties}
                                        strSelection={dirtyView.properties}
                                        setSelection={properties => {
                                            dirtyView.properties = [...properties];
                                        }}
                                    ></GridOptions>
                                </Stack.Item>
                            </Stack>
                        </div>
                    </PivotItem>
                    <PivotItem key="sql" headerText={nlsHPCC.SQL} >
                        <div style={{ height: innerHeight }}>
                            <SourceEditor mode="sql" text={dirtyView.sql} toolbar={false} onTextChange={sql => {
                                dirtyView.sql = sql;
                                forceRefresh();
                            }} />
                        </div>
                    </PivotItem>
                    <PivotItem key="graph" headerText={nlsHPCC.Graph}>
                        <div style={{ height: innerHeight, overflow: "auto" }}>
                            <Checkbox label={nlsHPCC.IgnoreGlobalStoreOutEdges} checked={dirtyView.ignoreGlobalStoreOutEdges} onChange={(ev, checked) => {
                                dirtyView.ignoreGlobalStoreOutEdges = checked;
                                forceRefresh();
                            }} />
                            <TextField label={nlsHPCC.SubgraphLabel} value={dirtyView.subgraphTpl} multiline autoAdjustHeight onChange={(evt, newValue) => {
                                dirtyView.subgraphTpl = newValue;
                                forceRefresh();
                            }} />
                            <TextField label={nlsHPCC.ActivityLabel} value={dirtyView.activityTpl} multiline autoAdjustHeight onChange={(evt, newValue) => {
                                dirtyView.activityTpl = newValue;
                                forceRefresh();
                            }} />
                            <TextField label={nlsHPCC.EdgeLabel} value={dirtyView.edgeTpl} multiline autoAdjustHeight onChange={(evt, newValue) => {
                                dirtyView.edgeTpl = newValue;
                                forceRefresh();
                            }} />
                        </div>
                    </PivotItem>
                    <PivotItem key="layout" headerText={nlsHPCC.Layout} >
                        <div style={{ height: innerHeight }}>
                            <Checkbox label={nlsHPCC.Timeline} checked={dirtyView.showTimeline} onChange={(ev, checked) => {
                                dirtyView.showTimeline = checked;
                                forceRefresh();
                            }} />
                            <JSONSourceEditor json={dirtyView.layout} toolbar={false} onChange={obj => {
                                if (obj) {
                                    dirtyView.layout = obj as DockPanelLayout;
                                    forceRefresh();
                                }
                            }} />
                        </div>
                    </PivotItem>
                    <PivotItem key="all" headerText={nlsHPCC.All} >
                        <div style={{ height: innerHeight }}>
                            <JSONSourceEditor json={dirtyView} toolbar={false} onChange={(obj?: MetricsView) => {
                                if (obj) {
                                    setDirtyView(obj);
                                    forceRefresh();
                                }
                            }} />
                        </div>
                    </PivotItem>
                </Pivot>
            </>
        </MessageBox>
        <AddLabel show={showAdd} setShow={setShowAdd} onOk={onAddLabel} />
    </>;
};