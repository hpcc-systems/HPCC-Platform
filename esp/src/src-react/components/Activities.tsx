import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as ESPActivity from "src/ESPActivity";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid, selector, tree } from "./DojoGrid";

class DelayedRefresh {
    _promises: Promise<any>[] = [];

    constructor(private _refresh: (clearSelection?) => void) {
    }

    push(promise: Promise<any>) {
        this._promises.push(promise);
    }

    refresh() {
        if (this._promises.length) {
            Promise.all(this._promises).then(() => {
                this._refresh();
            });
        }
    }
}

const defaultUIState = {
    clusterSelected: false,
    thorClusterSelected: false,
    wuSelected: false,
    clusterPausedSelected: false,
    clusterNotPausedSelected: false,
    clusterHasItems: false,
    wuCanHigh: false,
    wuCanNormal: false,
    wuCanLow: false,
    wuCanUp: false,
    wuCanDown: false
};

interface ActivitiesProps {
}

export const Activities: React.FunctionComponent<ActivitiesProps> = ({
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Command Bar  ---
    const wuPriority = (priority) => {
        const promises = new DelayedRefresh(refreshTable);
        selection.forEach((item, idx) => {
            const queue = item.get("ESPQueue");
            if (queue) {
                promises.push(queue.setPriority(item.Wuid, priority));
            }
        });
        promises.refresh();
    };

    const buttons: ICommandBarItemProps[] = [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => activity.refresh()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.wuSelected && !uiState.thorClusterSelected, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/clusters/${selection[0].ClusterName}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/clusters/${selection[i].ClusterName}`, "_blank");
                    }
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "pause", text: nlsHPCC.Pause, disabled: !uiState.clusterNotPausedSelected,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                selection.forEach((item, idx) => {
                    if (activity.isInstanceOfQueue(item)) {
                        promises.push(item.pause());
                    }
                });
                promises.refresh();
            }
        },
        {
            key: "resume", text: nlsHPCC.Resume, disabled: !uiState.clusterPausedSelected,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                selection.forEach((item, idx) => {
                    if (activity.isInstanceOfQueue(item)) {
                        promises.push(item.resume());
                    }
                });
                promises.refresh();
            }
        },
        {
            key: "clear", text: nlsHPCC.Clear, disabled: !uiState.clusterPausedSelected,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                selection.forEach((item, idx) => {
                    if (activity.isInstanceOfQueue(item)) {
                        promises.push(item.clear());
                    }
                });
                promises.refresh();
            }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "wu-pause", text: nlsHPCC.Pause, disabled: !uiState.wuSelected,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                selection.forEach((item, idx) => {
                    if (activity.isInstanceOfWorkunit(item)) {
                        promises.push(item.pause());
                    }
                });
                promises.refresh();
            }
        },
        {
            key: "wu-pause-now", text: nlsHPCC.PauseNow, disabled: !uiState.wuSelected,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                selection.forEach((item, idx) => {
                    if (activity.isInstanceOfWorkunit(item)) {
                        promises.push(item.pauseNow());
                    }
                });
                promises.refresh();
            }
        },
        {
            key: "wu-resume", text: nlsHPCC.Resume, disabled: !uiState.wuSelected,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                selection.forEach((item, idx) => {
                    if (activity.isInstanceOfWorkunit(item)) {
                        promises.push(item.resume());
                    }
                });
                promises.refresh();
            }
        },
        {
            key: "wu-abort", text: nlsHPCC.Abort, disabled: !uiState.wuSelected,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                selection.forEach((item, idx) => {
                    if (activity.isInstanceOfWorkunit(item)) {
                        promises.push(item.abort());
                    }
                });
                promises.refresh();
            }
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "wu-high", text: nlsHPCC.High, disabled: !uiState.wuCanHigh,
            onClick: () => wuPriority("high")
        },
        {
            key: "wu-normal", text: nlsHPCC.Normal, disabled: !uiState.wuCanNormal,
            onClick: () => wuPriority("normal")
        },
        {
            key: "wu-low", text: nlsHPCC.Low, disabled: !uiState.wuCanLow,
            onClick: () => wuPriority("low")
        },
        { key: "divider_5", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "wu-top", text: nlsHPCC.Top, disabled: !uiState.wuCanUp,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                for (let i = selection.length - 1; i >= 0; --i) {
                    const item = selection[i];
                    if (activity.isInstanceOfWorkunit(item)) {
                        const queue = item.get("ESPQueue");
                        if (queue) {
                            promises.push(queue.moveTop(item.Wuid));
                        }
                    }
                }
                promises.refresh();
            }
        },
        {
            key: "wu-up", text: nlsHPCC.Up, disabled: !uiState.wuCanUp,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                selection.forEach((item, idx) => {
                    if (activity.isInstanceOfWorkunit(item)) {
                        const queue = item.get("ESPQueue");
                        if (queue) {
                            promises.push(queue.moveUp(item.Wuid));
                        }
                    }
                });
                promises.refresh();
            }
        },
        {
            key: "wu-down", text: nlsHPCC.Down, disabled: !uiState.wuCanDown,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                for (let i = selection.length - 1; i >= 0; --i) {
                    const item = selection[i];
                    if (activity.isInstanceOfWorkunit(item)) {
                        const queue = item.get("ESPQueue");
                        if (queue) {
                            promises.push(queue.moveDown(item.Wuid));
                        }
                    }
                }
                promises.refresh();
            }
        },
        {
            key: "wu-bottom", text: nlsHPCC.Bottom, disabled: !uiState.wuCanDown,
            onClick: () => {
                const promises = new DelayedRefresh(refreshTable);
                selection.forEach((item, idx) => {
                    if (activity.isInstanceOfWorkunit(item)) {
                        const queue = item.get("ESPQueue");
                        if (queue) {
                            promises.push(queue.moveBottom(item.Wuid));
                        }
                    }
                });
                promises.refresh();
            }
        },
    ];

    const rightButtons: ICommandBarItemProps[] = [
        {
            key: "copy", text: nlsHPCC.CopyWUIDs, disabled: !uiState.wuSelected || !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
            onClick: () => {
                const wuids = selection.map(s => s.Wuid);
                navigator?.clipboard?.writeText(wuids.join("\n"));
            }
        },
        {
            key: "download", text: nlsHPCC.DownloadToCSV, disabled: !uiState.wuSelected, iconOnly: true, iconProps: { iconName: "Download" },
            onClick: () => {
                Utility.downloadToCSV(grid, selection.map(row => ([row.Protected, row.Wuid, row.Owner, row.Jobname, row.Cluster, row.RoxieCluster, row.State, row.TotalClusterTime])), "workunits.csv");
            }
        }
    ];

    //  Grid ---
    const activity = useConst(ESPActivity.Get());
    const gridParams = useConst({
        store: activity.getStore({}),
        query: {},
        columns: {
            col1: selector({
                width: 27,
                selectorType: "checkbox",
                sortable: false
            }),
            Priority: {
                renderHeaderCell: function (node) {
                    node.innerHTML = Utility.getImageHTML("priority.png", nlsHPCC.Priority);
                },
                width: 25,
                sortable: false,
                formatter: function (Priority) {
                    switch (Priority) {
                        case "high":
                            return Utility.getImageHTML("priority_high.png");
                        case "low":
                            return Utility.getImageHTML("priority_low.png");
                    }
                    return "";
                }
            },
            DisplayName: tree({
                label: nlsHPCC.TargetWuid,
                width: 300,
                sortable: true,
                shouldExpand: function (row, level, previouslyExpanded) {
                    if (level === 0) {
                        return previouslyExpanded === undefined ? true : previouslyExpanded;
                    }
                    return previouslyExpanded;
                },
                formatter: function (_name, row) {
                    const img = row.getStateImage();
                    if (activity.isInstanceOfQueue(row)) {
                        if (row.ClusterType === 3) {
                            return `<img src='${img}'/>&nbsp;<a href='#/clusters/${row.ClusterName}' class='dgrid-row-url'>${_name}</a>`;
                        } else {
                            return `<img src='${img}'/>&nbsp;${_name}`;
                        }
                    }
                    return `<img src='${img}'/>&nbsp;<a href='#/workunits/${row.Wuid}' class='dgrid-row-url'>${row.Wuid}</a>`;
                }
            }),
            GID: {
                label: nlsHPCC.Graph, width: 90, sortable: true,
                formatter: function (_gid, row) {
                    if (activity.isInstanceOfWorkunit(row)) {
                        if (row.GraphName) {
                            return `<a href='#/graphs/${row.GraphName}/${row.GID}' class='dgrid-row-url2'>${row.GraphName}-${row.GID}</a>`;
                        }
                    }
                    return "";
                }
            },
            State: {
                label: nlsHPCC.State,
                sortable: false,
                formatter: function (state, row) {
                    if (activity.isInstanceOfQueue(row)) {
                        return row.isNormal() ? "" : row.StatusDetails;
                    }
                    if (row.Duration) {
                        return state + " (" + row.Duration + ")";
                    } else if (row.Instance && !(state.indexOf && state.indexOf(row.Instance) !== -1)) {
                        return state + " [" + row.Instance + "]";
                    }
                    return state;
                }
            },
            Owner: { label: nlsHPCC.Owner, width: 90, sortable: false },
            Jobname: { label: nlsHPCC.JobName, sortable: false }
        },
        getSelected: function () {
            const retVal = [];
            for (const id in this.selection) {
                const item = activity.resolve(id);
                if (item) {
                    retVal.push(item);
                }
            }
            return retVal;
        }
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", {});
        if (clearSelection) {
            grid?.clearSelection();
        }
    };

    React.useEffect(() => {
        refreshTable();
        const handle = activity.watch("__hpcc_changedCount", function (item, oldValue, newValue) {
            refreshTable();
        });
        return () => handle.unwatch();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [grid]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        selection.forEach(item => {
            if (activity.isInstanceOfQueue(item)) {
                state.clusterSelected = true;
                if (item.isPaused()) {
                    state.clusterPausedSelected = true;
                } else {
                    state.clusterNotPausedSelected = true;
                }
                if (item.getChildCount()) {
                    state.clusterHasItems = true;
                }
                if (item.ClusterType === 3) {
                    state.thorClusterSelected = true;
                }
            } else if (activity.isInstanceOfWorkunit(item)) {
                state.wuSelected = true;
                const queue = item.get("ESPQueue");
                if (queue) {
                    if (queue.canChildMoveUp(item.__hpcc_id)) {
                        state.wuCanUp = true;
                    }
                    if (queue.canChildMoveDown(item.__hpcc_id)) {
                        state.wuCanDown = true;
                    }
                }
                if (item.get("Priority") !== "high") {
                    state.wuCanHigh = true;
                }
                if (item.get("Priority") !== "normal") {
                    state.wuCanNormal = true;
                }
                if (item.get("Priority") !== "low") {
                    state.wuCanLow = true;
                }
            }
        });
        setUIState(state);
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selection]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <DojoGrid type="Sel" store={gridParams.store} query={gridParams.query} columns={gridParams.columns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
