import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import nlsHPCC from "src/nlsHPCC";
import { Memory } from "src/store/Memory";
import { useConfirm } from "../hooks/confirm";
import { useGrid } from "../hooks/grid";
import * as Utility from "src/Utility";
import * as WsESDLConfig from "src/WsESDLConfig";
import { AddBindingForm } from "./forms/AddBinding";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { selector, tree } from "./DojoGrid";
import { pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/DESDLBindings.tsx");

const defaultUIState = {
    hasSelection: false,
};

class TreeStore extends Memory {
    mayHaveChildren(item) {
        return item.children;
    }

    getChildren(parent, options) {
        return this.query({ __hpcc_parentName: parent.__hpcc_id }, options);
    }
}

interface ESDLBindingProps {
}

export const DESDLBindings: React.FunctionComponent<ESDLBindingProps> = ({
}) => {
    const [showAddBinding, setShowAddBinding] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const store = useConst(() => new Observable(new TreeStore("__hpcc_id", { Name: true })));
    const { Grid, selection, refreshTable } = useGrid({
        store,
        query: { __hpcc_parentName: null },
        sort: { attribute: "__hpcc_id", descending: false },
        filename: "esdlBindings",
        columns: {
            col1: selector({
                width: 30,
                selectorType: "checkbox",
                disabled: function (item) {
                    if (item.type === "binding") {
                        return false;
                    }
                    return true;
                },
                sortable: false,
                unhidable: true
            }),
            Name: tree({
                formatter: function (_name, row) {
                    let img = "";
                    let name = _name;
                    if (row.type === "port") {
                        img = Utility.getImageHTML("machine.png") + nlsHPCC.Port + ":";
                    } else if (row.type === "binding") {
                        img = Utility.getImageHTML("sync.png");
                        name = `<a href="#/desdl/bindings/${name}">${name}</a>`;
                    }
                    return img + "&nbsp;" + name;
                },
                collapseOnRefresh: false,
                label: nlsHPCC.Process,
                width: 240,
                sortable: false,
                unhidable: true
            }),
            PublishBy: {
                label: nlsHPCC.PublishedBy,
                sortable: false,
                width: 160
            },
            CreatedTime: {
                label: nlsHPCC.CreatedTime,
                sortable: false,
                width: 160
            },
            LastEditBy: {
                label: nlsHPCC.LastEditedBy,
                sortable: false,
                width: 160
            },
            LastEditTime: {
                label: nlsHPCC.LastEditTime,
                sortable: false,
                width: 160
            }
        }
    });

    const refreshGrid = React.useCallback(() => {
        WsESDLConfig.ListESDLBindings({
            request: {
                ListESDLBindingsRequest: true
            }
        })
            .then(({ ListESDLBindingsResponse }) => {
                const rows = [];
                const processes = ListESDLBindingsResponse?.EspProcesses?.EspProcess ?? [];
                processes.forEach((row, idx) => {
                    row = {
                        ...row,
                        __hpcc_parentName: null,
                        __hpcc_id: row.Name + idx,
                        children: row.Ports ? true : false,
                        type: "service"
                    };
                    rows.push(row);
                    if (row?.Ports) {
                        row?.Ports?.Port.forEach((Port, portIdx) => {
                            rows.push({
                                __hpcc_parentName: row.Name + idx,
                                __hpcc_id: row.Name + Port.Value + portIdx,
                                Name: Port.Value,
                                children: Port ? true : false,
                                type: "port"
                            });
                            Port?.Bindings?.Binding.forEach((Binding, bindingIdx) => {
                                rows.push({
                                    ESPProcessName: row.Name,
                                    Port: Port.Value,
                                    __hpcc_parentName: row.Name + Port.Value + portIdx,
                                    __hpcc_id: Binding.Id + bindingIdx,
                                    Name: Binding.Id,
                                    PublishBy: Binding.History.PublishBy,
                                    CreatedTime: Binding.History.CreatedTime,
                                    LastEditBy: Binding.History.LastEditBy,
                                    LastEditTime: Binding.History.LastEditTime,
                                    children: false,
                                    type: "binding"
                                });
                            });
                        });
                    }
                });
                store.setData(rows);
                refreshTable();
            })
            .catch(err => logger.error(err))
            ;
    }, [refreshTable, store]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.YouAreAboutToDeleteBinding + "\n\n" + selection.map(binding => binding.Name).join("\n"),
        onSubmit: React.useCallback(() => {
            const requests = [];
            selection.forEach(binding => {
                requests.push(
                    WsESDLConfig.DeleteESDLBinding({
                        request: {
                            Id: binding.Name
                        }
                    })
                );
            });
            Promise
                .all(requests)
                .then(() => {
                    refreshGrid();
                })
                .catch(err => logger.error(err))
                ;
        }, [refreshGrid, selection])
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshGrid()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`/desdl/bindings/${selection[0].Name}`);
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/desdl/bindings/${selection[i].Name}`, "_blank");
                    }
                }
            }
        },
        {
            key: "add", text: nlsHPCC.AddBinding,
            onClick: () => setShowAddBinding(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "delete", text: nlsHPCC.DeleteBinding, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteConfirm(true),
        }
    ], [refreshGrid, selection, setShowDeleteConfirm, uiState]);

    React.useEffect(() => {
        refreshGrid();
    }, [store, refreshGrid]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        state.hasSelection = selection.length > 0;

        setUIState(state);
    }, [selection]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} />}
            main={<Grid />}
        />
        <AddBindingForm showForm={showAddBinding} setShowForm={setShowAddBinding} refreshGrid={refreshGrid} minWidth={420} />
        <DeleteConfirm />
    </>;
};