import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";
import { useConfirm } from "../hooks/confirm";
import { AddGroupForm } from "./forms/AddGroup";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/Groups.tsx");

const defaultUIState = {
    hasSelection: false,
};

interface GroupsProps {
    filter?: object;
}

export const Groups: React.FunctionComponent<GroupsProps> = ({
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [showAddGroup, setShowAddGroup] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const gridStore = useConst(WsAccess.CreateGroupsStore(null, true));
    const gridSort = useConst([{ attribute: "name", "descending": false }]);
    const gridQuery = useConst({});
    const gridColumns = useConst({
        check: selector({ width: 27, label: " " }, "checkbox"),
        name: {
            label: nlsHPCC.GroupName,
            formatter: function (_name, idx) {
                return `<a href="#/security/groups/${_name}">${_name}</a>`;
            }
        },
        groupOwner: { label: nlsHPCC.ManagedBy },
        groupDesc: { label: nlsHPCC.Description }
    });

    const refreshTable = React.useCallback((clearSelection = false) => {
        grid?.set("query", {});
        if (clearSelection) {
            grid?.clearSelection();
        }
    }, [grid]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedGroups + "\n" + selection.map(group => group.name).join("\n"),
        onSubmit: React.useCallback(() => {
            const request = { ActionType: "delete" };
            selection.forEach((item, idx) => {
                request["groupnames_i" + idx] = item.name;
            });

            WsAccess.GroupAction({
                request: request
            })
                .then((response) => {
                    refreshTable(true);
                })
                .catch(logger.error)
                ;
        }, [refreshTable, selection])
    });

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        if (selection.length > 0) {
            state.hasSelection = true;
        }

        setUIState(state);
    }, [selection]);

    const exportGroups = React.useCallback(() => {
        let groupnames = "";
        selection.forEach((item, idx) => {
            if (groupnames.length) {
                groupnames += "&";
            }
            groupnames += "groupnames_i" + idx + "=" + item.name;
        });
        window.open(`/ws_access/UserAccountExport?${groupnames}`, "_blank");
    }, [selection]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`/security/groups/${selection[0].name}`);
                } else {
                    selection.forEach(group => {
                        window.open(`#/security/groups/${group.name}`, "_blank");
                    });
                }
            }
        },
        {
            key: "add", text: nlsHPCC.Add,
            onClick: () => setShowAddGroup(true)
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "export", text: nlsHPCC.Export,
            onClick: () => exportGroups()
        },
    ], [exportGroups, refreshTable, selection, setShowDeleteConfirm, uiState]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} />}
        main={
            <>
                <DojoGrid
                    store={gridStore} query={gridQuery} sort={gridSort}
                    columns={gridColumns} setGrid={setGrid} setSelection={setSelection}
                />
                <AddGroupForm showForm={showAddGroup} setShowForm={setShowAddGroup} refreshGrid={refreshTable} />
                <DeleteConfirm />
            </>
        }
    />;

};
