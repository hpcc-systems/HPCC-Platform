import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/store/Memory";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { ShortVerticalDivider } from "./Common";
import { pushUrl } from "../util/history";
import { useConfirm } from "../hooks/confirm";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoGrid, selector } from "./DojoGrid";
import { UserAddGroupForm } from "./forms/UserAddGroup";

const logger = scopedLogger("src-react/components/UserGroups.tsx");

const defaultUIState = {
    hasSelection: false,
};

interface UserGroupsProps {
    username?: string;
}

export const UserGroups: React.FunctionComponent<UserGroupsProps> = ({
    username,
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [showAdd, setShowAdd] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("name")));
    const gridSort = useConst([{ attribute: "name", descending: false }]);
    const gridQuery = useConst({});
    const gridColumns = useConst({
        check: selector({ width: 27, label: " " }, "checkbox"),
        name: {
            label: nlsHPCC.GroupName,
            formatter: function (_name, idx) {
                _name = _name.replace(/[^-_a-zA-Z0-9\s]+/g, "");
                return `<a href="#/security/groups/${_name}">${_name}</a>`;
            }
        }
    });

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        if (selection.length > 0) {
            state.hasSelection = true;
        }

        setUIState(state);
    }, [selection]);

    const refreshTable = React.useCallback((clearSelection = false) => {
        WsAccess.UserEdit({
            request: { username: username }
        })
            .then(({ UserEditResponse }) => {
                if (UserEditResponse?.Groups) {
                    const groups = UserEditResponse?.Groups?.Group;
                    gridStore.setData(groups.map(group => {
                        return {
                            name: group.name
                        };
                    }));
                } else {
                    gridStore.setData([]);
                }

                grid?.set("query", gridQuery);
                if (clearSelection) {
                    grid?.clearSelection();
                }
            })
            .catch(err => logger.error(err))
            ;
    }, [grid, gridQuery, gridStore, username]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.YouAreAboutToRemoveUserFrom,
        onSubmit: React.useCallback(() => {
            const requests = [];
            selection.forEach((group, idx) => {
                const request = {
                    username: username,
                    action: "Delete"
                };
                request["groupnames_i" + idx] = group.name;
                requests.push(WsAccess.UserGroupEdit({ request: request }));
            });
            Promise.all(requests)
                .then(responses => refreshTable())
                .catch(err => logger.error(err));
        }, [refreshTable, selection, username])
    });

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
                        window.open(`#/security/groups/${group?.name}`, "_blank");
                    });
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "add", text: nlsHPCC.Add,
            onClick: () => setShowAdd(true)
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteConfirm(true)
        },
    ], [refreshTable, selection, setShowDeleteConfirm, uiState.hasSelection]);

    React.useEffect(() => {
        if (!grid || !gridStore) return;
        refreshTable();
    }, [grid, gridStore, refreshTable]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} />}
            main={
                <DojoGrid
                    store={gridStore} query={gridQuery} sort={gridSort}
                    columns={gridColumns} setGrid={setGrid} setSelection={setSelection}
                />
            }
        />
        <UserAddGroupForm showForm={showAdd} setShowForm={setShowAdd} refreshGrid={refreshTable} username={username} />
        <DeleteConfirm />
    </>;

};