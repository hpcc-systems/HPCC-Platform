import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useGrid } from "../hooks/grid";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";
import { AddUserForm } from "./forms/AddUser";
import { Filter } from "./forms/Filter";
import { Fields } from "./forms/Fields";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams, pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/Users.tsx");

const FilterFields: Fields = {
    "username": { type: "string", label: nlsHPCC.User }
};

function formatQuery(_filter) {
    const filter = { ..._filter };

    filter.Name = filter.username;
    delete filter.username;

    logger.debug(filter);
    return filter;
}

const defaultUIState = {
    hasSelection: false,
};

interface UsersProps {
    filter?: object;
}

const emptyFilter = {};

export const Users: React.FunctionComponent<UsersProps> = ({
    filter = emptyFilter
}) => {

    const [showAddUser, setShowAddUser] = React.useState(false);
    const [showFilter, setShowFilter] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store: WsAccess.CreateUsersStore(null, true),
        query: formatQuery(filter),
        sort: [{ attribute: "username", "descending": false }],
        filename: "users",
        columns: {
            check: selector({ width: 27 }, "checkbox"),
            username: {
                width: 180,
                label: nlsHPCC.Username,
                formatter: function (_name, idx) {
                    return `<a href="#/security/users/${_name}">${_name}</a>`;
                }
            },
            employeeID: { width: 180, label: nlsHPCC.EmployeeID },
            employeeNumber: { width: 180, label: nlsHPCC.EmployeeNumber },
            fullname: { label: nlsHPCC.FullName },
            passwordexpiration: { width: 180, label: nlsHPCC.PasswordExpiration }
        }
    });

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedUsers,
        items: selection.map(user => user.username),
        onSubmit: React.useCallback(() => {
            const request = {
                ActionType: "delete"
            };
            selection.forEach((item, idx) => {
                request["usernames_i" + idx] = item.username;
            });
            WsAccess.UserAction({ request: request })
                .then(response => {
                    refreshTable(true);
                })
                .catch(err => logger.error(err))
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

    const exportUsers = React.useCallback(() => {
        let usernames = "";
        selection.forEach((item, idx) => {
            if (usernames.length) {
                usernames += "&";
            }
            usernames += "usernames_i" + idx + "=" + item.username;
        });
        window.open(`/ws_access/UserAccountExport?${usernames}`, "_blank");
    }, [selection]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter,
            onClick: () => setShowFilter(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`/security/users/${selection[0].username}`);
                } else {
                    selection.forEach(user => {
                        window.open(`#/security/users/${user?.username}`, "_blank");
                    });
                }
            }
        },
        {
            key: "add", text: nlsHPCC.Add,
            onClick: () => setShowAddUser(true)
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "export", text: nlsHPCC.Export,
            onClick: () => exportUsers()
        },
    ], [exportUsers, refreshTable, selection, setShowDeleteConfirm, uiState]);

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] };
    }

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} farItems={copyButtons} />}
            main={
                <>
                    <Grid />
                    <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                </>
            }
        />
        <AddUserForm showForm={showAddUser} setShowForm={setShowAddUser} refreshGrid={refreshTable} />
        <DeleteConfirm />
    </>;

};
