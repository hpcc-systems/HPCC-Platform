import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { AccessService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useFluentGrid } from "../hooks/grid";
import { ShortVerticalDivider } from "./Common";
import { AddUserForm } from "./forms/AddUser";
import { Filter } from "./forms/Filter";
import { Fields } from "./forms/Fields";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams, pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/Users.tsx");
const wsAccess = new AccessService({ baseUrl: "" });

const FilterFields: Fields = {
    "username": { type: "string", label: nlsHPCC.User }
};

const defaultUIState = {
    hasSelection: false,
};

interface UsersProps {
    filter?: { [key: string]: any };
}

const emptyFilter = {};

export const Users: React.FunctionComponent<UsersProps> = ({
    filter = emptyFilter
}) => {

    const [showAddUser, setShowAddUser] = React.useState(false);
    const [showFilter, setShowFilter] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const { Grid, selection, copyButtons } = useFluentGrid({
        data,
        primaryID: "username",
        sort: { attribute: "username", descending: false },
        filename: "users",
        columns: {
            check: { width: 27, selectorType: "checkbox" },
            username: {
                width: 180,
                label: nlsHPCC.Username,
                formatter: React.useCallback(function (_name, idx) {
                    return <Link href={`#/security/users/${_name}`}>{_name}</Link>;
                }, [])
            },
            employeeID: { width: 180, label: nlsHPCC.EmployeeID },
            employeeNumber: { width: 180, label: nlsHPCC.EmployeeNumber },
            fullname: { label: nlsHPCC.FullName },
            passwordexpiration: { width: 180, label: nlsHPCC.PasswordExpiration }
        }
    });

    const refreshData = React.useCallback(() => {
        wsAccess.UserQuery({ Name: filter?.username ?? "" })
            .then(({ Users }) => {
                if (Users) {
                    setData(Users?.User.map((user, idx) => {
                        return {
                            username: user.username,
                            employeeID: user.employeeID,
                            employeeNumber: user.employeeNumber,
                            fullname: user.fullname,
                            passwordexpiration: user.passwordexpiration
                        };
                    }));
                }
            })
            .catch(err => logger.error(err));
    }, [filter]);

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
            wsAccess.UserAction(request)
                .then(response => refreshData())
                .catch(err => logger.error(err));
        }, [refreshData, selection])
    });

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        if (selection.length > 0) {
            state.hasSelection = true;
        }

        setUIState(state);
    }, [selection]);

    React.useEffect(() => refreshData(), [refreshData]);

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
            onClick: () => refreshData()
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
    ], [exportUsers, refreshData, selection, setShowDeleteConfirm, uiState]);

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] };
    }

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} farItems={copyButtons} />}
            main={
                <Grid />
            }
        />
        <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
        <AddUserForm showForm={showAddUser} setShowForm={setShowAddUser} refreshGrid={refreshData} />
        <DeleteConfirm />
    </>;

};
