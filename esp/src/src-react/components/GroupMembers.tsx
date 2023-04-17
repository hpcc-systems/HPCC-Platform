import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";
import { GroupMemberStore, CreateGroupMemberStore } from "src/ws_access";
import { ShortVerticalDivider } from "./Common";
import { useConfirm } from "../hooks/confirm";
import { useFluentPagedGrid } from "../hooks/grid";
import { pushUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { GroupAddUserForm } from "./forms/GroupAddUser";
import { QuerySortItem } from "src/store/Store";

const logger = scopedLogger("src-react/components/GroupMembers.tsx");

const defaultUIState = {
    hasSelection: false,
};

interface GroupMembersProps {
    groupname?: string;
    sort?: QuerySortItem;
    page?: number;
    store?: GroupMemberStore;
}

const defaultSort = { attribute: "username", descending: false };

export const GroupMembers: React.FunctionComponent<GroupMembersProps> = ({
    groupname,
    sort = defaultSort,
    page = 1,
    store
}) => {

    const [showAdd, setShowAdd] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const query = React.useMemo(() => {
        return { GroupName: groupname };
    }, [groupname]);

    const gridStore = React.useMemo(() => {
        return store ? store : CreateGroupMemberStore();
    }, [store]);

    const { Grid, GridPagination, selection, copyButtons, refreshTable } = useFluentPagedGrid({
        persistID: "username",
        store: gridStore,
        query,
        sort,
        pageNum: page,
        filename: "groupMembers",
        columns: {
            username: {
                label: nlsHPCC.UserName,
                formatter: React.useCallback(function (_name, idx) {
                    return <Link href={`#/${Utility.opsRouteCategory}/security/users/${_name}`}>{_name}</Link>;
                }, [])
            },
            employeeID: { label: nlsHPCC.EmployeeID },
            employeeNumber: { label: nlsHPCC.EmployeeNumber },
            fullname: { label: nlsHPCC.FullName },
            passwordexpiration: { label: nlsHPCC.PasswordExpiration }
        }
    });

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.YouAreAboutToRemoveUserFrom,
        onSubmit: React.useCallback(() => {
            const requests = [];
            selection.forEach((user, idx) => {
                const request = {
                    groupname: groupname,
                    action: "Delete"
                };
                request["usernames_i" + idx] = user.username;
                requests.push(WsAccess.GroupMemberEdit({ request: request }));
            });
            Promise.all(requests)
                .then(() => refreshTable(true))
                .catch(err => logger.error(err));
        }, [groupname, refreshTable, selection])
    });

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        if (selection.length > 0) {
            state.hasSelection = true;
        }

        setUIState(state);
    }, [selection]);

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
                    pushUrl(`/${Utility.opsRouteCategory}/security/users/${selection[0].username}`);
                } else {
                    selection.forEach(user => {
                        window.open(`#/${Utility.opsRouteCategory}/security/users/${user?.username}`, "_blank");
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

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} farItems={copyButtons} />}
            main={<Grid />}
            footer={<GridPagination />}
        />
        <GroupAddUserForm showForm={showAdd} setShowForm={setShowAdd} refreshGrid={refreshTable} groupname={groupname} />
        <DeleteConfirm />
    </>;

};
