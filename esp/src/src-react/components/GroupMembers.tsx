import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { GroupMemberStore, CreateGroupMemberStore } from "src/ws_access";
import { ShortVerticalDivider } from "./Common";
import { useConfirm } from "../hooks/confirm";
import { useBuildInfo } from "../hooks/platform";
import { pushUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentPagedGrid, FluentPagedFooter, useCopyButtons, useFluentStoreState } from "./controls/Grid";
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

    const [, { opsCategory }] = useBuildInfo();

    const [showAdd, setShowAdd] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const {
        selection, setSelection,
        pageNum, setPageNum,
        pageSize, setPageSize,
        total, setTotal,
        refreshTable } = useFluentStoreState({ page });

    //  Grid ---
    const query = React.useMemo(() => {
        return { GroupName: groupname };
    }, [groupname]);

    const gridStore = React.useMemo(() => {
        return store ? store : CreateGroupMemberStore();
    }, [store]);

    const columns = React.useMemo(() => {
        return {
            username: {
                label: nlsHPCC.UserName,
                formatter: (_name, idx) => {
                    return <Link href={`#/${opsCategory}/security/users/${_name}`}>{_name}</Link>;
                }
            },
            employeeID: { label: nlsHPCC.EmployeeID },
            employeeNumber: { label: nlsHPCC.EmployeeNumber },
            fullname: { label: nlsHPCC.FullName },
            passwordexpiration: { label: nlsHPCC.PasswordExpiration }
        };
    }, [opsCategory]);

    const copyButtons = useCopyButtons(columns, selection, "groupMembers");

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
                .then(() => refreshTable.call(true))
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
            onClick: () => refreshTable.call()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`/${opsCategory}/security/users/${selection[0].username}`);
                } else {
                    selection.forEach(user => {
                        window.open(`#/${opsCategory}/security/users/${user?.username}`, "_blank");
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
    ], [refreshTable, opsCategory, selection, setShowDeleteConfirm, uiState.hasSelection]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} farItems={copyButtons} />}
            main={<FluentPagedGrid
                store={gridStore}
                query={query}
                sort={sort}
                pageNum={pageNum}
                pageSize={pageSize}
                total={total}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentPagedGrid>}
            footer={<FluentPagedFooter
                persistID={"username"}
                pageNum={pageNum}
                setPageNum={setPageNum}
                setPageSize={setPageSize}
                total={total}
            >
            </FluentPagedFooter>}
        />
        <GroupAddUserForm showForm={showAdd} setShowForm={setShowAdd} refreshGrid={refreshTable.call} groupname={groupname} />
        <DeleteConfirm />
    </>;

};
