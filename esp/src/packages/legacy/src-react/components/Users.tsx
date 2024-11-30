import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { AccessService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { UserStore, CreateUserStore } from "src/ws_access";
import { useConfirm } from "../hooks/confirm";
import { useBuildInfo } from "../hooks/platform";
import { FluentPagedGrid, FluentPagedFooter, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";
import { AddUserForm } from "./forms/AddUser";
import { Filter } from "./forms/Filter";
import { Fields } from "./forms/Fields";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams, pushUrl } from "../util/history";
import { QuerySortItem } from "src/store/Store";

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
    sort?: QuerySortItem;
    page?: number;
    store?: UserStore;
}

const emptyFilter: { [key: string]: any } = {};
const defaultSort = { attribute: "username", descending: false };

export const Users: React.FunctionComponent<UsersProps> = ({
    filter = emptyFilter,
    sort = defaultSort,
    page = 1,
    store
}) => {

    const [, { opsCategory }] = useBuildInfo();

    const [showAddUser, setShowAddUser] = React.useState(false);
    const [showFilter, setShowFilter] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const {
        selection, setSelection,
        pageNum, setPageNum,
        pageSize, setPageSize,
        total, setTotal,
        refreshTable } = useFluentStoreState({ page });

    //  Grid ---
    const query = React.useMemo(() => {
        return { Name: filter?.username };
    }, [filter]);

    const gridStore = React.useMemo(() => {
        return store ? store : CreateUserStore();
    }, [store]);

    const columns = React.useMemo((): FluentColumns => {
        return {
            check: { width: 27, selectorType: "checkbox" },
            username: {
                width: 180,
                label: nlsHPCC.Username,
                formatter: (_name, idx) => {
                    return <Link href={`#/${opsCategory}/security/users/${_name}`}>{_name}</Link>;
                }
            },
            employeeID: { width: 180, label: nlsHPCC.EmployeeID },
            employeeNumber: { width: 180, label: nlsHPCC.EmployeeNumber },
            fullname: { label: nlsHPCC.FullName },
            passwordexpiration: { width: 180, label: nlsHPCC.PasswordExpiration }
        };
    }, [opsCategory]);

    const copyButtons = useCopyButtons(columns, selection, "users");

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
                .then(response => refreshTable.call())
                .catch(err => logger.error(err));
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
            onClick: () => refreshTable.call()
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
                    pushUrl(`/${opsCategory}/security/users/${selection[0].username}`);
                } else {
                    selection.forEach(user => {
                        window.open(`#/${opsCategory}/security/users/${user?.username}`, "_blank");
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
    ], [exportUsers, opsCategory, refreshTable, selection, setShowDeleteConfirm, uiState]);

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] };
    }

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <SizeMe monitorHeight>{({ size }) =>
                    <div style={{ width: "100%", height: "100%" }}>
                        <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                            <FluentPagedGrid
                                store={gridStore}
                                query={query}
                                sort={sort}
                                pageNum={pageNum}
                                pageSize={pageSize}
                                total={total}
                                columns={columns}
                                height={`${size.height}px`}
                                setSelection={setSelection}
                                setTotal={setTotal}
                                refresh={refreshTable}
                            ></FluentPagedGrid>
                        </div>
                    </div>
                }</SizeMe>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <AddUserForm showForm={showAddUser} setShowForm={setShowAddUser} refreshGrid={refreshTable.call} />
                <DeleteConfirm />
            </>
        }
        footer={<FluentPagedFooter
            persistID={"username"}
            pageNum={pageNum}
            selectionCount={selection.length}
            setPageNum={setPageNum}
            setPageSize={setPageSize}
            total={total}
        ></FluentPagedFooter>}
    />;

};
