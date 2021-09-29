import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { ShortVerticalDivider } from "./Common";
import { pushUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoGrid, selector } from "./DojoGrid";
import { GroupAddUserForm } from "./forms/GroupAddUser";

const logger = scopedLogger("src-react/components/GroupMembers.tsx");

const defaultUIState = {
    hasSelection: false,
};

interface GroupMembersProps {
    groupname?: string;
}

export const GroupMembers: React.FunctionComponent<GroupMembersProps> = ({
    groupname,
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [showAdd, setShowAdd] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("username")));
    const gridSort = useConst([{ attribute: "name", "descending": false }]);
    const gridQuery = useConst({});
    const gridColumns = useConst({
        check: selector({ width: 27, label: " " }, "checkbox"),
        username: {
            label: nlsHPCC.UserName,
            formatter: function (_name, idx) {
                return `<a href="#/security/users/${_name}">${_name}</a>`;
            }
        },
        employeeID: { label: nlsHPCC.EmployeeID },
        employeeNumber: { label: nlsHPCC.EmployeeNumber },
        fullname: { label: nlsHPCC.FullName },
        passwordexpiration: { label: nlsHPCC.PasswordExpiration }
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
        WsAccess.GroupMemberQuery({
            request: { GroupName: groupname }
        })
            .then(({ GroupMemberQueryResponse }) => {
                if (GroupMemberQueryResponse?.Users) {
                    const users = GroupMemberQueryResponse?.Users?.User;
                    gridStore.setData(users);
                } else {
                    gridStore.setData([]);
                }

                grid?.set("query", gridQuery);
                if (clearSelection) {
                    grid?.clearSelection();
                }
            })
            .catch(logger.error)
            ;
    }, [grid, gridQuery, gridStore, groupname]);

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
                    pushUrl(`/security/users/${selection[0].username}`);
                } else {
                    selection.forEach(user => {
                        window.open(`#/security/users/${user?.username}`, "_blank");
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
            onClick: () => {
                if (confirm(nlsHPCC.YouAreAboutToRemoveUserFrom)) {
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
                        .then(responses => refreshTable())
                        .catch(logger.error);
                }
            }
        },
    ], [groupname, refreshTable, selection, uiState.hasSelection]);

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
        <GroupAddUserForm showForm={showAdd} setShowForm={setShowAdd} refreshGrid={refreshTable} groupname={groupname} />
    </>;

};
