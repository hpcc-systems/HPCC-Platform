import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { ShortVerticalDivider } from "./Common";
import { useConfirm } from "../hooks/confirm";
import { useFluentGrid } from "../hooks/grid";
import { pushUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
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

    const [showAdd, setShowAdd] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "username",
        sort: [{ attribute: "name", "descending": false }],
        filename: "fileParts",
        columns: {
            username: {
                label: nlsHPCC.UserName,
                formatter: function (_name, idx) {
                    return <Link href={`#/security/users/${_name}`}>{_name}</Link>;
                }
            },
            employeeID: { label: nlsHPCC.EmployeeID },
            employeeNumber: { label: nlsHPCC.EmployeeNumber },
            fullname: { label: nlsHPCC.FullName },
            passwordexpiration: { label: nlsHPCC.PasswordExpiration }
        }
    });

    const refreshData = React.useCallback(() => {
        WsAccess.GroupMemberQuery({
            request: { GroupName: groupname }
        }).then(({ GroupMemberQueryResponse }) => {
            if (GroupMemberQueryResponse?.Users) {
                const users = GroupMemberQueryResponse?.Users?.User;
                setData(users);
            } else {
                setData([]);
            }
        }).catch(err => logger.error(err))
            ;
    }, [groupname]);

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
                .then(() => refreshData())
                .catch(err => logger.error(err));
        }, [groupname, refreshData, selection])
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
            onClick: () => refreshData()
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
            onClick: () => setShowDeleteConfirm(true)
        },
    ], [refreshData, selection, setShowDeleteConfirm, uiState.hasSelection]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} farItems={copyButtons} />}
            main={
                <Grid />
            }
        />
        <GroupAddUserForm showForm={showAdd} setShowForm={setShowAdd} refreshGrid={refreshData} groupname={groupname} />
        <DeleteConfirm />
    </>;

};
