import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { AccessService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { ShortVerticalDivider } from "./Common";
import { pushUrl } from "../util/history";
import { useConfirm } from "../hooks/confirm";
import { useBuildInfo } from "../hooks/platform";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { UserAddGroupForm } from "./forms/UserAddGroup";

const logger = scopedLogger("src-react/components/UserGroups.tsx");
const wsAccess = new AccessService({ baseUrl: "" });

const defaultUIState = {
    hasSelection: false,
};

interface UserGroupsProps {
    username?: string;
}

export const UserGroups: React.FunctionComponent<UserGroupsProps> = ({
    username,
}) => {

    const [, { opsCategory }] = useBuildInfo();

    const [showAdd, setShowAdd] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---

    const columns = React.useMemo((): FluentColumns => {
        return {
            check: { width: 27, label: " ", selectorType: "checkbox" },
            name: {
                label: nlsHPCC.GroupName,
                formatter: (_name, idx) => {
                    _name = _name.replace(/[^-_a-zA-Z0-9\s]+/g, "");
                    return <Link href={`#/${opsCategory}/security/groups/${_name}`}>{_name}</Link>;
                }
            }
        };
    }, [opsCategory]);

    const refreshData = React.useCallback(() => {
        wsAccess.UserEdit({ username: username })
            .then(({ Groups }) => setData(Groups?.Group ?? []))
            .catch(err => logger.error(err));
    }, [username]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        if (selection.length > 0) {
            state.hasSelection = true;
        }

        setUIState(state);
    }, [selection]);

    React.useEffect(() => refreshData(), [refreshData]);

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
                requests.push(wsAccess.UserGroupEdit(request));
            });
            Promise.all(requests)
                .then(responses => refreshData())
                .catch(err => logger.error(err));
        }, [refreshData, selection, username])
    });

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
                    pushUrl(`/${opsCategory}/security/groups/${selection[0].name}`);
                } else {
                    selection.forEach(group => {
                        window.open(`#/${opsCategory}/security/groups/${group?.name}`, "_blank");
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
    ], [opsCategory, refreshData, selection, setShowDeleteConfirm, uiState.hasSelection]);

    const copyButtons = useCopyButtons(columns, selection, "userGroups");

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} farItems={copyButtons} />}
            main={<FluentGrid
                data={data}
                primaryID={"username"}
                sort={{ attribute: "name", descending: false }}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentGrid >}
        />
        < UserAddGroupForm showForm={showAdd} setShowForm={setShowAdd} refreshGrid={refreshData} username={username} />
        <DeleteConfirm />
    </>;

};