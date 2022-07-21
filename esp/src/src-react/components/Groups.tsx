import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { AccessService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { ShortVerticalDivider } from "./Common";
import { useConfirm } from "../hooks/confirm";
import { useFluentGrid } from "../hooks/grid";
import { AddGroupForm } from "./forms/AddGroup";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/Groups.tsx");
const wsAccess = new AccessService({ baseUrl: "" });

const defaultUIState = {
    hasSelection: false,
};

interface GroupsProps {
    filter?: object;
}

export const Groups: React.FunctionComponent<GroupsProps> = ({
}) => {

    const [showAddGroup, setShowAddGroup] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const { Grid, selection } = useFluentGrid({
        data,
        primaryID: "name",
        sort: { attribute: "name", descending: false },
        filename: "groups",
        columns: {
            check: { width: 27, label: " ", selectorType: "checkbox" },
            name: {
                label: nlsHPCC.GroupName,
                formatter: function (_name, idx) {
                    return <Link href={`#/security/groups/${_name}`}>{_name}</Link>;
                }
            },
            groupOwner: { label: nlsHPCC.ManagedBy },
            groupDesc: { label: nlsHPCC.Description }
        }
    });

    const refreshData = React.useCallback(() => {
        wsAccess.GroupQuery({})
            .then(({ Groups }) => {
                if (Groups) {
                    setData(Groups?.Group.map((group, idx) => {
                        return {
                            name: group.name,
                            groupOwner: group.groupOwner,
                            groupDesc: group.groupDesc
                        };
                    }));
                }
            })
            .catch(err => logger.error(err));
    }, []);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedGroups,
        items: selection.map(group => group.name),
        onSubmit: React.useCallback(() => {
            const request = { ActionType: "delete" };
            selection.forEach((item, idx) => {
                request["groupnames_i" + idx] = item.name;
            });

            wsAccess.GroupAction(request)
                .then((response) => refreshData())
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
            onClick: () => refreshData()
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
    ], [exportGroups, refreshData, selection, setShowDeleteConfirm, uiState]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} overflowButtonProps={{}} />}
            main={<Grid />}
        />
        <AddGroupForm showForm={showAddGroup} setShowForm={setShowAddGroup} refreshGrid={refreshData} />
        <DeleteConfirm />
    </>;

};
