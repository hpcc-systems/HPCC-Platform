import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { AccessService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { GroupStore, CreateGroupStore } from "src/ws_access";
import { ShortVerticalDivider } from "./Common";
import { useConfirm } from "../hooks/confirm";
import { useFluentPagedGrid } from "../hooks/grid";
import { AddGroupForm } from "./forms/AddGroup";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushUrl } from "../util/history";
import { QuerySortItem } from "src/store/Store";
import * as Utility from "src/Utility";

const logger = scopedLogger("src-react/components/Groups.tsx");
const wsAccess = new AccessService({ baseUrl: "" });

const defaultUIState = {
    hasSelection: false,
};

interface GroupsProps {
    sort?: QuerySortItem;
    page?: number;
    store?: GroupStore;
}

const defaultSort = { attribute: "Name", descending: false };

export const Groups: React.FunctionComponent<GroupsProps> = ({
    sort = defaultSort,
    page = 1,
    store
}) => {

    const [showAddGroup, setShowAddGroup] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const gridStore = React.useMemo(() => {
        return store ? store : CreateGroupStore();
    }, [store]);

    const { Grid, GridPagination, selection, refreshTable } = useFluentPagedGrid({
        persistID: "Name",
        store: gridStore,
        sort,
        pageNum: page,
        filename: "groups",
        columns: {
            check: { width: 27, label: " ", selectorType: "checkbox" },
            name: {
                label: nlsHPCC.GroupName,
                formatter: function (_name, idx) {
                    return <Link href={`#/${Utility.opsRouteCategory}/security/groups/${_name}`}>{_name}</Link>;
                }
            },
            groupOwner: { label: nlsHPCC.ManagedBy },
            groupDesc: { label: nlsHPCC.Description }
        }
    });

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
                .then((response) => refreshTable())
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
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`/${Utility.opsRouteCategory}/security/groups/${selection[0].name}`);
                } else {
                    selection.forEach(group => {
                        window.open(`#/${Utility.opsRouteCategory}/security/groups/${group.name}`, "_blank");
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
    ], [exportGroups, refreshTable, selection, setShowDeleteConfirm, uiState]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} overflowButtonProps={{}} />}
            main={
                <>
                    <SizeMe monitorHeight>{({ size }) =>
                        <div style={{ width: "100%", height: "100%" }}>
                            <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                                <Grid height={`${size.height}px`} />
                            </div>
                        </div>
                    }</SizeMe>
                    <AddGroupForm showForm={showAddGroup} setShowForm={setShowAddGroup} refreshGrid={refreshTable} />
                    <DeleteConfirm />
                </>
            }
            footer={<GridPagination />}
        />
    </>;

};
