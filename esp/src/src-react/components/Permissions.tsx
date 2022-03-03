import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { ShortVerticalDivider } from "./Common";
import { useConfirm } from "../hooks/confirm";
import { DojoGrid, selector, tree } from "./DojoGrid";
import { AddPermissionForm } from "./forms/AddPermission";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/Permissions.tsx");

const defaultUIState = {
    canDelete: false,
    categorySelected: false,
    fileScope: false,
    hasSelection: false,
    repositoryModule: false,
    workunitScope: false
};

interface PermissionsProps {
}

export const Permissions: React.FunctionComponent<PermissionsProps> = ({
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);

    const [showAddPermission, setShowAddPermission] = React.useState(false);
    const [scopeScansEnabled, setScopeScansEnabled] = React.useState(false);
    const [modulesDn, setModulesDn] = React.useState("");
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const gridStore = useConst(WsAccess.CreatePermissionsStore(null, null));
    const gridSort = useConst([{ attribute: "name", descending: false }]);
    const gridQuery = useConst({});
    const gridColumns = useConst({
        check: selector({
            width: 27,
            disabled: function (row) {
                if (row.name === "File Scopes" || row.name === "Workunit Scopes" || row.name === "Repository Modules") {
                    return false;
                }
                return row.children ? true : false;
            }
        }, "checkbox"),
        name: tree({
            width: 360,
            label: nlsHPCC.Name,
            formatter: function (_name, idx) {
                if (idx.__hpcc_parent) {
                    return `<a href="/#/security/permissions/${_name}/${idx.__hpcc_parent.name}">${_name}</a>`;
                } else {
                    return _name;
                }
            }
        }),
        description: { label: nlsHPCC.Description },
        basedn: { label: "basedn" }
    });

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };
        if (selection.length) {
            state.hasSelection = true;
            if (!selection.filter(item => item.hasOwnProperty("children")).length) {
                state.canDelete = true;
            } else {
                state.categorySelected = true;
            }
            if (selection.filter(item => item.name === "File Scopes").length) {
                state.fileScope = true;
            }
            const _modules = selection.filter(item => item.name === "Repository Modules");
            if (_modules.length) {
                setModulesDn(_modules[0].basedn);
                state.repositoryModule = true;
            }
            if (selection.filter(item => item.name === "Workunit Scopes").length) {
                state.workunitScope = true;
            }
        }
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        WsAccess.Resources({
            request: {
                name: "File Scopes"
            }
        })
            .then(({ ResourcesResponse }) => {
                setScopeScansEnabled(ResourcesResponse?.scopeScansStatus?.isEnabled || false);
            })
            .catch(err => logger.error(err))
            ;
    }, [setScopeScansEnabled]);

    const refreshTable = React.useCallback((clearSelection = false) => {
        grid?.set("query", gridQuery);
        if (clearSelection) {
            grid?.clearSelection();
        }
    }, [grid, gridQuery]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedPermissions,
        items: selection.map(file => file.name),
        onSubmit: React.useCallback(() => {
            const deleteRequests = {};
            const requests = [];
            selection.forEach((item, idx) => {
                if (!deleteRequests[item.__hpcc_id]) {
                    deleteRequests[item.__hpcc_id] = {
                        action: "Delete",
                        BasednName: item.__hpcc_parent.name,
                        rtype: item.__hpcc_parent.rtype,
                        rtitle: item.__hpcc_parent.rtitle
                    };
                }
                deleteRequests[item.__hpcc_id]["names_i" + idx] = item.name;
            });
            for (const key in deleteRequests) {
                requests.push(WsAccess.ResourceDelete({
                    request: deleteRequests[key]
                }));
            }

            Promise.all(requests)
                .then(() => {
                    refreshTable();
                })
                .catch(err => logger.error(err))
                ;
        }, [refreshTable, selection])
    });

    const [ClearPermissionsConfirm, setShowClearPermissionsConfirm] = useConfirm({
        title: nlsHPCC.ClearPermissionsCache,
        message: nlsHPCC.ClearPermissionsCacheConfirm,
        onSubmit: () => WsAccess.ClearPermissionsCache
    });

    const [EnableScopesConfirm, setShowEnableScopesConfirm] = useConfirm({
        title: nlsHPCC.EnableScopeScans,
        message: nlsHPCC.EnableScopeScansConfirm,
        onSubmit: () => WsAccess.EnableScopeScans
    });

    const [DisableScopesConfirm, setShowDisableScopesConfirm] = useConfirm({
        title: nlsHPCC.DisableScopeScans,
        message: nlsHPCC.DisableScopeScanConfirm,
        onSubmit: () => WsAccess.DisableScopeScans
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "add", text: nlsHPCC.Add,
            onClick: () => setShowAddPermission(true)
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.canDelete,
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "clearPermissions", text: nlsHPCC.ClearPermissionsCache,
            onClick: () => setShowClearPermissionsConfirm(true)
        },
        {
            key: "advanced", text: nlsHPCC.Advanced, disabled: !uiState.hasSelection || !uiState.categorySelected,
            subMenuProps: {
                items: [
                    {
                        key: "enableScopeScans",
                        text: nlsHPCC.EnableScopeScans,
                        onClick: () => setShowEnableScopesConfirm(true),
                        disabled: scopeScansEnabled
                    },
                    {
                        key: "disableScopeScans",
                        text: nlsHPCC.DisableScopeScans,
                        onClick: () => setShowDisableScopesConfirm(true),
                        disabled: !scopeScansEnabled
                    },
                    { key: "fileScopeDefaults", text: nlsHPCC.FileScopeDefaultPermissions, onClick: (evt, item) => pushUrl("/security/permissions/_/File%20Scopes"), disabled: !uiState.fileScope },
                    { key: "workunitScopeDefaults", text: nlsHPCC.WorkUnitScopeDefaultPermissions, onClick: (evt, item) => pushUrl("/security/permissions/_/Workunit%20Scopes"), disabled: !uiState.workunitScope },
                    { key: "physicalFiles", text: nlsHPCC.PhysicalFiles, onClick: (evt, item) => pushUrl("/security/permissions/file/File%20Scopes"), disabled: !uiState.fileScope },
                    { key: "checkFilePermissions", text: nlsHPCC.CheckFilePermissions, disabled: !uiState.fileScope },
                    { key: "codeGenerator", text: nlsHPCC.CodeGenerator, onClick: (evt, item) => pushUrl(`/security/permissions/_/${modulesDn}`), disabled: !uiState.repositoryModule },
                ],
            },
        },
    ], [modulesDn, refreshTable, setShowClearPermissionsConfirm, setShowDeleteConfirm, setShowDisableScopesConfirm, setShowEnableScopesConfirm, scopeScansEnabled, uiState]);

    React.useEffect(() => {
        refreshTable();
    }, [grid?.data, refreshTable]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} overflowButtonProps={{}} />}
            main={<DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />}
        />
        <DeleteConfirm />
        <ClearPermissionsConfirm />
        <EnableScopesConfirm />
        <DisableScopesConfirm />
        <AddPermissionForm showForm={showAddPermission} setShowForm={setShowAddPermission} refreshGrid={refreshTable} />
    </>;

};