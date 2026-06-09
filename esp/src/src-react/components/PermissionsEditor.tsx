import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { Checkbox, Label } from "@fluentui/react-components";
import { WsAccess, AccessService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { HolyGrail } from "../layouts/HolyGrail";
import { AddGroupResourceForm } from "./forms/AddGroupResource";

const logger = scopedLogger("src-react/components/PermissionsEditor.tsx");

const service = new AccessService({ baseUrl: "" });

// from ShowIndividualPermissionsWidget.js
const calcPermissionState = (field, value, row) => {
    switch (field) {
        case "allow_access":
            row.allow_full = value && row.allow_read && row.allow_write;
            if (value)
                calcPermissionState("deny_access", false, row);
            break;
        case "allow_read":
            row.allow_full = row.allow_access && value && row.allow_write;
            if (value)
                calcPermissionState("deny_read", false, row);
            break;
        case "allow_write":
            row.allow_full = row.allow_access && row.allow_read && value;
            if (value)
                calcPermissionState("deny_write", false, row);
            break;
        case "allow_full":
            row.allow_access = value;
            row.allow_read = value;
            row.allow_write = value;
            if (value)
                calcPermissionState("deny_full", false, row);
            break;
        case "deny_access":
            row.deny_full = value && row.deny_read && row.deny_write;
            if (value)
                calcPermissionState("allow_access", false, row);
            break;
        case "deny_read":
            row.deny_full = row.deny_access && value && row.deny_write;
            if (value)
                calcPermissionState("allow_read", false, row);
            break;
        case "deny_write":
            row.deny_full = row.deny_access && row.deny_read && value;
            if (value)
                calcPermissionState("allow_write", false, row);
            break;
        case "deny_full":
            row.deny_access = value;
            row.deny_read = value;
            row.deny_write = value;
            if (value)
                calcPermissionState("allow_full", false, row);
            break;
    }
    row[field] = value;
};

interface PermissionsEditorProps {
    BaseDn?: string;
    Name?: string;
}

export const PermissionsEditor: React.FunctionComponent<PermissionsEditorProps> = ({
    BaseDn,
    Name
}) => {

    const [data, setData] = React.useState<any[]>([]);
    const [selectedIndex, setSelectedIndex] = React.useState(-1);
    const [showAddGroup, setShowAddGroup] = React.useState(false);

    const refreshData = React.useCallback(() => {
        service.ResourcePermissions({ BasednName: BaseDn, name: Name })
            .then(({ Permissions }: WsAccess.ResourcePermissionsResponse) => {
                setData(Permissions?.Permission.map(Permission => {
                    return {
                        account_name: Permission.account_name,
                        allow_access: Permission.allow_access ?? false,
                        allow_read: Permission.allow_read ?? false,
                        allow_write: Permission.allow_write ?? false,
                        allow_full: Permission.allow_full ?? false,
                        deny_access: Permission.deny_access ?? false,
                        deny_read: Permission.deny_read ?? false,
                        deny_write: Permission.deny_write ?? false,
                        deny_full: Permission.deny_full ?? false,
                    };
                }));
                setSelectedIndex(-1);
            })
            .catch(err => logger.error(err))
            ;
    }, [BaseDn, Name]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedGroups + "\n\n" + data[selectedIndex]?.account_name,
        onSubmit: React.useCallback(() => {
            service.PermissionAction({
                action: "delete",
                BasednName: BaseDn,
                rname: Name,
                account_name: data[selectedIndex]?.account_name
            })
                .then(() => refreshData())
                .catch(err => logger.error(err))
                ;
        }, [BaseDn, data, Name, refreshData, selectedIndex])
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "add", text: nlsHPCC.Add,
            onClick: () => setShowAddGroup(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: selectedIndex < 0,
            onClick: () => setShowDeleteConfirm(true)
        },
    ], [refreshData, selectedIndex, setShowDeleteConfirm]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    const onRowSelect = React.useCallback((evt, index) => {
        if (evt.target.checked) {
            setSelectedIndex(index);
        } else {
            setSelectedIndex(-1);
        }
    }, [setSelectedIndex]);

    const onPermissionCheckboxClick = React.useCallback((evt, permission, prop) => {
        calcPermissionState(prop, evt.target.checked, permission);
        service.PermissionAction({
            action: "update",
            BasednName: BaseDn,
            rname: Name,
            account_type: 1,
            ...permission
        }).then(({ retcode, retmsg }) => {
            if (retcode === 0) {
                setData(prevState => {
                    const newState = Array.from(prevState);
                    return newState;
                });
            } else if (retcode === -1) {
                logger.error(retmsg);
                refreshData();
            }
        }).catch(err => logger.error(err));
    }, [BaseDn, Name, refreshData]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} />}
            main={
                <div style={{ margin: "0 10px" }}>
                    <div style={{ display: "flex", flexDirection: "row" }}>
                        <div style={{ flexGrow: 1 }}><Label>{nlsHPCC.Account}</Label></div>
                        <div style={{ width: "min-content", margin: "0 10px" }}><Label>{nlsHPCC.AllowAccess}</Label></div>
                        <div style={{ width: "min-content", margin: "0 10px" }}><Label>{nlsHPCC.AllowRead}</Label></div>
                        <div style={{ width: "min-content", margin: "0 10px" }}><Label>{nlsHPCC.AllowWrite}</Label></div>
                        <div style={{ width: "min-content", margin: "0 16px 0 10px" }}><Label>{nlsHPCC.AllowFull}</Label></div>
                        <div style={{ width: "min-content", margin: "0 10px 0 16px" }}><Label>{nlsHPCC.DenyAccess}</Label></div>
                        <div style={{ width: "min-content", margin: "0 10px" }}><Label>{nlsHPCC.DenyRead}</Label></div>
                        <div style={{ width: "min-content", margin: "0 10px" }}><Label>{nlsHPCC.DenyWrite}</Label></div>
                        <div style={{ width: "min-content", margin: "0 10px" }}><Label>{nlsHPCC.DenyFull}</Label></div>
                    </div>
                    {data?.map((permission, index) => (
                        <div key={`${permission.account_name}-${index}`} style={{ display: "flex", flexDirection: "row", marginBottom: 5 }}>
                            <div><Checkbox checked={index === selectedIndex ? true : false} onChange={(ev) => onRowSelect(ev, index)} /></div>
                            <div style={{ flexGrow: 1 }}>{permission.account_name}</div>
                            <div style={{ width: 36, margin: "0 10px" }}><Checkbox checked={permission.allow_access} onChange={(ev) => onPermissionCheckboxClick(ev, permission, "allow_access")} /></div>
                            <div style={{ width: 36, margin: "0 10px" }}><Checkbox checked={permission.allow_read} onChange={(ev) => onPermissionCheckboxClick(ev, permission, "allow_read")} /></div>
                            <div style={{ width: 36, margin: "0 10px" }}><Checkbox checked={permission.allow_write} onChange={(ev) => onPermissionCheckboxClick(ev, permission, "allow_write")} /></div>
                            <div style={{ width: 36, margin: "0 20px 0 10px" }}><Checkbox checked={permission.allow_full} onChange={(ev) => onPermissionCheckboxClick(ev, permission, "allow_full")} /></div>
                            <div style={{ width: 36, margin: "0 10px" }}><Checkbox checked={permission.deny_access} onChange={(ev) => onPermissionCheckboxClick(ev, permission, "deny_access")} /></div>
                            <div style={{ width: 36, margin: "0 10px" }}><Checkbox checked={permission.deny_read} onChange={(ev) => onPermissionCheckboxClick(ev, permission, "deny_read")} /></div>
                            <div style={{ width: 36, margin: "0 10px" }}><Checkbox checked={permission.deny_write} onChange={(ev) => onPermissionCheckboxClick(ev, permission, "deny_write")} /></div>
                            <div style={{ width: 36, margin: "0 10px" }}><Checkbox checked={permission.deny_full} onChange={(ev) => onPermissionCheckboxClick(ev, permission, "deny_full")} /></div>
                        </div>
                    ))}
                </div>
            }
        />
        <AddGroupResourceForm rname={Name} BasednName={BaseDn} refreshGrid={refreshData} showForm={showAddGroup} setShowForm={setShowAddGroup} />
        <DeleteConfirm />
    </>;
};