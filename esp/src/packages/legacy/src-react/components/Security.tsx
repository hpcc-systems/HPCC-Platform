import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { pushUrl } from "../util/history";
import { Groups } from "./Groups";
import { Permissions } from "./Permissions";
import { PermissionsEditor } from "./PermissionsEditor";
import { Users } from "./Users";
import { useBuildInfo } from "../hooks/platform";
import { pivotItemStyle } from "../layouts/pivot";
import nlsHPCC from "src/nlsHPCC";

interface SecurityProps {
    filter?: object;
    tab?: string;
    page?: number;
    name?: string;
    baseDn?: string;
}

export const Security: React.FunctionComponent<SecurityProps> = ({
    filter,
    tab = "users",
    page = 1,
    name,
    baseDn
}) => {

    const [, { opsCategory }] = useBuildInfo();

    const [permissionTabTitle, setPermissionTabTitle] = React.useState(nlsHPCC.Permissions);

    React.useEffect(() => {
        setPermissionTabTitle(nlsHPCC.Permissions);
        if (name === "_") {
            if (baseDn === "File Scopes") setPermissionTabTitle(nlsHPCC.FileScopeDefaultPermissions);
            else if (baseDn === "Workunit Scopes") setPermissionTabTitle(nlsHPCC.WorkUnitScopeDefaultPermissions);
        } else if (name === "file") {
            if (baseDn === "File Scopes") setPermissionTabTitle(nlsHPCC.PhysicalFiles);
        } else if (name) {
            setPermissionTabTitle(name);
        }
    }, [name, baseDn]);

    return <>
        <SizeMe monitorHeight>{({ size }) =>
            <Pivot
                overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
                onLinkClick={evt => pushUrl(`/${opsCategory}/security/${evt.props.itemKey}`)}
            >
                <PivotItem headerText={nlsHPCC.Users} itemKey="users" style={pivotItemStyle(size)}>
                    <Users filter={filter} page={page} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Groups} itemKey="groups" style={pivotItemStyle(size)}>
                    <Groups page={page} />
                </PivotItem>
                <PivotItem headerText={permissionTabTitle} itemKey="permissions" style={pivotItemStyle(size)}>
                    {!name && !baseDn &&
                        <Permissions />
                    }
                    {name && baseDn &&
                        <PermissionsEditor BaseDn={baseDn} Name={name} />
                    }
                </PivotItem>
            </Pivot>
        }</SizeMe>
    </>;

};
