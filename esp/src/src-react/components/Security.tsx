import * as React from "react";
import { SelectTabData, SelectTabEvent, Tab, TabList, makeStyles } from "@fluentui/react-components";
import { SizeMe } from "../layouts/SizeMe";
import { pushUrl } from "../util/history";
import { Groups } from "./Groups";
import { Permissions } from "./Permissions";
import { PermissionsEditor } from "./PermissionsEditor";
import { Users } from "./Users";
import { useBuildInfo } from "../hooks/platform";
import { pivotItemStyle } from "../layouts/pivot";
import nlsHPCC from "src/nlsHPCC";

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative"
    }
});

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

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        pushUrl(`/${opsCategory}/security/${data.value as string}`);
    }, [opsCategory]);

    const styles = useStyles();

    return <>
        <SizeMe>{({ size }) =>
            <div className={styles.container}>
                <TabList selectedValue={tab} onTabSelect={onTabSelect} size="medium">
                    <Tab value="users">{nlsHPCC.Users}</Tab>
                    <Tab value="groups">{nlsHPCC.Groups}</Tab>
                    <Tab value="permissions">{permissionTabTitle}</Tab>
                </TabList>
                {tab === "users" &&
                    <div style={pivotItemStyle(size)}>
                        <Users filter={filter} page={page} />
                    </div>
                }
                {tab === "groups" &&
                    <div style={pivotItemStyle(size)}>
                        <Groups page={page} />
                    </div>
                }
                {tab === "permissions" &&
                    <div style={pivotItemStyle(size)}>
                        {!name && !baseDn &&
                            <Permissions />
                        }
                        {name && baseDn &&
                            <PermissionsEditor BaseDn={baseDn} Name={name} />
                        }
                    </div>
                }
            </div>
        }</SizeMe>
    </>;

};
