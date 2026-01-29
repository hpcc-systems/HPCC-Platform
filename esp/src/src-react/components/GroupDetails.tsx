import * as React from "react";
import { CommandBar, ICommandBarItemProps, MessageBar, MessageBarType, Sticky, StickyPositionType } from "@fluentui/react";
import { SelectTabData, SelectTabEvent, Tab, TabList, makeStyles } from "@fluentui/react-components";
import { SizeMe } from "../layouts/SizeMe";
import { scopedLogger } from "@hpcc-js/util";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { useBuildInfo } from "../hooks/platform";
import { pivotItemStyle } from "../layouts/pivot";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { TableGroup } from "./forms/Groups";
import { GroupMembers } from "./GroupMembers";
import { pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/GroupDetails.tsx");

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative"
    }
});

interface GroupDetailsProps {
    name: string;
    tab?: string;
}

export const GroupDetails: React.FunctionComponent<GroupDetailsProps> = ({
    name,
    tab = "summary"
}) => {

    const [, { opsCategory }] = useBuildInfo();

    const [groupName, setGroupName] = React.useState(name);
    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const canSave = groupName && (name !== groupName);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                //  Note from dojo component --> Currently disabled.  TODO:  Add ESP Method to rename group?  ---
                WsAccess.GroupEdit({
                    request: {
                        name: groupName,
                    }
                })
                    .then(({ Exceptions }) => {
                        const err = Exceptions?.Exception[0];
                        if (err.Code < 0) {
                            setShowError(true);
                            setErrorMessage(err.Message);
                        } else {
                            setShowError(false);
                            setErrorMessage("");
                        }
                    })
                    .catch(err => logger.error(err))
                    ;
            }
        }
    ], [canSave, groupName]);

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        pushUrl(`/${opsCategory}/security/groups/${groupName}/${data.value as string}`);
    }, [groupName, opsCategory]);

    const styles = useStyles();

    return <SizeMe>{({ size }) =>
        <div className={styles.container}>
            <TabList selectedValue={tab} onTabSelect={onTabSelect} size="medium">
                <Tab value="summary">{groupName}</Tab>
                <Tab value="members">{nlsHPCC.Members}</Tab>
                <Tab value="activePermissions">{nlsHPCC.title_ActiveGroupPermissions}</Tab>
                <Tab value="availablePermissions">{nlsHPCC.title_AvailableGroupPermissions}</Tab>
            </TabList>
            {tab === "summary" &&
                <div style={pivotItemStyle(size)}>
                    <Sticky stickyPosition={StickyPositionType.Header}>
                        <CommandBar items={buttons} />
                    </Sticky>
                    {showError &&
                        <MessageBar messageBarType={MessageBarType.error} isMultiline={true} onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                            {errorMessage}
                        </MessageBar>
                    }
                    <h3 style={{ margin: "0.5em 0 0 0" }}>
                        <span className="bold">{nlsHPCC.ContactAdmin}</span>
                    </h3>
                    <TableGroup fields={{
                        "name": { label: nlsHPCC.Name, type: "string", value: groupName, readonly: true },
                    }} onChange={(id, value) => {
                        switch (id) {
                            case "name":
                                setGroupName(value);
                                break;
                            default:
                                console.log(id, value);
                        }
                    }} />
                </div>
            }
            {tab === "members" &&
                <div style={pivotItemStyle(size, 0)}>
                    <GroupMembers groupname={groupName} />
                </div>
            }
            {tab === "activePermissions" &&
                <div style={pivotItemStyle(size, 0)}>
                    <DojoAdapter widgetClassID="ShowAccountPermissionsWidget" params={{ IsGroup: true, IncludeGroup: false, AccountName: name }} />
                </div>
            }
            {tab === "availablePermissions" &&
                <div style={pivotItemStyle(size, 0)}>
                    <DojoAdapter widgetClassID="PermissionsWidget" params={{ IsGroup: true, IncludeGroup: false, groupname: name }} />
                </div>
            }
        </div>
    }</SizeMe>;

};
