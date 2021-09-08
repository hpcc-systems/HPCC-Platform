import * as React from "react";
import { CommandBar, ICommandBarItemProps, MessageBar, MessageBarType, Pivot, PivotItem, Sticky, StickyPositionType } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { scopedLogger } from "@hpcc-js/util";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { pivotItemStyle } from "../layouts/pivot";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { TableGroup } from "./forms/Groups";
import { GroupMembers } from "./GroupMembers";
import { pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/GroupDetails.tsx");

interface GroupDetailsProps {
    name: string;
    tab?: string;
}

export const GroupDetails: React.FunctionComponent<GroupDetailsProps> = ({
    name,
    tab = "summary"
}) => {

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
                    .catch(logger.error)
                    ;
            }
        }
    ], [canSave, groupName]);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot
            overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
            onLinkClick={evt => {
                pushUrl(`/security/groups/${groupName}/${evt.props.itemKey}`);
            }}
        >
            <PivotItem headerText={groupName} itemKey="summary" style={pivotItemStyle(size)} >
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
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Members} itemKey="members" style={pivotItemStyle(size, 0)}>
                <GroupMembers groupname={groupName} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.title_ActiveGroupPermissions} itemKey="activePermissions" style={pivotItemStyle(size, 0)}>
                <DojoAdapter widgetClassID="ShowAccountPermissionsWidget" params={{ IsGroup: true, IncludeGroup: false, AccountName: name }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.title_AvailableGroupPermissions} itemKey="availablePermissions" style={pivotItemStyle(size, 0)}>
                <DojoAdapter widgetClassID="PermissionsWidget" params={{ IsGroup: true, IncludeGroup: false, groupname: name }} />
            </PivotItem>
        </Pivot>
    }</SizeMe>;

};
