import * as React from "react";
import { CommandBar, ICommandBarItemProps, MessageBar, MessageBarType, Pivot, PivotItem, Sticky, StickyPositionType } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { scopedLogger } from "@hpcc-js/util";
import * as WsAccess from "src/ws_access";
import nlsHPCC from "src/nlsHPCC";
import { pivotItemStyle } from "../layouts/pivot";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { TableGroup } from "./forms/Groups";
import { UserGroups } from "./UserGroups";
import { pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/UserDetails.tsx");

interface UserDetailsProps {
    username?: string;
    tab?: string;
}

export const UserDetails: React.FunctionComponent<UserDetailsProps> = ({
    username,
    tab = "summary"
}) => {

    const [user, setUser] = React.useState<any>();
    const [employeeID, setEmployeeID] = React.useState("");
    const [employeeNumber, setEmployeeNumber] = React.useState("");
    const [firstName, setFirstName] = React.useState("");
    const [lastName, setLastName] = React.useState("");
    const [password1, setPassword1] = React.useState("");
    const [password2, setPassword2] = React.useState("");
    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const canSave = user && (
        employeeID !== user?.employeeID ||
        employeeNumber !== user?.employeeNumber ||
        firstName !== user.firstname ||
        lastName !== user.lastname ||
        (password1 === password2 && password1 !== "")
    );

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                WsAccess.UserInfoEdit({
                    request: {
                        username: username,
                        firstname: firstName,
                        lastname: lastName,
                        employeeID: employeeID,
                        employeeNumber: employeeNumber
                    }
                });

                if (password1 !== "") {
                    WsAccess.UserResetPass({
                        request: {
                            username: username,
                            newPassword: password1,
                            newPasswordRetype: password2
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
        }
    ], [canSave, employeeID, employeeNumber, firstName, lastName, password1, password2, username]);

    React.useEffect(() => {
        WsAccess.UserInfoEditInput({
            request: {
                username: username
            }
        })
            .then(({ UserInfoEditInputResponse }) => {
                setUser(UserInfoEditInputResponse);
                setEmployeeID(UserInfoEditInputResponse.employeeID);
                setEmployeeNumber(UserInfoEditInputResponse.employeeNumber);
                setFirstName(UserInfoEditInputResponse.firstname);
                setLastName(UserInfoEditInputResponse.lastname);
            })
            .catch(logger.error)
            ;
    }, [username, setUser]);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot
            overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
            onLinkClick={evt => {
                pushUrl(`/security/users/${user?.username}/${evt.props.itemKey}`);
            }}
        >
            <PivotItem headerText={user?.username} itemKey="summary" style={pivotItemStyle(size)} >
                <Sticky stickyPosition={StickyPositionType.Header}>
                    <CommandBar items={buttons} />
                </Sticky>
                {showError &&
                    <MessageBar messageBarType={MessageBarType.error} isMultiline={true} onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                        {errorMessage}
                    </MessageBar>
                }
                <TableGroup fields={{
                    "username": { label: nlsHPCC.Name, type: "string", value: username, readonly: true },
                    "employeeID": { label: nlsHPCC.EmployeeID, type: "string", value: employeeID },
                    "employeeNumber": { label: nlsHPCC.EmployeeNumber, type: "string", value: employeeNumber },
                    "firstname": { label: nlsHPCC.FirstName, type: "string", value: firstName },
                    "lastname": { label: nlsHPCC.LastName, type: "string", value: lastName },
                    "password1": { label: nlsHPCC.NewPassword, type: "password", value: password1 },
                    "password2": { label: nlsHPCC.ConfirmPassword, type: "password", value: password2 },
                    "PasswordExpiration": { label: nlsHPCC.PasswordExpiration, type: "string", value: user?.PasswordExpiration, readonly: true },
                }} onChange={(id, value) => {
                    switch (id) {
                        case "employeeID":
                            setEmployeeID(value);
                            break;
                        case "employeeNumber":
                            setEmployeeNumber(value);
                            break;
                        case "firstname":
                            setFirstName(value);
                            break;
                        case "lastname":
                            setLastName(value);
                            break;
                        case "password1":
                            setPassword1(value);
                            break;
                        case "password2":
                            setPassword2(value);
                            break;
                        default:
                            console.log(id, value);
                    }
                }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.MemberOf} itemKey="groups" style={pivotItemStyle(size, 0)}>
                <UserGroups username={username} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.title_ActivePermissions} itemKey="activePermissions" style={pivotItemStyle(size, 0)}>
                <DojoAdapter widgetClassID="ShowAccountPermissionsWidget" params={{ IsGroup: false, IncludeGroup: true, AccountName: username }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.title_AvailablePermissions} itemKey="availablePermissions" style={pivotItemStyle(size, 0)}>
                <DojoAdapter widgetClassID="PermissionsWidget" params={{ username: username }} />
            </PivotItem>
        </Pivot>
    }</SizeMe>;

};