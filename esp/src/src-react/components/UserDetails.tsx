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
import { UserGroups } from "./UserGroups";
import { pushUrl } from "../util/history";

const logger = scopedLogger("src-react/components/UserDetails.tsx");

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative"
    }
});

interface UserDetailsProps {
    username?: string;
    tab?: string;
}

export const UserDetails: React.FunctionComponent<UserDetailsProps> = ({
    username,
    tab = "summary"
}) => {

    const [, { opsCategory }] = useBuildInfo();

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
                            if (err?.Code < 0) {
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
            .catch(err => logger.error(err))
            ;
    }, [username, setUser]);

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        const account = user?.username ?? username ?? "";
        pushUrl(`/${opsCategory}/security/users/${account}/${data.value as string}`);
    }, [opsCategory, user?.username, username]);

    const styles = useStyles();

    return <SizeMe>{({ size }) =>
        <div className={styles.container}>
            <TabList selectedValue={tab} onTabSelect={onTabSelect} size="medium">
                <Tab value="summary">{user?.username ?? username}</Tab>
                <Tab value="groups">{nlsHPCC.MemberOf}</Tab>
                <Tab value="activePermissions">{nlsHPCC.title_ActivePermissions}</Tab>
                <Tab value="availablePermissions">{nlsHPCC.title_AvailablePermissions}</Tab>
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
                </div>
            }
            {tab === "groups" &&
                <div style={pivotItemStyle(size, 0)}>
                    <UserGroups username={username} />
                </div>
            }
            {tab === "activePermissions" &&
                <div style={pivotItemStyle(size, 0)}>
                    <DojoAdapter widgetClassID="ShowAccountPermissionsWidget" params={{ IsGroup: false, IncludeGroup: true, AccountName: username }} />
                </div>
            }
            {tab === "availablePermissions" &&
                <div style={pivotItemStyle(size, 0)}>
                    <DojoAdapter widgetClassID="PermissionsWidget" params={{ username: username }} />
                </div>
            }
        </div>
    }</SizeMe>;

};