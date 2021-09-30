import * as React from "react";
import { DefaultButton, Dialog, DialogFooter, DialogType, MessageBar, MessageBarType, PrimaryButton } from "@fluentui/react";
import { AccountService, WsAccount } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { TableGroup } from "./forms/Groups";
import { useConst } from "@fluentui/react-hooks";

const logger = scopedLogger("src-react/components/MyAccount.tsx");

interface MyAccountProps {
    currentUser: WsAccount.MyAccountResponse;
    show?: boolean;
    onClose?: () => void;
}

export const MyAccount: React.FunctionComponent<MyAccountProps> = ({
    currentUser,
    show = false,
    onClose = () => { }
}) => {

    const [oldPassword, setOldPassword] = React.useState("");
    const [newPassword1, setNewPassword1] = React.useState("");
    const [newPassword2, setNewPassword2] = React.useState("");
    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const service = useConst(() => new AccountService({ baseUrl: "" }));

    const dialogContentProps = React.useMemo(() => {
        return {
            type: DialogType.largeHeader,
            title: currentUser?.username
        };
    }, [currentUser]);

    const saveUser = React.useCallback(() => {
        if (oldPassword !== "" && newPassword1 !== "") {
            service.UpdateUser({
                username: currentUser.username,
                oldpass: oldPassword,
                newpass1: newPassword1,
                newpass2: newPassword2
            })
                .then(response => {
                    if (response?.retcode < 0) {
                        setShowError(true);
                        setErrorMessage(response?.message);
                    } else {
                        setShowError(false);
                        setErrorMessage("");
                    }
                })
                .catch(logger.error)
                ;
        }
    }, [currentUser, newPassword1, newPassword2, oldPassword, service]);

    return <Dialog hidden={!show} onDismiss={onClose} dialogContentProps={dialogContentProps} minWidth="640px">
        {showError &&
            <MessageBar messageBarType={MessageBarType.error} isMultiline={true} onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                {errorMessage}
            </MessageBar>
        }
        <TableGroup fields={{
            "username": { label: nlsHPCC.Name, type: "string", value: currentUser?.username, readonly: true },
            "employeeID": { label: nlsHPCC.EmployeeID, type: "string", value: currentUser?.employeeID, readonly: true },
            "firstname": { label: nlsHPCC.FirstName, type: "string", value: currentUser?.firstName, readonly: true },
            "lastname": { label: nlsHPCC.LastName, type: "string", value: currentUser?.lastName, readonly: true },
            "oldPassword": { label: nlsHPCC.OldPassword, type: "password", value: oldPassword },
            "newPassword1": { label: nlsHPCC.NewPassword, type: "password", value: newPassword1 },
            "newPassword2": { label: nlsHPCC.ConfirmPassword, type: "password", value: newPassword2 },
            "PasswordExpiration": { label: nlsHPCC.PasswordExpiration, type: "string", value: currentUser?.passwordExpiration, readonly: true },
        }} onChange={(id, value) => {
            switch (id) {
                case "oldPassword":
                    setOldPassword(value);
                    break;
                case "newPassword1":
                    setNewPassword1(value);
                    break;
                case "newPassword2":
                    setNewPassword2(value);
                    break;
                default:
                    logger.debug(`${id}: ${value}`);
            }
        }} />
        <DialogFooter>
            <PrimaryButton onClick={saveUser} text={nlsHPCC.Save} />
            <DefaultButton onClick={onClose} text={nlsHPCC.Cancel} />
        </DialogFooter>
    </Dialog>;
};
