import * as React from "react";
import { DefaultButton, Dialog, DialogFooter, DialogType, MessageBar, MessageBarType, PrimaryButton } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { AccountService, WsAccount } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { PasswordStatus } from "../hooks/user";
import nlsHPCC from "src/nlsHPCC";
import { TableGroup } from "./forms/Groups";

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
    const [passwordMismatch, setPasswordMismatch] = React.useState("");

    const service = useConst(() => new AccountService({ baseUrl: "" }));

    const dialogContentProps = React.useMemo(() => {
        return {
            type: DialogType.largeHeader,
            title: currentUser?.username
        };
    }, [currentUser]);

    const resetForm = React.useCallback(() => {
        setOldPassword("");
        setNewPassword1("");
        setNewPassword2("");
    }, []);

    const saveUser = React.useCallback(() => {
        if (currentUser?.CanUpdatePassword && oldPassword !== "" && newPassword1 !== "") {
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
                        resetForm();
                        onClose();
                    }
                })
                .catch(err => logger.error(err))
                ;
        }
    }, [currentUser, newPassword1, newPassword2, oldPassword, onClose, resetForm, service]);

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
            "oldPassword": { label: nlsHPCC.OldPassword, type: "password", value: oldPassword, disabled: () => !currentUser?.CanUpdatePassword },
            "newPassword1": { label: nlsHPCC.NewPassword, type: "password", value: newPassword1, disabled: () => !currentUser?.CanUpdatePassword },
            "newPassword2": { label: nlsHPCC.ConfirmPassword, type: "password", value: newPassword2, errorMessage: passwordMismatch, disabled: () => !currentUser?.CanUpdatePassword },
            "PasswordExpiration": { label: nlsHPCC.PasswordExpiration, type: "string", value: currentUser?.passwordDaysRemaining === PasswordStatus.NeverExpires ? nlsHPCC.PasswordNeverExpires : currentUser?.passwordExpiration, readonly: true },
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
                    if (value && value !== newPassword1) {
                        setPasswordMismatch(nlsHPCC.PasswordsDoNotMatch);
                    } else {
                        setPasswordMismatch("");
                    }
                    break;
                default:
                    logger.debug(`${id}: ${value}`);
            }
        }} />
        <DialogFooter>
            <PrimaryButton onClick={saveUser} text={nlsHPCC.Save} />
            <DefaultButton onClick={() => { resetForm(); onClose(); }} text={nlsHPCC.Cancel} />
        </DialogFooter>
    </Dialog>;
};
