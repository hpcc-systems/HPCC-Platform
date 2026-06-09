import * as React from "react";
import { Button, Dialog, DialogActions, DialogBody, DialogContent, DialogOpenChangeData, DialogOpenChangeEvent, DialogSurface, DialogTitle, MessageBar, MessageBarActions, MessageBarBody, makeStyles } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { useConst } from "@fluentui/react-hooks";
import { AccountService, WsAccount } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { PasswordStatus } from "../hooks/user";
import nlsHPCC from "src/nlsHPCC";
import { TableGroup } from "./forms/Groups";

const logger = scopedLogger("src-react/components/MyAccount.tsx");

const useStyles = makeStyles({
    surface: {
        minWidth: "640px",
    },
});

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

    const styles = useStyles();
    const [oldPassword, setOldPassword] = React.useState("");
    const [newPassword1, setNewPassword1] = React.useState("");
    const [newPassword2, setNewPassword2] = React.useState("");
    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");
    const [passwordMismatch, setPasswordMismatch] = React.useState("");

    const service = useConst(() => new AccountService({ baseUrl: "" }));

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

    const onOpenChange = React.useCallback((_: DialogOpenChangeEvent, data: DialogOpenChangeData) => {
        if (!data.open) onClose();
    }, [onClose]);

    return <Dialog open={show} modalType="modal" onOpenChange={onOpenChange}>
        <DialogSurface className={styles.surface}>
            <DialogBody>
                <DialogTitle>{currentUser?.username}</DialogTitle>
                <DialogContent>
                    {showError &&
                        <MessageBar intent="error">
                            <MessageBarBody>{errorMessage}</MessageBarBody>
                            <MessageBarActions containerAction={<Button onClick={() => setShowError(false)} aria-label="Close" appearance="transparent" icon={<DismissRegular />} />} />
                        </MessageBar>
                    }
                    <TableGroup fields={{
                        "username": { label: nlsHPCC.Name, type: "string", value: currentUser?.username, readonly: true },
                        "employeeID": { label: nlsHPCC.EmployeeID, type: "string", value: currentUser?.employeeID, readonly: true },
                        "firstname": { label: nlsHPCC.FirstName, type: "string", value: currentUser?.firstName, readonly: true },
                        "lastname": { label: nlsHPCC.LastName, type: "string", value: currentUser?.lastName, readonly: true },
                        "groups": { label: nlsHPCC.Groups, type: "string", value: (currentUser?.Groups?.Group ?? []).join(", "), readonly: true },
                        "accountType": { label: nlsHPCC.AccountType, type: "string", value: currentUser?.accountType, readonly: true },
                        "oldPassword": { label: nlsHPCC.OldPassword, type: "password", value: oldPassword, autoComplete: "current-password", disabled: () => !currentUser?.CanUpdatePassword },
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
                </DialogContent>
                <DialogActions>
                    <Button appearance="primary" onClick={saveUser}>{nlsHPCC.Save}</Button>
                    <Button onClick={() => { resetForm(); onClose(); }}>{nlsHPCC.Cancel}</Button>
                </DialogActions>
            </DialogBody>
        </DialogSurface>
    </Dialog>;
};
