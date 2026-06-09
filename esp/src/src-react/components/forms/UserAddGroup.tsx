import * as React from "react";
import { IDropdownOption } from "./Fields";
import { Button, MessageBar, MessageBarActions, MessageBarBody, Spinner } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as WsAccess from "src/ws_access";
import { UserGroupsField } from "./Fields";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddUser.tsx");

interface UserAddGroupValues {
    groupnames: string;
}

const defaultValues: UserAddGroupValues = {
    groupnames: ""
};

interface UserAddGroupProps {
    refreshGrid?: () => void;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
    username: string;
}

export const UserAddGroupForm: React.FunctionComponent<UserAddGroupProps> = ({
    refreshGrid,
    showForm,
    setShowForm,
    username
}) => {

    const { handleSubmit, control, reset } = useForm<UserAddGroupValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);
                const request: any = data;
                request.username = username;
                request.action = "add";

                WsAccess.UserGroupEdit({ request: request })
                    .then(({ UserGroupEditResponse }) => {
                        if (UserGroupEditResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            setErrorMessage(UserGroupEditResponse?.retmsg);
                            logger.error(UserGroupEditResponse?.retmsg);
                        } else {
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            closeForm();
                            reset(defaultValues);
                            if (refreshGrid) refreshGrid();
                        }
                    })
                    .catch(err => logger.error(err))
                    ;
            },
            logger.info
        )();
    }, [closeForm, handleSubmit, refreshGrid, reset, username]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.PleaseSelectAGroupToAddUser} minWidth={400}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Add}</Button>
            <Button onClick={() => { reset(defaultValues); closeForm(); }}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="groupnames"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <UserGroupsField
                    key={fieldName}
                    username={username}
                    required={true}
                    label={nlsHPCC.GroupName}
                    selectedKey={value}
                    onChange={(evt, option: IDropdownOption) => {
                        onChange(option.key);
                    }}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        {showError &&
            <div style={{ marginTop: 16 }}>
                <MessageBar intent="error">
                    <MessageBarBody>{errorMessage}</MessageBarBody>
                    <MessageBarActions containerAction={<Button onClick={() => setShowError(false)} aria-label="Close" appearance="transparent" icon={<DismissRegular />} />} />
                </MessageBar>
            </div>
        }
    </MessageBox>;
};
