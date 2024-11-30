import * as React from "react";
import { DefaultButton, IDropdownOption, MessageBar, MessageBarType, PrimaryButton, Spinner, } from "@fluentui/react";
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
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Add} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => { reset(defaultValues); closeForm(); }} />
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
                <MessageBar
                    messageBarType={MessageBarType.error} isMultiline={true}
                    onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                    {errorMessage}
                </MessageBar>
            </div>
        }
    </MessageBox>;
};
