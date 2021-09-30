import * as React from "react";
import { DefaultButton, MessageBar, MessageBarType, PrimaryButton, } from "@fluentui/react";
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

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;
                request.username = username;
                request.action = "add";

                WsAccess.UserGroupEdit({ request: request })
                    .then(({ UserGroupEditResponse }) => {
                        if (UserGroupEditResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setErrorMessage(UserGroupEditResponse?.retmsg);
                            logger.error(UserGroupEditResponse?.retmsg);
                        } else {
                            closeForm();
                            reset(defaultValues);
                            if (refreshGrid) refreshGrid();
                        }
                    })
                    .catch(logger.error)
                    ;
            },
            logger.info
        )();
    }, [closeForm, handleSubmit, refreshGrid, reset, username]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.PleaseSelectAGroupToAddUser} width={400}
        footer={<>
            <PrimaryButton text={nlsHPCC.Add} onClick={handleSubmit(onSubmit)} />
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
                    defaultSelectedKey={value}
                    onChange={(evt, option) => {
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
