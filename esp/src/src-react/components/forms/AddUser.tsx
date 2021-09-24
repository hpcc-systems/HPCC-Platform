import * as React from "react";
import { DefaultButton, MessageBar, MessageBarType, PrimaryButton, TextField, } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as WsAccess from "src/ws_access";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddUser.tsx");

interface AddUserFormValues {
    username: string;
    employeeID: string;
    employeeNumber: string;
    firstname: string;
    lastname: string;
    password1: string;
    password2: string;
}

const defaultValues: AddUserFormValues = {
    username: "",
    employeeID: "",
    employeeNumber: "",
    firstname: "",
    lastname: "",
    password1: "",
    password2: ""
};

interface AddUserFormProps {
    refreshGrid?: () => void;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const AddUserForm: React.FunctionComponent<AddUserFormProps> = ({
    refreshGrid,
    showForm,
    setShowForm
}) => {

    const { handleSubmit, control, reset, watch } = useForm<AddUserFormValues>({ defaultValues });

    const pwd1 = watch("password1");

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;

                WsAccess.AddUser({ request: request })
                    .then(({ AddUserResponse }) => {
                        if (AddUserResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setErrorMessage(AddUserResponse?.retmsg);
                            logger.error(AddUserResponse?.retmsg);
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
    }, [closeForm, handleSubmit, refreshGrid, reset]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.AddUser} width={400}
        footer={<>
            <PrimaryButton text={nlsHPCC.Add} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => { reset(defaultValues); closeForm(); }} />
        </>}>
        <Controller
            control={control} name="username"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    required={true}
                    label={nlsHPCC.UserID}
                    value={value}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="employeeID"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.EmployeeID}
                    value={value}
                    errorMessage={error && error?.message}
                />}
        />
        <Controller
            control={control} name="employeeNumber"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.EmployeeNumber}
                    value={value}
                    errorMessage={error && error?.message}
                />}
        />
        <Controller
            control={control} name="firstname"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.FirstName}
                    value={value}
                    placeholder={nlsHPCC.PlaceholderFirstName}
                    errorMessage={error && error?.message}
                />}
        />
        <Controller
            control={control} name="lastname"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.LastName}
                    value={value}
                    placeholder={nlsHPCC.PlaceholderLastName}
                    errorMessage={error && error?.message}
                />}
        />
        <Controller
            control={control} name="password1"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    type="password"
                    onChange={onChange}
                    required={true}
                    label={nlsHPCC.Password}
                    value={value}
                    canRevealPassword
                    revealPasswordAriaLabel={nlsHPCC.ShowPassword}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="password2"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    type="password"
                    onChange={onChange}
                    required={true}
                    label={nlsHPCC.RetypePassword}
                    value={value}
                    canRevealPassword
                    revealPasswordAriaLabel={nlsHPCC.ShowPassword}
                    errorMessage={error && error?.message}

                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired,
                validate: value => value === pwd1 || nlsHPCC.PasswordsDoNotMatch
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
