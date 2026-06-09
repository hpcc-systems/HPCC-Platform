import * as React from "react";
import { Button, Field, Input, MessageBar, MessageBarActions, MessageBarBody, Spinner } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, useWatch, Controller } from "react-hook-form";
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

    const { handleSubmit, control, reset } = useForm<AddUserFormValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const pwd1 = useWatch({ control, name: "password1" });

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

                WsAccess.AddUser({ request: request })
                    .then(({ AddUserResponse }) => {
                        if (AddUserResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            setErrorMessage(AddUserResponse?.retmsg);
                            logger.error(AddUserResponse?.retmsg);
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
    }, [closeForm, handleSubmit, refreshGrid, reset]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.AddUser} minWidth={400}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Add}</Button>
            <Button onClick={() => { reset(defaultValues); closeForm(); }}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="username"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.UserID} required validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="employeeID"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.EmployeeID} validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
        />
        <Controller
            control={control} name="employeeNumber"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.EmployeeNumber} validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
        />
        <Controller
            control={control} name="firstname"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.FirstName} validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        placeholder={nlsHPCC.PlaceholderFirstName}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
        />
        <Controller
            control={control} name="lastname"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.LastName} validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        placeholder={nlsHPCC.PlaceholderLastName}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
        />
        <Controller
            control={control} name="password1"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.Password} required validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        type="password"
                        autoComplete="off"
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="password2"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.RetypePassword} required validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        type="password"
                        autoComplete="off"
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
            rules={{
                required: nlsHPCC.ValidationErrorRequired,
                validate: value => value === pwd1 || nlsHPCC.PasswordsDoNotMatch
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
