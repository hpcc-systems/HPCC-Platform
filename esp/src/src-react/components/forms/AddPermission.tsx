import * as React from "react";
import { IDropdownOption } from "./Fields";
import { Button, Field, Input, MessageBar, MessageBarActions, MessageBarBody, Spinner } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as WsAccess from "src/ws_access";
import { PermissionTypeField } from "./Fields";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddPermission.tsx");

interface AddPermissionFormValues {
    BasednName: string;
    name: string;
    description: string;
}

const defaultValues: AddPermissionFormValues = {
    BasednName: "",
    name: "",
    description: "",
};

interface AddPermissionFormProps {
    refreshGrid?: () => void;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const AddPermissionForm: React.FunctionComponent<AddPermissionFormProps> = ({
    refreshGrid,
    showForm,
    setShowForm
}) => {

    const { handleSubmit, control, reset } = useForm<AddPermissionFormValues>({ defaultValues });
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

                WsAccess.ResourceAdd({ request: request })
                    .then(({ ResourceAddResponse }) => {
                        if (ResourceAddResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            setErrorMessage(ResourceAddResponse?.retmsg);
                            logger.error(ResourceAddResponse?.retmsg);
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

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.AddResource} minWidth={400}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Add}</Button>
            <Button onClick={() => { reset(defaultValues); closeForm(); }}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="BasednName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <PermissionTypeField
                    key={fieldName}
                    required={true}
                    label={nlsHPCC.Type}
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
        <Controller
            control={control} name="name"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.Name} validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
        />
        <Controller
            control={control} name="description"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.Description} validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
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