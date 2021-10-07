import * as React from "react";
import { DefaultButton, MessageBar, MessageBarType, PrimaryButton, TextField, } from "@fluentui/react";
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

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;

                WsAccess.ResourceAdd({ request: request })
                    .then(({ ResourceAddResponse }) => {
                        if (ResourceAddResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setErrorMessage(ResourceAddResponse?.retmsg);
                            logger.error(ResourceAddResponse?.retmsg);
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
            control={control} name="BasednName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <PermissionTypeField
                    key={fieldName}
                    required={true}
                    label={nlsHPCC.Type}
                    selectedKey={value}
                    onChange={(evt, option) => {
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
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.Name}
                    value={value}
                    errorMessage={error && error?.message}
                />}
        />
        <Controller
            control={control} name="description"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.Description}
                    value={value}
                    errorMessage={error && error?.message}
                />}
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