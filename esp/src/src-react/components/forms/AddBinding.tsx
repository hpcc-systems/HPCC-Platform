import * as React from "react";
import { IDropdownOption } from "./Fields";
import { Button, Field, Input, Spinner } from "@fluentui/react-components";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import { EsdlDefinitionsTextField, EsdlEspProcessesTextField } from "./Fields";
import nlsHPCC from "src/nlsHPCC";
import * as WsESDLConfig from "src/WsESDLConfig";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddBinding.tsx");

interface AddBindingFormValues {
    EspProcName: string;
    EspPort: string;
    EsdlDefinitionID: string;
    EsdlServiceName: string;
}

const defaultValues: AddBindingFormValues = {
    EspProcName: "",
    EspPort: "",
    EsdlDefinitionID: "",
    EsdlServiceName: ""
};

interface AddBindingFormProps {
    minWidth?: number;

    refreshGrid?: () => void;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const AddBindingForm: React.FunctionComponent<AddBindingFormProps> = ({
    minWidth = 300,
    refreshGrid,
    showForm,
    setShowForm
}) => {

    const { handleSubmit, control, reset } = useForm<AddBindingFormValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);

                const request: any = data;
                request.Overwrite = true;

                WsESDLConfig.PublishESDLBinding({ request: request })
                    .then(() => {
                        setSubmitDisabled(false);
                        setSpinnerHidden(true);
                        closeForm();
                        reset(defaultValues);
                        if (refreshGrid) refreshGrid();
                    })
                    .catch(err => logger.error(err))
                    ;
            },
            logger.info
        )();
    }, [closeForm, handleSubmit, refreshGrid, reset]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.AddBinding}
        minWidth={minWidth}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Add}</Button>
            <Button onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="EspProcName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <EsdlEspProcessesTextField
                    key={fieldName}
                    onChange={(evt, option: IDropdownOption) => {
                        onChange(option.key);
                    }}
                    required={true}
                    label={nlsHPCC.ESPProcessName}
                    selectedKey={value}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="EspPort"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.Port} validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
        />
        <Controller
            control={control} name="EsdlDefinitionID"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <EsdlDefinitionsTextField
                    key={fieldName}
                    onChange={(evt, option: IDropdownOption) => {
                        onChange(option.key);
                    }}
                    required={true}
                    label={nlsHPCC.SourceProcess}
                    selectedKey={value}
                    errorMessage={error && error?.message}
                />}
        />
        <Controller
            control={control} name="EsdlServiceName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.ServiceName} validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
        />
    </MessageBox>;
};