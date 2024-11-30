import * as React from "react";
import { DefaultButton, IDropdownOption, PrimaryButton, Spinner, TextField, } from "@fluentui/react";
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
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Add} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
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
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.Port}
                    value={value}
                    errorMessage={error && error?.message}
                />}
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
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.ServiceName}
                    value={value}
                    errorMessage={error && error?.message}
                />}
        />
    </MessageBox>;
};