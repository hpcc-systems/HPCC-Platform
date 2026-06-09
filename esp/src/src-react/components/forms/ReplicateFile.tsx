import * as React from "react";
import { IDropdownOption } from "./Fields";
import { Button, Field, Input, Spinner } from "@fluentui/react-components";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as FileSpray from "src/FileSpray";
import { useFile } from "../../hooks/file";
import { MessageBox } from "../../layouts/MessageBox";
import { pushUrl } from "../../util/history";
import { TargetGroupTextField } from "./Fields";

interface ReplicateFileFormValues {
    sourceLogicalName: string;
    replicateOffset: string;
    cluster: string;
}

const defaultValues: ReplicateFileFormValues = {
    sourceLogicalName: "",
    replicateOffset: "1",
    cluster: ""
};

interface ReplicateFileProps {
    cluster: string;
    logicalFile: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const ReplicateFile: React.FunctionComponent<ReplicateFileProps> = ({
    cluster,
    logicalFile,
    showForm,
    setShowForm
}) => {

    const { file } = useFile(cluster, logicalFile);
    const { handleSubmit, control, reset } = useForm<ReplicateFileFormValues>({ defaultValues });
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
                const request = { ...data, srcname: logicalFile };
                FileSpray.Replicate({ request: request }).then(response => {
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                    closeForm();
                    pushUrl(`/dfuworkunits/${response?.ReplicateResponse?.wuid}`);
                });
            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, handleSubmit, logicalFile]);

    React.useEffect(() => {
        const newValues = { ...defaultValues, sourceLogicalName: logicalFile };
        reset(newValues);
    }, [file, logicalFile, reset]);

    return <MessageBox title={nlsHPCC.Rename} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Replicate}</Button>
            <Button onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
        </>}>
        <div style={{ display: "flex", flexDirection: "column" }}>
            <Controller
                control={control} name="sourceLogicalName"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field label={nlsHPCC.SourceLogicalFile} required validationMessage={error?.message}>
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
                control={control} name="replicateOffset"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field label={nlsHPCC.ReplicateOffset} validationMessage={error?.message}>
                        <Input
                            name={fieldName}
                            value={value}
                            onChange={(_, data) => onChange(data.value)}
                        />
                    </Field>}
                rules={{
                    pattern: {
                        value: /^[0-9]+$/i,
                        message: nlsHPCC.ValidationErrorEnterNumber
                    }
                }}
            />
            <Controller
                control={control} name="cluster"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetGroupTextField
                        key={fieldName}
                        label={nlsHPCC.Cluster}
                        required={true}
                        selectedKey={value}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option: IDropdownOption) => {
                            onChange(option.key);
                        }}
                        errorMessage={error && error.message}
                    />}
                rules={{
                    required: `${nlsHPCC.SelectA} ${nlsHPCC.Cluster}`
                }}
            />
        </div>
    </MessageBox>;
};
