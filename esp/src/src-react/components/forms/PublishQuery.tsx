import * as React from "react";
import { Button, Checkbox, Dropdown, Field, Input, Option, Spinner } from "@fluentui/react-components";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";
import { useWorkunit } from "../../hooks/workunit";

const logger = scopedLogger("../components/forms/PublishQuery.tsx");

interface PublishFormValues {
    jobName: string;
    remoteDali: string;
    remoteStorage: string;
    sourceProcess: string;
    comment: string;
    priority: string;
    allowForeignFiles: boolean;
    updateSuperFiles: boolean;
}

const defaultValues: PublishFormValues = {
    jobName: "",
    remoteDali: "",
    remoteStorage: "",
    sourceProcess: "",
    comment: "",
    priority: "",
    allowForeignFiles: true,
    updateSuperFiles: false
};

interface PublishFormProps {
    wuid?: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const PublishQueryForm: React.FunctionComponent<PublishFormProps> = ({
    wuid,
    showForm,
    setShowForm
}) => {

    const { workunit } = useWorkunit(wuid);

    const { handleSubmit, control, reset } = useForm<PublishFormValues>({ defaultValues });
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
                const request = {
                    Wuid: workunit?.Wuid,
                    Cluster: workunit?.Cluster,

                    JobName: data.jobName,
                    RemoteDali: data?.remoteDali ?? "",
                    RemoteStorage: data?.remoteStorage ?? "",
                    Comment: data?.comment ?? "",
                    SourceProcess: data?.sourceProcess ?? "",
                    Priority: data?.priority,
                    AllowForeignFiles: data?.allowForeignFiles,
                    UpdateSuperFiles: data?.updateSuperFiles
                };
                workunit.publishEx(request).then(() => {
                    return workunit.update({ Jobname: data.jobName });
                }).then(() => {
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                    closeForm();
                    reset(defaultValues);
                }).catch(err => logger.error(err));
            },
            logger.info
        )();
    }, [closeForm, handleSubmit, reset, workunit]);

    React.useEffect(() => {
        if (workunit) {
            reset({ ...defaultValues, jobName: workunit?.Jobname });
        }
    }, [reset, workunit]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.Publish}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "flex" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Publish}</Button>
            <Button onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="jobName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field
                label={nlsHPCC.JobName}
                required
                validationMessage={error?.message}
                validationState={error ? "error" : undefined}
            >
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="remoteDali"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field
                label={nlsHPCC.RemoteDali}
                validationMessage={error?.message}
                validationState={error ? "error" : undefined}
            >
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name="remoteStorage"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field
                label={nlsHPCC.RemoteStorage}
                validationMessage={error?.message}
                validationState={error ? "error" : undefined}
            >
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name="sourceProcess"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field
                label={nlsHPCC.SourceProcess}
                validationMessage={error?.message}
                validationState={error ? "error" : undefined}
            >
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name="comment"
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.Comment}>
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name="priority"
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.Priority}>
                    <Dropdown
                        name={fieldName}
                        value={value === "" ? nlsHPCC.None : value === "SLA" ? nlsHPCC.SLA : value === "Low" ? nlsHPCC.Low : nlsHPCC.High}
                        selectedOptions={[value]}
                        onOptionSelect={(_, data) => onChange(data.optionValue)}
                    >
                        <Option value="">{nlsHPCC.None}</Option>
                        <Option value="SLA">{nlsHPCC.SLA}</Option>
                        <Option value="Low">{nlsHPCC.Low}</Option>
                        <Option value="High">{nlsHPCC.High}</Option>
                    </Dropdown>
                </Field>}
        />
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allowForeignFiles"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.AllowForeignFiles} />}
            />
            <br />
            <Controller
                control={control} name="updateSuperFiles"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.UpdateSuperFiles} />}
            />
        </div>
    </MessageBox>;
};