import * as React from "react";
import { Checkbox, DefaultButton, Dropdown, PrimaryButton, Spinner, TextField, } from "@fluentui/react";
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

    const [workunit] = useWorkunit(wuid);

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
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Publish} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Controller
            control={control} name="jobName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    required={true}
                    label={nlsHPCC.JobName}
                    value={value}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="remoteDali"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.RemoteDali}
                    value={value}
                    errorMessage={error && error?.message}
                />}
        />
        <Controller
            control={control} name="remoteStorage"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.RemoteStorage}
                    value={value}
                    errorMessage={error && error?.message}
                />}
        />
        <Controller
            control={control} name="sourceProcess"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.SourceProcess}
                    value={value}
                    errorMessage={error && error?.message}
                />}
        />
        <Controller
            control={control} name="comment"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.Comment}
                    value={value}
                />}
        />
        <Controller
            control={control} name="priority"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Dropdown
                    key={fieldName}
                    label={nlsHPCC.Priority}
                    options={[
                        { key: "", text: nlsHPCC.None },
                        { key: "SLA", text: nlsHPCC.SLA },
                        { key: "Low", text: nlsHPCC.Low },
                        { key: "High", text: nlsHPCC.High },
                    ]}
                    selectedKey={value}
                    onChange={(evt, option) => {
                        onChange(option.key);
                    }}
                />}
        />
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allowForeignFiles"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.AllowForeignFiles} />}
            />
        </div>
        <div style={{ paddingTop: "10px" }}>
            <Controller
                control={control} name="updateSuperFiles"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.UpdateSuperFiles} />}
            />
        </div>
    </MessageBox>;
};