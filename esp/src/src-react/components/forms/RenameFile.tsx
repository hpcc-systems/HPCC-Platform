import * as React from "react";
import { Checkbox, DefaultButton, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as FileSpray from "src/FileSpray";
import { useFile } from "../../hooks/file";
import { MessageBox } from "../../layouts/MessageBox";
import { pushUrl } from "../../util/history";

interface RenameFileFormValues {
    dstname: string;
    overwrite: boolean;
}

const defaultValues: RenameFileFormValues = {
    dstname: "",
    overwrite: false
};

interface RenameFileProps {
    cluster: string;
    logicalFile: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const RenameFile: React.FunctionComponent<RenameFileProps> = ({
    cluster,
    logicalFile,
    showForm,
    setShowForm
}) => {

    const [file] = useFile(cluster, logicalFile);

    const { handleSubmit, control, reset } = useForm<RenameFileFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request = { ...data, srcname: logicalFile };
                FileSpray.Rename({ request: request }).then(response => {
                    closeForm();
                    pushUrl(`/files/${cluster}/${data.dstname}`);
                });
            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, cluster, handleSubmit, logicalFile]);

    React.useEffect(() => {
        const newValues = { ...defaultValues, dstname: logicalFile };
        reset(newValues);
    }, [file, logicalFile, reset]);

    return <MessageBox title={nlsHPCC.Rename} show={showForm} setShow={closeForm}
        footer={<>
            <PrimaryButton text={nlsHPCC.Rename} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Stack>
            <Controller
                control={control} name="dstname"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        required={true}
                        label={nlsHPCC.TargetName}
                        value={value}
                        errorMessage={error && error.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <div style={{ paddingTop: "15px" }}>
                <Controller
                    control={control} name="overwrite"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Overwrite} />}
                />
            </div>
        </Stack>
    </MessageBox>;
};
