import * as React from "react";
import { Checkbox, DefaultButton, mergeStyleSets, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as FileSpray from "src/FileSpray";
import { MessageBox } from "../../layouts/MessageBox";
import { pushUrl } from "../../util/history";
import * as FormStyles from "./landing-zone/styles";

interface RenameFileFormValues {
    dstname: string;
    targetRenameFile?: {
        name: string
    }[],
    overwrite: boolean;
}

const defaultValues: RenameFileFormValues = {
    dstname: "",
    overwrite: false
};

interface RenameFileProps {
    logicalFiles: string[];

    showForm: boolean;
    setShowForm: (_: boolean) => void;

    refreshGrid?: (_?: boolean) => void;
}

export const RenameFile: React.FunctionComponent<RenameFileProps> = ({
    logicalFiles,
    showForm,
    setShowForm,
    refreshGrid
}) => {

    const { handleSubmit, control, reset } = useForm<RenameFileFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                if (logicalFiles?.length > 0) {
                    if (logicalFiles?.length === 1) {
                        const request = { ...data, srcname: logicalFiles[0] };
                        FileSpray.Rename({ request: request }).then(response => {
                            closeForm();
                            pushUrl(`/files/${data.dstname}`);
                        });
                    } else {
                        logicalFiles.forEach((logicalFile, idx) => {
                            const request = { ...data, srcname: logicalFile, dstname: data.targetRenameFile[idx].name };
                            const requests = [];
                            requests.push(FileSpray.Rename({ request: request }));
                            Promise.all(requests).then(_ => {
                                closeForm();
                                if (refreshGrid) refreshGrid(true);
                            });
                        });
                    }
                }
            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, handleSubmit, logicalFiles, refreshGrid]);

    const componentStyles = mergeStyleSets(
        FormStyles.componentStyles,
        {
            container: {
                minWidth: 440,
            }
        }
    );

    React.useEffect(() => {
        if (logicalFiles.length === 1) {
            const newValues = { ...defaultValues, dstname: logicalFiles[0] };
            reset(newValues);
        } else if (logicalFiles.length > 1) {
            const _files = [];
            logicalFiles.forEach(file => {
                _files.push({ name: file });
            });
            const newValues = { ...defaultValues, targetRenameFile: _files };
            reset(newValues);
        }
    }, [logicalFiles, reset]);

    return <MessageBox title={nlsHPCC.Rename} show={showForm} setShow={closeForm}
        footer={<>
            <PrimaryButton text={nlsHPCC.Rename} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Stack>
            {logicalFiles?.length === 1 &&
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
            }
            {logicalFiles?.length > 1 &&
                <Stack>
                    <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`}>
                        <thead>
                            <tr>
                                <th>{nlsHPCC.TargetName}</th>
                            </tr>
                        </thead>
                        <tbody>
                            {logicalFiles.map((file, idx) => {
                                return <tr key={`File-${idx}`}>
                                    <td>
                                        <Controller
                                            control={control} name={`targetRenameFile.${idx}.name` as const}
                                            render={({
                                                field: { onChange, name: fieldName, value: file },
                                                fieldState: { error }
                                            }) => <TextField
                                                    name={fieldName}
                                                    onChange={onChange}
                                                    value={file}
                                                    errorMessage={error && error?.message}
                                                />}
                                            rules={{
                                                required: nlsHPCC.ValidationErrorTargetNameRequired
                                            }}
                                        />
                                    </td>
                                </tr>;
                            })}
                        </tbody>
                    </table>
                </Stack>
            }
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
