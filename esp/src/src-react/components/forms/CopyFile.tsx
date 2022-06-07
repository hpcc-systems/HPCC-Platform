import * as React from "react";
import { Checkbox, DefaultButton, mergeStyleSets, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as FileSpray from "src/FileSpray";
import { MessageBox } from "../../layouts/MessageBox";
import { pushUrl } from "../../util/history";
import { TargetGroupTextField } from "./Fields";
import * as FormStyles from "./landing-zone/styles";

interface CopyFileFormValues {
    destGroup: string;
    destLogicalName: string;
    targetCopyName?: {
        name: string
    }[];
    overwrite: boolean;
    replicate: boolean;
    nosplit: boolean;
    compress: boolean;
    Wrap: boolean;
    superCopy: boolean;
    preserveCompression: boolean;
    ExpireDays: string;
    maxConnections: string;
}

const defaultValues: CopyFileFormValues = {
    destGroup: "",
    destLogicalName: "",
    overwrite: false,
    replicate: false,
    nosplit: false,
    compress: false,
    Wrap: false,
    superCopy: false,
    preserveCompression: true,
    ExpireDays: "",
    maxConnections: ""
};

interface CopyFileProps {
    logicalFiles: string[];

    showForm: boolean;
    setShowForm: (_: boolean) => void;

    refreshGrid?: () => void;
}

export const CopyFile: React.FunctionComponent<CopyFileProps> = ({
    logicalFiles,
    showForm,
    setShowForm,
    refreshGrid
}) => {

    const { handleSubmit, control, reset } = useForm<CopyFileFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                if (logicalFiles.length > 0) {
                    if (logicalFiles.length === 1) {
                        const request = { ...data, sourceLogicalName: logicalFiles[0] };
                        FileSpray.Copy({ request: request }).then(response => {
                            closeForm();
                            pushUrl(`/files/${data.destGroup}/${request.destLogicalName}`);
                        });
                    } else {
                        logicalFiles.forEach((logicalFile, idx) => {
                            const request = { ...data, sourceLogicalName: logicalFile, destLogicalName: data.targetCopyName[idx].name };
                            const requests = [];
                            requests.push(FileSpray.Copy({ request: request }));
                            Promise.all(requests).then(_ => {
                                closeForm();
                                if (refreshGrid) refreshGrid();
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
        if (logicalFiles?.length === 1) {
            const newValues = { ...defaultValues, destLogicalName: logicalFiles[0] };
            reset(newValues);
        } else if (logicalFiles?.length > 1) {
            const _files = [];
            logicalFiles.forEach(file => {
                _files.push({ name: file });
            });
            const newValues = { ...defaultValues, targetCopyName: _files };
            reset(newValues);
        }
    }, [logicalFiles, reset]);

    return <MessageBox title={nlsHPCC.Copy} show={showForm} setShow={closeForm}
        footer={<>
            <PrimaryButton text={nlsHPCC.Copy} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Stack>
            <Controller
                control={control} name="destGroup"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetGroupTextField
                        key={fieldName}
                        label={nlsHPCC.Group}
                        required={true}
                        selectedKey={value}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                        errorMessage={error && error.message}
                    />}
                rules={{
                    required: `${nlsHPCC.SelectA} ${nlsHPCC.Group}`
                }}
            />
            {logicalFiles.length === 1 &&
                <Controller
                    control={control} name="destLogicalName"
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
        </Stack>
        {logicalFiles.length > 1 &&
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
                                        control={control} name={`targetCopyName.${idx}.name` as const}
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
        <Stack>
            <table className={componentStyles.twoColumnTable}>
                <tbody>
                    <tr>
                        <td><Controller
                            control={control} name="overwrite"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Overwrite} />}
                        /></td>
                        <td><Controller
                            control={control} name="replicate"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Replicate} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="nosplit"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.NoSplit} />}
                        /></td>
                        <td><Controller
                            control={control} name="compress"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Compress} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="Wrap"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Wrap} />}
                        /></td>
                        <td><Controller
                            control={control} name="superCopy"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.RetainSuperfileStructure} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="preserveCompression"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.PreserveCompression} />}
                        /></td>
                        <td></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="ExpireDays" defaultValue={""}
                            render={({
                                field: { onChange, name: fieldName, value },
                                fieldState: { error }
                            }) => <TextField
                                    name={fieldName}
                                    onChange={onChange}
                                    label={nlsHPCC.ExpireDays}
                                    value={value}
                                    errorMessage={error && error.message}
                                />}
                            rules={{
                                min: {
                                    value: 1,
                                    message: `${nlsHPCC.ValidationErrorNumberLess} 1`
                                }
                            }}
                        /></td>
                        <td><Controller
                            control={control} name="maxConnections" defaultValue={""}
                            render={({
                                field: { onChange, name: fieldName, value },
                                fieldState: { error }
                            }) => <TextField
                                    name={fieldName}
                                    onChange={onChange}
                                    label={nlsHPCC.MaxConnections}
                                    value={value}
                                    errorMessage={error && error.message}
                                />}
                            rules={{
                                min: {
                                    value: 1,
                                    message: `${nlsHPCC.ValidationErrorNumberLess} 1`
                                }
                            }}
                        /></td>
                    </tr>
                </tbody>
            </table>
        </Stack>
    </MessageBox>;
};
