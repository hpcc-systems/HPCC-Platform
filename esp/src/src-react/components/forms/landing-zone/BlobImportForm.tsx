import * as React from "react";
import { Checkbox, DefaultButton, mergeStyleSets, PrimaryButton, Stack, TextField } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import * as FileSpray from "src/FileSpray";
import { TargetDfuSprayQueueTextField, TargetGroupTextField } from "../Fields";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../../layouts/MessageBox";
import * as FormStyles from "./styles";
import { pushUrl } from "../../../util/history";

interface BlobImportFormValues {
    destGroup: string;
    DFUServerQueue: string;
    destLogicalName: string;
    selectedFiles?: {
        TargetName: string,
        SourceFile: string,
        SourceIP: string
    }[],
    prefix: string;
    overwrite: boolean;
    replicate: boolean;
    nosplit: boolean;
    noCommon: boolean;
    compress: boolean;
    failIfNoSourceFile: boolean;
    delayedReplication: boolean;
    expireDays: string;
}

const defaultValues: BlobImportFormValues = {
    destGroup: "",
    DFUServerQueue: "",
    destLogicalName: "",
    prefix: "",
    overwrite: false,
    replicate: false,
    nosplit: false,
    noCommon: true,
    compress: false,
    failIfNoSourceFile: false,
    delayedReplication: true,
    expireDays: ""
};

interface BlobImportFormProps {
    formMinWidth?: number;
    showForm: boolean;
    selection: object[];
    setShowForm: (_: boolean) => void;
}

export const BlobImportForm: React.FunctionComponent<BlobImportFormProps> = ({
    formMinWidth = 300,
    showForm,
    selection,
    setShowForm
}) => {

    const { handleSubmit, control, reset } = useForm<BlobImportFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
        reset(defaultValues);
    }, [reset, setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                let request = {};
                const files = data.selectedFiles;

                delete data.selectedFiles;

                files.forEach(file => {
                    request = data;
                    request["sourceIP"] = file.SourceIP;
                    request["sourcePath"] = file.SourceFile;
                    FileSpray.SprayFixed({
                        request: request
                    }).then((response) => {
                        if (response.SprayFixedResponse?.wuid) {
                            pushUrl(`/dfuworkunits/${response.SprayFixedResponse.wuid}`);
                        }
                    });
                });
            },
            err => {
                console.log(err);
            }
        )();
    }, [handleSubmit]);

    const componentStyles = mergeStyleSets(
        FormStyles.componentStyles,
        {
            container: {
                minWidth: formMinWidth ? formMinWidth : 300,
            }
        }
    );

    React.useEffect(() => {
        if (selection) {
            const newValues = defaultValues;
            newValues.selectedFiles = [];
            selection.forEach((file, idx) => {
                newValues.selectedFiles[idx] = {
                    TargetName: "",
                    SourceFile: file["fullPath"],
                    SourceIP: file["NetAddress"]
                };
            });
            reset(newValues);
        }
    }, [selection, reset]);

    return <MessageBox title={`${nlsHPCC.Import} ${nlsHPCC.Blob}`} show={showForm} setShow={closeForm}
        footer={<>
            <PrimaryButton text={nlsHPCC.Import} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Stack>
            <Controller
                control={control} name="destGroup"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetGroupTextField
                        key="destGroup"
                        label={nlsHPCC.Group}
                        required={true}
                        optional={true}
                        selectedKey={value}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: `${nlsHPCC.SelectA} ${nlsHPCC.Group}`
                }}
            />
            <Controller
                control={control} name="DFUServerQueue"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetDfuSprayQueueTextField
                        key="DFUServerQueue"
                        label={nlsHPCC.Queue}
                        required={true}
                        optional={true}
                        selectedKey={value}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: `${nlsHPCC.SelectA} ${nlsHPCC.Queue}`
                }}
            />
            <Controller
                control={control} name="destLogicalName"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.TargetScope}
                        value={value}
                        placeholder={nlsHPCC.TargetNamePlaceholder}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    pattern: {
                        value: /^([a-z0-9]+(::)?)+$/i,
                        message: nlsHPCC.ValidationErrorNamePrefix
                    }
                }}
            />
        </Stack>
        <Stack>
            <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`}>
                <thead>
                    <tr>
                        <th>{nlsHPCC.SourcePath}</th>
                    </tr>
                </thead>
                <tbody>
                    {selection && selection.map((file, idx) => {
                        return <tr key={`File-${idx}`}>
                            <td>
                                <Controller
                                    control={control} name={`selectedFiles.${idx}.SourceFile` as const}
                                    render={({
                                        field: { onChange, name: fieldName, value },
                                        fieldState: { error }
                                    }) => <TextField
                                            name={fieldName}
                                            onChange={onChange}
                                            value={value}
                                            errorMessage={error && error?.message}
                                        />}
                                    rules={{
                                        required: nlsHPCC.ValidationErrorTargetNameRequired,
                                        pattern: {
                                            value: /^([a-z0-9]+[-a-z0-9 \._]+)+$/i,
                                            message: nlsHPCC.ValidationErrorTargetNameInvalid
                                        }
                                    }}
                                />
                                <input type="hidden" name={`selectedFiles.${idx}.SourceIP` as const} value={file["NetAddress"]} />
                            </td>
                        </tr>;
                    })}
                </tbody>
            </table>
        </Stack>
        <Stack>
            <Controller
                control={control} name="prefix"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        value={value}
                        label={nlsHPCC.BlobPrefix}
                        placeholder={nlsHPCC.PrefixPlaceholder}
                        errorMessage={error && error?.message}
                    />}
            />
        </Stack>
        <Stack>
            <table className={componentStyles.twoColumnTable}>
                <tbody><tr>
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
                            control={control} name="noCommon"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.NoCommon} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="compress"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Compress} />}
                        /></td>
                        <td><Controller
                            control={control} name="failIfNoSourceFile"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.FailIfNoSourceFile} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="expireDays"
                            render={({
                                field: { onChange, name: fieldName, value },
                                fieldState: { error }
                            }) => <TextField
                                    name={fieldName}
                                    onChange={onChange}
                                    label={nlsHPCC.ExpireDays}
                                    value={value}
                                    errorMessage={error && error?.message}
                                />}
                            rules={{
                                min: {
                                    value: 1,
                                    message: nlsHPCC.ValidationErrorExpireDaysMinimum
                                }
                            }}
                        /></td>
                        <td><Controller
                            control={control} name="delayedReplication"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.DelayedReplication} disabled={true} />}
                        /></td>
                    </tr></tbody>
            </table>
        </Stack>
    </MessageBox>;
};