import * as React from "react";
import { Checkbox, DefaultButton, Dropdown, mergeStyleSets, PrimaryButton, Stack, TextField } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import * as FileSpray from "src/FileSpray";
import { TargetDfuSprayQueueTextField, TargetGroupTextField } from "../Fields";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../../layouts/MessageBox";
import { pushUrl } from "../../../util/history";
import * as FormStyles from "./styles";

interface DelimitedImportFormValues {
    destGroup: string;
    DFUServerQueue: string;
    namePrefix: string;
    selectedFiles?: {
        TargetName: string,
        SourceFile: string,
        SourceIP: string
    }[],
    sourceFormat: string;
    sourceMaxRecordSize: string;
    sourceCsvQuote: string;
    sourceCsvEscape: string;
    sourceCsvSeparate: string;
    sourceCsvTerminate: string;
    NoSourceCsvSeparator: boolean;
    overwrite: boolean;
    replicate: boolean;
    nosplit: boolean;
    noCommon: boolean;
    compress: boolean;
    failIfNoSourceFile: boolean;
    recordStructurePresent: boolean;
    quotedTerminator: boolean;
    delayedReplication: boolean;
    expireDays: string;
}

const defaultValues: DelimitedImportFormValues = {
    destGroup: "",
    DFUServerQueue: "",
    namePrefix: "",
    sourceFormat: "1",
    sourceMaxRecordSize: "",
    sourceCsvQuote: "\"",
    sourceCsvEscape: "",
    sourceCsvSeparate: "\,",
    sourceCsvTerminate: "\\n,\\r\\n",
    NoSourceCsvSeparator: false,
    overwrite: false,
    replicate: false,
    nosplit: false,
    noCommon: true,
    compress: false,
    failIfNoSourceFile: false,
    recordStructurePresent: false,
    quotedTerminator: false,
    delayedReplication: true,
    expireDays: ""
};

interface DelimitedImportFormProps {
    formMinWidth?: number;
    showForm: boolean;
    selection: object[];
    setShowForm: (_: boolean) => void;
}

export const DelimitedImportForm: React.FunctionComponent<DelimitedImportFormProps> = ({
    formMinWidth = 300,
    showForm,
    selection,
    setShowForm
}) => {

    const { handleSubmit, control, reset } = useForm<DelimitedImportFormValues>({ defaultValues });

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
                    request["destLogicalName"] = data.namePrefix + ((
                        data.namePrefix && data.namePrefix.substr(-2) !== "::" &&
                        file.TargetName && file.TargetName.substr(0, 2) !== "::"
                    ) ? "::" : "") + file.TargetName;
                    FileSpray.SprayVariable({
                        request: request
                    }).then((response) => {
                        if (response.SprayResponse?.wuid) {
                            pushUrl(`/dfuworkunits/${response.SprayResponse.wuid}`);
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
                    TargetName: file["name"],
                    SourceFile: file["fullPath"],
                    SourceIP: file["NetAddress"]
                };
            });
            reset(newValues);
        }
    }, [selection, reset]);

    return <MessageBox title={nlsHPCC.Import} show={showForm} setShow={closeForm}
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
                control={control} name="namePrefix"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.TargetScope}
                        value={value}
                        placeholder={nlsHPCC.NamePrefixPlaceholder}
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
                        <th>{nlsHPCC.TargetName}</th>
                    </tr>
                </thead>
                <tbody>
                    {selection && selection.map((file, idx) => {
                        return <tr key={`File-${idx}`}>
                            <td>
                                <Controller
                                    control={control} name={`selectedFiles.${idx}.TargetName` as const}
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
                                <input type="hidden" name={`selectedFiles.${idx}.SourceFile` as const} value={file["fullPath"]} />
                                <input type="hidden" name={`selectedFiles.${idx}.SourceIP` as const} value={file["NetAddress"]} />
                            </td>
                        </tr>;
                    })}
                </tbody>
            </table>
        </Stack>
        <Stack>
            <table><tbody>
                <tr>
                    <td><Controller
                        control={control} name="sourceFormat"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Dropdown
                                key="sourceFormat"
                                label={nlsHPCC.Format}
                                options={[
                                    { key: "1", text: "ASCII" },
                                    { key: "2", text: "UTF-8" },
                                    { key: "3", text: "UTF-8N" },
                                    { key: "4", text: "UTF-16" },
                                    { key: "5", text: "UTF-16LE" },
                                    { key: "6", text: "UTF-16BE" },
                                    { key: "7", text: "UTF-32" },
                                    { key: "8", text: "UTF-32LE" },
                                    { key: "9", text: "UTF-32BE" }
                                ]}
                                defaultSelectedKey="1"
                                onChange={(evt, option) => {
                                    onChange(option.key);
                                }}
                                errorMessage={error && error?.message}
                            />}
                        rules={{
                            required: `${nlsHPCC.SelectA} ${nlsHPCC.Format}`
                        }}
                    /></td>
                    <td><Controller
                        control={control} name="sourceMaxRecordSize"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <TextField
                                name={fieldName}
                                onChange={onChange}
                                label={nlsHPCC.MaxRecordLength}
                                value={value}
                                placeholder="8192"
                                errorMessage={error && error?.message}
                            />}
                    /></td>
                </tr>
                <tr>
                    <td><Controller
                        control={control} name="sourceCsvQuote"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <TextField
                                name={fieldName}
                                onChange={onChange}
                                label={nlsHPCC.Quote}
                                value={value}
                                placeholder="'"
                                errorMessage={error && error?.message}
                            />}
                    /></td>
                    <td><Controller
                        control={control} name="sourceCsvEscape"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <TextField
                                name={fieldName}
                                onChange={onChange}
                                label={nlsHPCC.Escape}
                                value={value}
                                errorMessage={error && error?.message}
                            />}
                    /></td>
                </tr>
                <tr>
                    <td><Controller
                        control={control} name="sourceCsvSeparate" defaultValue="\\,"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <TextField
                                name={fieldName}
                                onChange={onChange}
                                label={nlsHPCC.Separators}
                                value={value}
                                placeholder={nlsHPCC.NamePrefixPlaceholder}
                                errorMessage={error && error?.message}
                            />}
                    /></td>
                    <td><Controller
                        control={control} name="sourceCsvTerminate" defaultValue="\\n,\\r\\n"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <TextField
                                name={fieldName}
                                onChange={onChange}
                                label={nlsHPCC.LineTerminators}
                                value={value}
                                placeholder="\\n,\\r\\n"
                                errorMessage={error && error?.message}
                            />}
                    /></td>
                </tr>
            </tbody></table>
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