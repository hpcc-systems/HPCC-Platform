import * as React from "react";
import { IDropdownOption } from "../Fields";
import { Button, Checkbox, Dropdown, Field, Input, Option, Spinner, Tooltip } from "@fluentui/react-components";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import * as FileSpray from "src/FileSpray";
import { TargetDfuSprayQueueTextField, TargetGroupTextField } from "../Fields";
import nlsHPCC from "src/nlsHPCC";
import { useBuildInfo } from "../../../hooks/platform";
import { MessageBox } from "../../../layouts/MessageBox";
import { pushUrl } from "../../../util/history";
import * as FormStyles from "./styles";

const logger = scopedLogger("src-react/components/forms/landing-zone/DelimitedImportForm.tsx");

interface DelimitedImportFormValues {
    destGroup: string;
    DFUServerQueue: string;
    namePrefix: string;
    selectedFiles?: {
        TargetName: string,
        NumParts: string,
        SourceFile: string,
        SourcePlane: string,
        SourceIP: string,
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

    const [, { isContainer }] = useBuildInfo();

    const { handleSubmit, control, reset } = useForm<DelimitedImportFormValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
        reset(defaultValues);
    }, [reset, setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);
                let request = {};
                const files = data.selectedFiles;

                delete data.selectedFiles;

                const requests = [];

                files.forEach(file => {
                    request = data;
                    if (!isContainer) {
                        request["sourceIP"] = file.SourceIP;
                    }
                    request["sourcePlane"] = file.SourcePlane;
                    request["sourcePath"] = file.SourceFile;
                    request["destLogicalName"] = data.namePrefix + ((
                        data.namePrefix && data.namePrefix.substring(-2) !== "::" &&
                        file.TargetName && file.TargetName.substring(0, 2) !== "::"
                    ) ? "::" : "") + file.TargetName;
                    request["destNumParts"] = file.NumParts;
                    requests.push(FileSpray.SprayVariable({
                        request: request
                    }));
                });

                Promise.all(requests).then(responses => {
                    if (responses.length === 1) {
                        const response = responses[0];
                        if (response?.Exceptions) {
                            const err = response.Exceptions.Exception[0].Message;
                            logger.error(err);
                        } else if (response.SprayResponse?.wuid) {
                            pushUrl(`#/dfuworkunits/${response.SprayResponse.wuid}`);
                        }
                    } else {
                        const errors = [];
                        responses.forEach(response => {
                            if (response?.Exceptions) {
                                const err = response.Exceptions.Exception[0].Message;
                                errors.push(err);
                                logger.error(err);
                            } else if (response.SprayResponse?.wuid) {
                                window.open(`#/dfuworkunits/${response.SprayResponse.wuid}`);
                            }
                        });
                        if (errors.length === 0) {
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            closeForm();
                        }
                    }
                }).catch(err => logger.error(err));
            },
            err => {
                logger.error(err);
            }
        )();
    }, [closeForm, handleSubmit, isContainer]);

    const componentStyles = FormStyles.useComponentStyles();

    React.useEffect(() => {
        if (selection) {
            const newValues = defaultValues;
            newValues.selectedFiles = [];
            selection.forEach((file: { [id: string]: any }, idx) => {
                newValues.selectedFiles[idx] = {
                    TargetName: file["name"],
                    NumParts: file["NumParts"],
                    SourceFile: file["fullPath"],
                    SourcePlane: file?.DropZone?.Name ?? "",
                    SourceIP: file["NetAddress"]
                };
            });
            reset(newValues);
        }
    }, [selection, reset]);

    return <MessageBox title={nlsHPCC.Import} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Import}</Button>
            <Button onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
        </>}>
        <div style={{ display: "flex", flexDirection: "column" }}>
            <Controller
                control={control} name="destGroup"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetGroupTextField
                        key="destGroup"
                        label={isContainer ? nlsHPCC.TargetPlane : nlsHPCC.Group}
                        required={true}
                        selectedKey={value}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option: IDropdownOption) => {
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
                        selectedKey={value}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option: IDropdownOption) => {
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
                }) => <Field label={nlsHPCC.TargetScope} validationMessage={error?.message}>
                        <Input
                            name={fieldName}
                            value={value}
                            placeholder={nlsHPCC.NamePrefixPlaceholder}
                            onChange={(_, data) => onChange(data.value)}
                        />
                    </Field>}
                rules={{
                    pattern: {
                        value: /^[_a-z0-9]+(::[_a-z0-9]+)*(?:::)?$/i,
                        message: nlsHPCC.ValidationErrorNamePrefix
                    }
                }}
            />
        </div>
        <div style={{ display: "flex", flexDirection: "column" }}>
            <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`}>
                <thead>
                    <tr>
                        <th>{nlsHPCC.TargetName}</th>
                        <th>{nlsHPCC.NumberofParts}</th>
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
                                    }) => <Field validationMessage={error?.message}>
                                            <Input
                                                name={fieldName}
                                                value={value}
                                                onChange={(_, data) => onChange(data.value)}
                                            />
                                        </Field>}
                                    rules={{
                                        required: nlsHPCC.ValidationErrorTargetNameRequired,
                                        pattern: {
                                            value: /^[-a-z0-9_]+[-a-z0-9 _\.]+$/i,
                                            message: nlsHPCC.ValidationErrorTargetNameInvalid
                                        }
                                    }}
                                />
                            </td>
                            <td>
                                <Controller
                                    control={control} name={`selectedFiles.${idx}.NumParts` as const}
                                    render={({
                                        field: { onChange, name: fieldName, value },
                                        fieldState: { error }
                                    }) => <Field validationMessage={error?.message}>
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
                                <input type="hidden" name={`selectedFiles.${idx}.SourceFile` as const} value={file["fullPath"]} />
                                <input type="hidden" name={`selectedFiles.${idx}.SourceIP` as const} value={file["NetAddress"]} />
                            </td>
                        </tr>;
                    })}
                </tbody>
            </table>
        </div>
        <div style={{ display: "flex", flexDirection: "column" }}>
            <table><tbody>
                <tr>
                    <td><Controller
                        control={control} name="sourceFormat"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.Format} validationMessage={error?.message}>
                                <Dropdown
                                    key={fieldName}
                                    selectedOptions={value ? [value] : []}
                                    onOptionSelect={(_evt, data) => onChange(data.optionValue)}
                                >
                                    <Option key="1" text="ASCII" value="1">ASCII</Option>
                                    <Option key="2" text="UTF-8" value="2">UTF-8</Option>
                                    <Option key="3" text="UTF-8N" value="3">UTF-8N</Option>
                                    <Option key="4" text="UTF-16" value="4">UTF-16</Option>
                                    <Option key="5" text="UTF-16LE" value="5">UTF-16LE</Option>
                                    <Option key="6" text="UTF-16BE" value="6">UTF-16BE</Option>
                                    <Option key="7" text="UTF-32" value="7">UTF-32</Option>
                                    <Option key="8" text="UTF-32LE" value="8">UTF-32LE</Option>
                                    <Option key="9" text="UTF-32BE" value="9">UTF-32BE</Option>
                                </Dropdown>
                            </Field>}
                        rules={{
                            required: `${nlsHPCC.SelectA} ${nlsHPCC.Format}`
                        }}
                    /></td>
                    <td><Controller
                        control={control} name="sourceMaxRecordSize"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.MaxRecordLength} validationMessage={error?.message}>
                                <Input
                                    name={fieldName}
                                    value={value}
                                    placeholder="8192"
                                    onChange={(_, data) => onChange(data.value)}
                                />
                            </Field>}
                    /></td>
                </tr>
                <tr>
                    <td><Controller
                        control={control} name="sourceCsvQuote"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.Quote} validationMessage={error?.message}>
                                <Input
                                    name={fieldName}
                                    value={value}
                                    placeholder="'"
                                    onChange={(_, data) => onChange(data.value)}
                                />
                            </Field>}
                    /></td>
                    <td><Controller
                        control={control} name="sourceCsvEscape"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.Escape} validationMessage={error?.message}>
                                <Input
                                    name={fieldName}
                                    value={value}
                                    onChange={(_, data) => onChange(data.value)}
                                />
                            </Field>}
                    /></td>
                </tr>
                <tr>
                    <td><Controller
                        control={control} name="sourceCsvSeparate" defaultValue="\\,"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.Separators} validationMessage={error?.message}>
                                <Input
                                    name={fieldName}
                                    value={value}
                                    placeholder=","
                                    onChange={(_, data) => onChange(data.value)}
                                />
                            </Field>}
                    /></td>
                    <td><Controller
                        control={control} name="sourceCsvTerminate" defaultValue="\\n,\\r\\n"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.LineTerminators} validationMessage={error?.message}>
                                <Input
                                    name={fieldName}
                                    value={value}
                                    placeholder="\\n,\\r\\n"
                                    onChange={(_, data) => onChange(data.value)}
                                />
                            </Field>}
                    /></td>
                </tr>
            </tbody></table>
        </div>
        <div style={{ display: "flex", flexDirection: "column" }}>
            <table className={componentStyles.twoColumnTable}>
                <tbody><tr>
                    <td><Controller
                        control={control} name="overwrite"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.Overwrite} />}
                    /></td>
                    <td><Controller
                        control={control} name="replicate"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Tooltip content={nlsHPCC.ReplicateTooltip} relationship="label">
                                <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.Replicate} />
                            </Tooltip>}
                    /></td>
                </tr>
                    <tr>
                        <td><Controller
                            control={control} name="nosplit"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.NoSplit} />}
                        /></td>
                        <td><Controller
                            control={control} name="noCommon"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.NoCommon} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="compress"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.Compress} />}
                        /></td>
                        <td><Controller
                            control={control} name="failIfNoSourceFile"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.FailIfNoSourceFile} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="recordStructurePresent"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.RecordStructurePresent} />}
                        /></td>
                        <td><Controller
                            control={control} name="quotedTerminator"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.QuotedTerminator} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="expireDays"
                            render={({
                                field: { onChange, name: fieldName, value },
                                fieldState: { error }
                            }) => <Field label={nlsHPCC.ExpireDays} validationMessage={error?.message}>
                                    <Input
                                        name={fieldName}
                                        value={value}
                                        onChange={(_, data) => onChange(data.value)}
                                    />
                                </Field>}
                            rules={{
                                min: {
                                    value: 1,
                                    message: nlsHPCC.ValidationErrorExpireDaysMinimum
                                }
                            }}
                        /></td>
                        <td></td>
                    </tr></tbody>
            </table>
        </div>
    </MessageBox>;
};