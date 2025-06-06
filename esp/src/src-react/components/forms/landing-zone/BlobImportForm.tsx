import * as React from "react";
import { Checkbox, DefaultButton, IDropdownOption, mergeStyleSets, PrimaryButton, Spinner, Stack, TextField } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import * as FileSpray from "src/FileSpray";
import { TargetDfuSprayQueueTextField, TargetGroupTextField } from "../Fields";
import nlsHPCC from "src/nlsHPCC";
import { useBuildInfo } from "../../../hooks/platform";
import { MessageBox } from "../../../layouts/MessageBox";
import * as FormStyles from "./styles";
import { pushUrl } from "../../../util/history";

const logger = scopedLogger("src-react/components/forms/landing-zone/BlobImportForm.tsx");

interface BlobImportFormValues {
    destGroup: string;
    DFUServerQueue: string;
    destLogicalName: string;
    selectedFiles?: {
        TargetName: string,
        NumParts: string,
        SourceFile: string,
        SourcePlane: string,
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
    nosplit: true,
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

    const [, { isContainer }] = useBuildInfo();

    const { handleSubmit, control, reset } = useForm<BlobImportFormValues>({ defaultValues });
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
                    request["fullPath"] = file.SourceFile;
                    request["destNumParts"] = file.NumParts;
                    requests.push(FileSpray.SprayFixed({
                        request: request
                    }));
                });

                Promise.all(requests).then(responses => {
                    if (responses.length === 1) {
                        const response = responses[0];
                        if (response?.Exceptions) {
                            const err = response.Exceptions.Exception[0].Message;
                            logger.error(err);
                        } else if (response.SprayFixedResponse?.wuid) {
                            pushUrl(`#/dfuworkunits/${response.SprayFixedResponse.wuid}`);
                        }
                    } else {
                        const errors = [];
                        responses.forEach(response => {
                            if (response?.Exceptions) {
                                const err = response.Exceptions.Exception[0].Message;
                                errors.push(err);
                                logger.error(err);
                            } else if (response.SprayFixedResponse?.wuid) {
                                window.open(`#/dfuworkunits/${response.SprayFixedResponse.wuid}`);
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
            selection.forEach((file: { [id: string]: any }, idx) => {
                newValues.selectedFiles[idx] = {
                    TargetName: "",
                    NumParts: "",
                    SourceFile: file["fullPath"],
                    SourcePlane: file?.DropZone?.Name ?? "",
                    SourceIP: file["NetAddress"]
                };
            });
            reset(newValues);
        }
    }, [selection, reset]);

    return <MessageBox title={`${nlsHPCC.Import} ${nlsHPCC.Blob}`} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Import} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
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
                        value: /^([_a-z0-9]+(::)?)+$/i,
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
                        <th>{nlsHPCC.NumberofParts}</th>
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
                                            value: /^(?:[\/\\]?[-a-z0-9\*_]+[\/\\])+([-a-z0-9 \*_\.]+)$/i,
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
                                    }) => <TextField
                                            name={fieldName}
                                            onChange={onChange}
                                            value={value}
                                            errorMessage={error && error?.message}
                                        />}
                                    rules={{
                                        pattern: {
                                            value: /^[0-9]+$/i,
                                            message: nlsHPCC.ValidationErrorEnterNumber
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
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.NoSplit} disabled={true} />}
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