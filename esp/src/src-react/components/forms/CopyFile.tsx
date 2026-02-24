import * as React from "react";
import { Checkbox, DefaultButton, IDropdownOption, mergeStyleSets, PrimaryButton, Spinner, TextField, } from "@fluentui/react";
import { StackShim } from "@fluentui/react-migration-v8-v9";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as FileSpray from "src/FileSpray";
import { MessageBox } from "../../layouts/MessageBox";
import { pushUrl } from "../../util/history";
import { TargetGroupTextField } from "./Fields";
import * as FormStyles from "./landing-zone/styles";

const logger = scopedLogger("src-react/components/forms/CopyFile.tsx");

interface CopyFileFormValues {
    destGroup: string;
    destLogicalName: string;
    targetCopyName?: {
        name: string,
        numParts: string
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
                const requests = [];
                logicalFiles.forEach((logicalFile, idx) => {
                    const destLogicalName = data.targetCopyName[idx].name ? data.targetCopyName[idx].name : "";
                    const destNumParts = data.targetCopyName[idx].numParts ? data.targetCopyName[idx].numParts : "0";
                    const request = { ...data, sourceLogicalName: logicalFile, destLogicalName, DestNumParts: destNumParts };
                    requests.push(FileSpray.Copy({ request: request }));
                });
                Promise.all(requests).then(responses => {
                    const urls: string[] = [];
                    const errors: string[] = [];
                    responses.forEach(response => {
                        if (response?.Exceptions) {
                            const err = response.Exceptions.Exception[0].Message;
                            errors.push(err);
                            logger.error(err);
                        } else if (response.CopyResponse?.result) {
                            urls.push(`#/dfuworkunits/${response.CopyResponse.result}`);
                        }
                    });
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                    if (errors.length === 0) {
                        closeForm();
                    }
                    if (urls.length === 1) {
                        pushUrl(urls[0]);
                    } else {
                        if (refreshGrid) {
                            window.setTimeout(() => refreshGrid(), 200);
                        }
                        urls.forEach(url => window.open(url));
                    }
                });
            },
            err => {
                logger.error(err);
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
        const _files = [];
        logicalFiles.forEach(file => {
            _files.push({ name: file, numParts: "" });
        });
        const newValues = { ...defaultValues, targetCopyName: _files };
        reset(newValues);
    }, [logicalFiles, reset]);

    return <MessageBox title={nlsHPCC.Copy} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Copy} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <StackShim>
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
                        onChange={(evt, option: IDropdownOption) => {
                            onChange(option.key);
                        }}
                        errorMessage={error && error.message}
                    />}
                rules={{
                    required: `${nlsHPCC.SelectA} ${nlsHPCC.Group}`
                }}
            />
        </StackShim>
        <StackShim>
            <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`}>
                <thead>
                    <tr>
                        <th>{nlsHPCC.TargetName}</th>
                        <th>{nlsHPCC.NumberofParts}</th>
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
                            <td>
                                <Controller
                                    control={control} name={`targetCopyName.${idx}.numParts` as const}
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
                            </td>
                        </tr>;
                    })}
                </tbody>
            </table>
        </StackShim>
        <StackShim>
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
        </StackShim>
    </MessageBox>;
};
