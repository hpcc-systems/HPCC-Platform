import * as React from "react";
import { Checkbox, DefaultButton, mergeStyleSets, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import { FileSpray, FileSprayService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { useBuildInfo } from "../../hooks/platform";
import { MessageBox } from "../../layouts/MessageBox";
import { TargetDropzoneTextField, TargetFolderTextField, TargetServerTextField } from "./Fields";
import * as FormStyles from "./landing-zone/styles";

const logger = scopedLogger("src-react/components/forms/DesprayFile.tsx");

const myFileSprayService = new FileSprayService({ baseUrl: "" });

interface DesprayFileFormValues {
    destGroup: string;
    destIP: string;
    destPlane: string;
    destPath: string;
    sourceLogicalName: string;
    targetName?: {
        name: string
    }[];
    splitprefix: string;
    overwrite: boolean;
    SingleConnection: boolean;
    wrap: boolean;
}

const defaultValues: DesprayFileFormValues = {
    destGroup: "",
    destIP: "",
    destPlane: "",
    destPath: "",
    sourceLogicalName: "",
    splitprefix: "",
    overwrite: false,
    SingleConnection: false,
    wrap: false
};

interface DesprayFileProps {
    cluster?: string;
    logicalFiles: string[];

    showForm: boolean;
    setShowForm: (_: boolean) => void;

    refreshGrid?: (_?: boolean) => void;
}

export const DesprayFile: React.FunctionComponent<DesprayFileProps> = ({
    cluster,
    logicalFiles,
    showForm,
    setShowForm,
    refreshGrid
}) => {

    const [, { isContainer }] = useBuildInfo();

    const [machine, setMachine] = React.useState<string>("");
    const [directory, setDirectory] = React.useState<string>("/");
    const [dropzone, setDropzone] = React.useState<string>("");
    const [pathSep, setPathSep] = React.useState<string>("/");
    const [os, setOs] = React.useState<number>();

    const { handleSubmit, control, reset } = useForm<DesprayFileFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data: DesprayFileFormValues, evt) => {
                if (logicalFiles.length > 0) {
                    if (logicalFiles.length === 1) {
                        const request = {
                            ...data,
                            destPlane: isContainer ? data.destGroup : "",
                            destPath: [data.destPath, data.sourceLogicalName].join(pathSep),
                            sourceLogicalName: logicalFiles[0]
                        } as FileSpray.Despray;
                        myFileSprayService.Despray(request).then(response => {
                            closeForm();
                            reset(defaultValues);
                            if (refreshGrid) refreshGrid(true);
                        }).catch(err => logger.error(err));
                    } else {
                        const requests = [];
                        logicalFiles.forEach((logicalFile, idx) => {
                            const request = {
                                ...data,
                                sourceLogicalName: logicalFile,
                                destPath: [data.destPath, data.targetName[idx].name].join(pathSep),
                            } as FileSpray.Despray;
                            requests.push(myFileSprayService.Despray(request));
                        });
                        Promise.all(requests).then(_ => {
                            closeForm();
                            if (refreshGrid) refreshGrid(true);
                        }).catch(err => logger.error(err));
                    }
                }
            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, handleSubmit, isContainer, logicalFiles, pathSep, refreshGrid, reset]);

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
            const newValues = { ...defaultValues, sourceLogicalName: logicalFiles[0] };
            reset(newValues);
        } else if (logicalFiles.length > 1) {
            const _files = [];
            logicalFiles.forEach(file => {
                _files.push({ name: file });
            });
            const newValues = { ...defaultValues, targetName: _files };
            reset(newValues);
        }
    }, [logicalFiles, reset]);

    return <MessageBox title={nlsHPCC.Despray} show={showForm} setShow={closeForm}
        footer={<>
            <PrimaryButton text={nlsHPCC.Despray} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Stack>
            <Controller
                control={control} name="destGroup"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetDropzoneTextField
                        key={fieldName}
                        label={nlsHPCC.DropZone}
                        required={true}
                        selectedKey={value}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option) => {
                            setDropzone(option.key as string);
                            setDirectory(option["path"] as string);
                            if (option["path"].indexOf("\\") > -1) {
                                setPathSep("\\");
                            }
                            setOs(option["OS"] as number);
                            onChange(option.key);
                        }}
                        errorMessage={error && error.message}
                    />}
                rules={{
                    required: `${nlsHPCC.SelectA} ${nlsHPCC.DropZone}`
                }}
            />
            {!isContainer &&
                <Controller
                    control={control} name="destIP"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TargetServerTextField
                            key={fieldName}
                            dropzone={dropzone}
                            required={true}
                            label={nlsHPCC.IPAddress}
                            selectedKey={value}
                            placeholder={nlsHPCC.SelectValue}
                            onChange={(evt, option) => {
                                setMachine(option.key as string);
                                setOs(option["OS"] as number);
                                onChange(option.key);
                            }}
                            errorMessage={error && error.message}
                        />}
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
                />
            }
            <Controller
                control={control} name="destPath"
                render={({
                    field: { onChange, name: fieldName },
                    fieldState: { error }
                }) => <TargetFolderTextField
                        key={fieldName}
                        label={nlsHPCC.Path}
                        dropzone={dropzone}
                        pathSepChar={pathSep}
                        machineAddress={machine}
                        machineDirectory={directory}
                        machineOS={os}
                        required={true}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(_evt, option, _idx, value) => {
                            if (option?.key) {
                                onChange(option.key);
                            } else {
                                value = value.replace(directory, "");
                                if (value.startsWith(pathSep)) {
                                    value = value.substring(1);
                                }
                                if (value.endsWith(pathSep)) {
                                    value = value.substring(0, value.length);
                                }
                                onChange([directory, value].join(pathSep));
                            }
                        }}
                        errorMessage={error && error.message}
                    />}
            />
            {logicalFiles?.length === 1 &&
                <Controller
                    control={control} name="sourceLogicalName"
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
                <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`} style={{ marginTop: "15px" }}>
                    <thead>
                        <tr>
                            <th>{nlsHPCC.TargetName}</th>
                        </tr>
                    </thead>
                    <tbody>
                        {logicalFiles.map((file, idx) => {
                            return <tr key={`File-${idx}`}>
                                <td width="50%">{file}</td>
                                <td width="50%">
                                    <Controller
                                        control={control} name={`targetName.${idx}.name` as const}
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
            }
            <Controller
                control={control} name="splitprefix"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.SplitPrefix}
                        value={value}
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
                        control={control} name="SingleConnection"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.UseSingleConnection} />}
                    /></td>
                </tr>
                    <tr>
                        <td><Controller
                            control={control} name="wrap"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.PreserveParts} />}
                        /></td>
                    </tr></tbody>
            </table>
        </Stack>
    </MessageBox>;
};
