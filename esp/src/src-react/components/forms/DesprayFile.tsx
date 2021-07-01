import * as React from "react";
import { Checkbox, ContextualMenu, FontWeights, getTheme, IconButton, IDragOptions, IIconProps, IStackStyles, mergeStyleSets, Modal, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../../hooks/File";
import * as FileSpray from "src/FileSpray";
import { TargetDropzoneTextField, TargetFolderTextField, TargetServerTextField } from "./Fields";
import { pushUrl } from "../../util/history";

type DesprayFileFormValues = {
    destGroup: string;
    destIP: string;
    destPath: string;
    targetName: string;
    splitprefix: string;
    overwrite: boolean;
    SingleConnection: boolean;
    wrap: boolean;
};

const defaultValues = {
    destGroup: "",
    destIP: "",
    destPath: "",
    targetName: "",
    splitprefix: "",
    overwrite: false,
    SingleConnection: false,
    wrap: false
};

interface DesprayFileProps {
    cluster: string;
    logicalFile: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const DesprayFile: React.FunctionComponent<DesprayFileProps> = ({
    cluster,
    logicalFile,
    showForm,
    setShowForm
}) => {

    const [file] = useFile(cluster, logicalFile);
    const [machine, setMachine] = React.useState<string>("");
    const [directory, setDirectory] = React.useState<string>("/");
    const [pathSep, setPathSep] = React.useState<string>("/");
    const [os, setOs] = React.useState<number>();

    const { handleSubmit, control, reset } = useForm<DesprayFileFormValues>({ defaultValues });

    const onSubmit = () => {
        handleSubmit(
            (data, evt) => {
                const request = {
                    ...data,
                    ...{
                        destPath: [data.destPath, data.targetName].join(pathSep),
                        sourceLogicalName: logicalFile
                    }
                };
                FileSpray.Despray({ request: request }).then(response => {
                    setShowForm(false);
                    pushUrl(`/dfuworkunits/${response?.DesprayResponse?.wuid}`);
                });
            },
            err => {
                console.log(err);
            }
        )();
    };

    const titleId = useId("title");

    const closeForm = () => {
        setShowForm(false);
        reset(defaultValues);
    };

    const dragOptions: IDragOptions = {
        moveMenuItemText: "Move",
        closeMenuItemText: "Close",
        menu: ContextualMenu,
    };

    const theme = getTheme();

    const cancelIcon: IIconProps = { iconName: "Cancel" };
    const iconButtonStyles = {
        root: {
            marginLeft: "auto",
            marginTop: "4px",
            marginRight: "2px",
        }
    };
    const buttonStackStyles: IStackStyles = {
        root: {
            height: "56px",
            justifyContent: "flex-end"
        },
    };
    const componentStyles = mergeStyleSets({
        container: {
            display: "flex",
            flexFlow: "column nowrap",
            alignItems: "stretch",
            minWidth: 440,
        },
        header: [
            {
                flex: "1 1 auto",
                borderTop: `4px solid ${theme.palette.themePrimary}`,
                display: "flex",
                alignItems: "center",
                fontWeight: FontWeights.semibold,
                padding: "12px 12px 14px 24px",
            },
        ],
        body: {
            flex: "4 4 auto",
            padding: "0 24px 24px 24px",
            overflowY: "hidden",
            selectors: {
                p: { margin: "14px 0" },
                "p:first-child": { marginTop: 0 },
                "p:last-child": { marginBottom: 0 },
            },
        },
        selectionTable: {
            padding: "4px",
            border: `1px solid ${theme.palette.themeDark}`
        },
        twoColumnTable: {
            marginTop: "14px",
            "selectors": {
                "tr": { marginTop: "10px" }
            }
        }
    });

    React.useEffect(() => {
        const newValues = { ...defaultValues, ...{ "targetName": file?.Filename } };
        reset(newValues);
    }, [file?.Filename, reset]);

    return <Modal
        titleAriaId={titleId}
        isOpen={showForm}
        onDismiss={closeForm}
        isBlocking={false}
        containerClassName={componentStyles.container}
        dragOptions={dragOptions}
    >
        <div className={componentStyles.header}>
            <span id={titleId}>{nlsHPCC.Despray}</span>
            <IconButton
                styles={iconButtonStyles}
                iconProps={cancelIcon}
                ariaLabel="Close popup modal"
                onClick={closeForm}
            />
        </div>
        <div className={componentStyles.body}>
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
                        placeholder={"Select a value"}
                        onChange={(evt, option) => {
                            setDirectory(option["path"] as string);
                            if (option["path"].indexOf("\\") > -1) {
                                setPathSep("\\");
                            }
                            onChange(option.key);
                        }}
                        errorMessage={ error && error.message }
                    /> }
                    rules={{
                        required: `Select a ${nlsHPCC.DropZone}`
                    }}
                />
                <Controller
                    control={control} name="destIP"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TargetServerTextField
                        key={fieldName}
                        required={true}
                        label={nlsHPCC.IPAddress}
                        selectedKey={value}
                        placeholder={"Select a value"}
                        onChange={(evt, option) => {
                            setMachine(option.key as string);
                            setOs(option["OS"] as number);
                            onChange(option.key);
                        }}
                        errorMessage={ error && error.message }
                    /> }
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
                />
                <Controller
                    control={control} name="destPath"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TargetFolderTextField
                        key={fieldName}
                        label={nlsHPCC.Path}
                        pathSepChar={pathSep}
                        machineAddress={machine}
                        machineDirectory={directory}
                        machineOS={os}
                        required={true}
                        placeholder="Select a value"
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                        errorMessage={ error && error.message }
                    /> }
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired,
                        // pattern: {
                        //     value: /^(\/[a-z0-9]*)+$/i,
                        //     message: nlsHPCC.ValidationErrorTargetNameRequired
                        // }
                    }}
                />
                <Controller
                    control={control} name="targetName"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        required={true}
                        label={nlsHPCC.TargetName}
                        value={value}
                        errorMessage={ error && error.message }
                    /> }
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
                />
                <Controller
                    control={control} name="splitprefix"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.SplitPrefix}
                        value={value}
                    /> }
                />
                </Stack>
                <Stack>
                <table className={componentStyles.twoColumnTable}>
                    <tbody><tr>
                        <td><Controller
                            control={control} name="overwrite"
                            render={({
                                field : { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Overwrite} /> }
                        /></td>
                        <td><Controller
                            control={control} name="SingleConnection"
                            render={({
                                field : { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.UseSingleConnection} /> }
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="wrap"
                            render={({
                                field : { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.PreserveParts} /> }
                        /></td>
                    </tr></tbody>
                </table>
            </Stack>

            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Despray} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};
