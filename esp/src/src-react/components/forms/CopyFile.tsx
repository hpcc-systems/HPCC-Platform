import * as React from "react";
import { Checkbox, ContextualMenu, FontWeights, getTheme, IconButton, IDragOptions, IIconProps, IStackStyles, mergeStyleSets, Modal, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../../hooks/File";
import * as FileSpray from "src/FileSpray";
import { TargetGroupTextField } from "./Fields";
import { pushUrl } from "../../util/history";

type CopyFileFormValues = {
    destGroup: string;
    destLogicalName: string;
    overwrite: boolean;
    replicate: boolean;
    nosplit: boolean;
    compress: boolean;
    Wrap: boolean;
    superCopy: boolean;
    preserveCompression: boolean;
    ExpireDays: string;
};

const defaultValues = {
    destGroup: "",
    destLogicalName: "",
    overwrite: false,
    replicate: false,
    nosplit: false,
    compress: false,
    Wrap: false,
    superCopy: false,
    preserveCompression: true,
    ExpireDays: ""
};

interface CopyFileProps {
    cluster: string;
    logicalFile: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const CopyFile: React.FunctionComponent<CopyFileProps> = ({
    cluster,
    logicalFile,
    showForm,
    setShowForm
}) => {

    const [file] = useFile(cluster, logicalFile);

    const { handleSubmit, control, reset } = useForm<CopyFileFormValues>({ defaultValues });

    const onSubmit = () => {
        handleSubmit(
            (data, evt) => {
                const request = { ...data, ...{ sourceLogicalName: logicalFile } };
                FileSpray.Copy({ request: request }).then(response => {
                    setShowForm(false);
                    pushUrl(`/files/${data.destGroup}/${data.destLogicalName}`);
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
        const newValues = { ...defaultValues, ...{ "destLogicalName": logicalFile } };
        reset(newValues);
    }, [file, logicalFile, reset]);

    return <Modal
        titleAriaId={titleId}
        isOpen={showForm}
        onDismiss={closeForm}
        isBlocking={false}
        containerClassName={componentStyles.container}
        dragOptions={dragOptions}
    >
        <div className={componentStyles.header}>
            <span id={titleId}>{nlsHPCC.Copy}</span>
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
                    }) => <TargetGroupTextField
                        key={fieldName}
                        label={nlsHPCC.Group}
                        required={true}
                        selectedKey={value}
                        placeholder={"Select a value"}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                        errorMessage={ error && error.message }
                    /> }
                    rules={{
                        required: `Select a ${nlsHPCC.Group}`
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
                        required={true}
                        label={nlsHPCC.TargetName}
                        value={value}
                        errorMessage={ error && error.message }
                    /> }
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
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
                            control={control} name="replicate"
                            render={({
                                field : { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Replicate} /> }
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="nosplit"
                            render={({
                                field : { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.NoSplit} /> }
                        /></td>
                        <td><Controller
                            control={control} name="compress"
                            render={({
                                field : { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Compress} /> }
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="Wrap"
                            render={({
                                field : { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Wrap} /> }
                        /></td>
                        <td><Controller
                            control={control} name="superCopy"
                            render={({
                                field : { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.RetainSuperfileStructure} /> }
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="preserveCompression"
                            render={({
                                field : { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.PreserveCompression} /> }
                        /></td>
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
                                errorMessage={ error && error.message }
                            />}
                            rules={{
                                min: {
                                    value: 1,
                                    message: `${nlsHPCC.ValidationErrorNumberLess} 1`
                                }
                            }}
                        /></td>
                    </tr></tbody>
                </table>
            </Stack>

            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Copy} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};
