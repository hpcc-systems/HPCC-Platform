import * as React from "react";
import { Checkbox, ContextualMenu, IconButton, IDragOptions, mergeStyleSets, Modal, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../../hooks/file";
import * as FileSpray from "src/FileSpray";
import { TargetGroupTextField } from "./Fields";
import * as FormStyles from "./landing-zone/styles";
import { pushUrl } from "../../util/history";

interface CopyFileFormValues {
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

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request = { ...data, sourceLogicalName: logicalFile };
                FileSpray.Copy({ request: request }).then(response => {
                    closeForm();
                    pushUrl(`/files/${data.destGroup}/${data.destLogicalName}`);
                });
            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, handleSubmit, logicalFile]);

    const titleId = useId("title");

    const dragOptions: IDragOptions = {
        moveMenuItemText: nlsHPCC.Move,
        closeMenuItemText: nlsHPCC.Close,
        menu: ContextualMenu,
    };

    const componentStyles = mergeStyleSets(
        FormStyles.componentStyles,
        {
            container: {
                minWidth: 440,
            }
        }
    );

    React.useEffect(() => {
        const newValues = { ...defaultValues, destLogicalName: logicalFile };
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
                styles={FormStyles.iconButtonStyles}
                iconProps={FormStyles.cancelIcon}
                ariaLabel={nlsHPCC.CloseModal}
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
            </Stack>
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
                        </tr>
                    </tbody>
                </table>
            </Stack>

            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={FormStyles.buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Copy} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};
