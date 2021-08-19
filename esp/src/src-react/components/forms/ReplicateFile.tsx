import * as React from "react";
import { ContextualMenu, IconButton, IDragOptions, mergeStyleSets, Modal, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../../hooks/file";
import * as FileSpray from "src/FileSpray";
import { TargetGroupTextField } from "./Fields";
import * as FormStyles from "./landing-zone/styles";
import { pushUrl } from "../../util/history";

interface ReplicateFileFormValues {
    sourceLogicalName: string;
    replicateOffset: string;
    cluster: string;
}

const defaultValues: ReplicateFileFormValues = {
    sourceLogicalName: "",
    replicateOffset: "1",
    cluster: ""
};

interface ReplicateFileProps {
    cluster: string;
    logicalFile: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const ReplicateFile: React.FunctionComponent<ReplicateFileProps> = ({
    cluster,
    logicalFile,
    showForm,
    setShowForm
}) => {

    const [file] = useFile(cluster, logicalFile);
    const { handleSubmit, control, reset } = useForm<ReplicateFileFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request = { ...data, srcname: logicalFile };
                FileSpray.Replicate({ request: request }).then(response => {
                    closeForm();
                    pushUrl(`/dfuworkunits/${response?.ReplicateResponse?.wuid}`);
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
        const newValues = { ...defaultValues, sourceLogicalName: logicalFile };
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
            <span id={titleId}>{nlsHPCC.Rename}</span>
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
                    control={control} name="sourceLogicalName"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            required={true}
                            label={nlsHPCC.SourceLogicalFile}
                            value={value}
                            errorMessage={error && error.message}
                        />}
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
                />
                <Controller
                    control={control} name="replicateOffset"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.ReplicateOffset}
                            value={value}
                            errorMessage={error && error.message}
                        />}
                    rules={{
                        pattern: {
                            value: /^[0-9]+$/i,
                            message: nlsHPCC.ValidationErrorEnterNumber
                        }
                    }}
                />
                <Controller
                    control={control} name="cluster"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TargetGroupTextField
                            key={fieldName}
                            label={nlsHPCC.Cluster}
                            required={true}
                            selectedKey={value}
                            placeholder={nlsHPCC.SelectValue}
                            onChange={(evt, option) => {
                                onChange(option.key);
                            }}
                            errorMessage={error && error.message}
                        />}
                    rules={{
                        required: `${nlsHPCC.SelectA} ${nlsHPCC.Cluster}`
                    }}
                />
            </Stack>

            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={FormStyles.buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Replicate} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};
