import * as React from "react";
import { Checkbox, ContextualMenu, IconButton, IDragOptions, mergeStyleSets, Modal, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../../hooks/file";
import * as FileSpray from "src/FileSpray";
import * as FormStyles from "./landing-zone/styles";
import { pushUrl } from "../../util/history";

interface RenameFileFormValues {
    dstname: string;
    overwrite: boolean;
}

const defaultValues: RenameFileFormValues = {
    dstname: "",
    overwrite: false
};

interface RenameFileProps {
    cluster: string;
    logicalFile: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const RenameFile: React.FunctionComponent<RenameFileProps> = ({
    cluster,
    logicalFile,
    showForm,
    setShowForm
}) => {

    const [file] = useFile(cluster, logicalFile);

    const { handleSubmit, control, reset } = useForm<RenameFileFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request = { ...data, srcname: logicalFile };
                FileSpray.Rename({ request: request }).then(response => {
                    closeForm();
                    pushUrl(`/files/${cluster}/${data.dstname}`);
                });
            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, cluster, handleSubmit, logicalFile]);

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
        const newValues = { ...defaultValues, dstname: logicalFile };
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
                    control={control} name="dstname"
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
                <div style={{ paddingTop: "15px" }}>
                    <Controller
                        control={control} name="overwrite"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Overwrite} />}
                    />
                </div>
            </Stack>

            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={FormStyles.buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Rename} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};
