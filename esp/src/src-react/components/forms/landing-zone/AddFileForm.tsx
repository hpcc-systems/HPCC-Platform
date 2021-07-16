import * as React from "react";
import { ContextualMenu, IconButton, IDragOptions, mergeStyleSets, Modal, PrimaryButton, Stack, TextField } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as FormStyles from "./styles";

interface AddFileFormValues {
    NetAddress: string;
    fullPath: string;
}

const defaultValues: AddFileFormValues = {
    NetAddress: "",
    fullPath: ""
};

interface AddFileFormProps {
    formMinWidth?: number;
    showForm: boolean;
    refreshGrid: (() => void),
    store: any;
    setShowForm: (_: boolean) => void;
}

export const AddFileForm: React.FunctionComponent<AddFileFormProps> = ({
    formMinWidth = 300,
    showForm,
    refreshGrid,
    store,
    setShowForm
}) => {

    const { handleSubmit, control, reset } = useForm<AddFileFormValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const dropZone = {
                    ...store.get(data.NetAddress),
                    NetAddress: data.NetAddress
                };
                let fullPathParts = data.fullPath.split("/");
                if (fullPathParts.length === 1) {
                    fullPathParts = data.fullPath.split("\\");
                }
                const file = {
                    ...store.get(data.NetAddress + data.fullPath),
                    name: fullPathParts[fullPathParts.length - 1],
                    displayName: fullPathParts[fullPathParts.length - 1],
                    fullPath: data.fullPath,
                    isDir: false,
                    DropZone: dropZone
                };
                store.addUserFile(file);
                refreshGrid();
                closeForm();
                reset(defaultValues);
            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, handleSubmit, refreshGrid, reset, store]);

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
                minWidth: formMinWidth ? formMinWidth : 300,
            }
        }
    );

    return <Modal
        titleAriaId={titleId}
        isOpen={showForm}
        onDismiss={closeForm}
        isBlocking={false}
        containerClassName={componentStyles.container}
        dragOptions={dragOptions}
    >
        <div className={componentStyles.header}>
            <span id={titleId}>{nlsHPCC.AddFile}</span>
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
                    control={control} name="NetAddress"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.IP}
                        required={true}
                        value={value}
                        errorMessage={ error && error?.message }
                    /> }
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
                />
                <Controller
                    control={control} name="fullPath"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.Path}
                        required={true}
                        value={value}
                        placeholder={nlsHPCC.NamePrefixPlaceholder}
                        errorMessage={ error && error?.message }
                    /> }
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
                />
            </Stack>
            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={FormStyles.buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Add} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};