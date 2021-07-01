import * as React from "react";
import { ContextualMenu, FontWeights, getTheme, IconButton, IDragOptions, IIconProps, IStackStyles, mergeStyleSets, Modal, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../../hooks/File";
import * as FileSpray from "src/FileSpray";
import { TargetGroupTextField } from "./Fields";
import { pushUrl } from "../../util/history";

type ReplicateFileFormValues = {
    sourceLogicalName: string;
    replicateOffset: string;
    cluster: string;
};

const defaultValues = {
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

    const onSubmit = () => {
        handleSubmit(
            (data, evt) => {
                const request = { ...data, ...{ srcname: logicalFile } };
                FileSpray.Replicate({ request: request }).then(response => {
                    setShowForm(false);
                    pushUrl(`/dfuworkunits/${response?.ReplicateResponse?.wuid}`);
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
        const newValues = { ...defaultValues, ...{ "sourceLogicalName": logicalFile } };
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
                styles={iconButtonStyles}
                iconProps={cancelIcon}
                ariaLabel="Close popup modal"
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
                        errorMessage={ error && error.message }
                    /> }
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
                        errorMessage={ error && error.message }
                    /> }
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
                        placeholder={"Select a value"}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                        errorMessage={ error && error.message }
                    /> }
                    rules={{
                        required: `Select a ${nlsHPCC.Cluster}`
                    }}
                />
            </Stack>

            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Replicate} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};
