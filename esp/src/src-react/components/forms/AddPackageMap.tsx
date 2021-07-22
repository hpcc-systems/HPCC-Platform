import * as React from "react";
import { Checkbox, ContextualMenu, Dropdown, IconButton, IDragOptions, IDropdownOption, mergeStyleSets, Modal, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import { scopedLogger } from "@hpcc-js/util";
import * as WsPackageMaps from "src/WsPackageMaps";
import nlsHPCC from "src/nlsHPCC";
import * as FormStyles from "./landing-zone/styles";

const logger = scopedLogger("src-react/components/forms/AddPackageMap.tsx");

interface AddPackageMapValues {
    Info: string;
    PackageMap: string;
    Target: string;
    Process: string;
    DaliIp: string;
    Activate: boolean
    OverWrite: boolean;
}

const defaultValues: AddPackageMapValues = {
    Info: "",
    PackageMap: "",
    Target: "",
    Process: "",
    DaliIp: "",
    Activate: true,
    OverWrite: false
};

interface AddPackageMapProps {
    showForm: boolean;
    setShowForm: (_: boolean) => void;

    refreshTable: (_: boolean) => void;

    processes: IDropdownOption[];
    targets: IDropdownOption[];
}

export const AddPackageMap: React.FunctionComponent<AddPackageMapProps> = ({
    showForm,
    setShowForm,
    refreshTable,
    processes,
    targets
}) => {

    const { handleSubmit, control, reset } = useForm<AddPackageMapValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                WsPackageMaps.AddPackage({
                    request: data
                })
                    .then(({ AddPackageResponse, Exceptions }) => {
                        if (AddPackageResponse?.status?.Code === 0) {
                            closeForm();
                            refreshTable(true);
                            reset(defaultValues);
                        } else if (Exceptions) {
                            closeForm();
                            logger.error(Exceptions.Exception[0].Message);
                        }
                    })
                    .catch(logger.error)
                    ;
            },
            err => {
                logger.error(err);
            }
        )();
    }, [closeForm, handleSubmit, refreshTable, reset]);

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
                minWidth: 620,
            }
        }
    );

    React.useEffect(() => {
        reset({ ...defaultValues, Target: "*", Process: "*" });
    }, [reset]);

    return <Modal
        titleAriaId={titleId}
        isOpen={showForm}
        onDismiss={closeForm}
        isBlocking={false}
        containerClassName={componentStyles.container}
        dragOptions={dragOptions}
    >
        <div className={componentStyles.header}>
            <span id={titleId}>{nlsHPCC.AddProcessMap}</span>
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
                    control={control} name="Info"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            required={true}
                            label={nlsHPCC.PackageContent}
                            value={value}
                            multiline={true}
                            rows={16}
                            errorMessage={error && error?.message}
                        />}
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
                />
                <Controller
                    control={control} name="PackageMap"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            required={true}
                            label={nlsHPCC.ID}
                            value={value}
                            errorMessage={error && error?.message}
                        />}
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
                />
                <Controller
                    control={control} name="Target"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <Dropdown
                            key={fieldName}
                            label={nlsHPCC.Target}
                            options={targets}
                            selectedKey={value}
                            onChange={(evt, option) => {
                                onChange(option.key);
                            }}
                        />}
                />
                <Controller
                    control={control} name="Process"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <Dropdown
                            key={fieldName}
                            label={nlsHPCC.ProcessFilter}
                            options={processes}
                            selectedKey={value}
                            onChange={(evt, option) => {
                                onChange(option.key);
                            }}
                        />}
                />
                <Controller
                    control={control} name="DaliIp"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.RemoteDaliIP}
                            value={value}
                        />}
                />
                <div style={{ paddingTop: "15px" }}>
                    <Controller
                        control={control} name="Activate"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Activate} />}
                    />
                </div>
                <div style={{ paddingTop: "10px" }}>
                    <Controller
                        control={control} name="OverWrite"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Overwrite} />}
                    />
                </div>
            </Stack>
            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={FormStyles.buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Submit} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};