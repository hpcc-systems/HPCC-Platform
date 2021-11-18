import * as React from "react";
import { Checkbox, DefaultButton, Dropdown, IDropdownOption, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import { scopedLogger } from "@hpcc-js/util";
import * as WsPackageMaps from "src/WsPackageMaps";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";
import * as FormStyles from "./landing-zone/styles";

const logger = scopedLogger("../components/forms/AddPackageMap.tsx");

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
                    .catch(err => logger.error(err))
                    ;
            },
            err => {
                logger.error(err);
            }
        )();
    }, [closeForm, handleSubmit, refreshTable, reset]);

    React.useEffect(() => {
        reset({ ...defaultValues, Target: "*", Process: "*" });
    }, [reset]);

    return <MessageBox title={nlsHPCC.AddProcessMap} show={showForm} setShow={closeForm}
        footer={<>
            <PrimaryButton text={nlsHPCC.Submit} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
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
    </MessageBox>;
};