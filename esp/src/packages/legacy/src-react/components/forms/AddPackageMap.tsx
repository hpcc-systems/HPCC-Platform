import * as React from "react";
import { Checkbox, DefaultButton, Dropdown, IDropdownOption, PrimaryButton, Spinner, Stack, TextField, } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import { FileSprayService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import * as WsPackageMaps from "src/WsPackageMaps";
import { TypedDropdownOption } from "../PackageMaps";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("../components/forms/AddPackageMap.tsx");

interface AddPackageMapValues {
    Info: string;
    PackageMap: string;
    Target: string;
    Process: string;
    DaliIp: string;
    RemoteStorage: string;
    Activate: boolean
    OverWrite: boolean;
}

const defaultValues: AddPackageMapValues = {
    Info: "",
    PackageMap: "",
    Target: "",
    Process: "",
    DaliIp: "",
    RemoteStorage: "",
    Activate: true,
    OverWrite: false
};

const fileSprayService = new FileSprayService({ baseUrl: "" });

interface AddPackageMapProps {
    showForm: boolean;
    setShowForm: (_: boolean) => void;
    refreshData: (_: boolean) => void;
    processes: TypedDropdownOption[];
    targets: TypedDropdownOption[];
}

export const AddPackageMap: React.FunctionComponent<AddPackageMapProps> = ({
    showForm,
    setShowForm,
    refreshData,
    processes,
    targets
}) => {

    const { handleSubmit, control, reset } = useForm<AddPackageMapValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const [remoteTargets, setRemoteTargets] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        fileSprayService.GetRemoteTargets({}).then(response => {
            setRemoteTargets(response?.TargetNames?.Item?.map(item => { return { key: item, text: item }; }));
        }).catch(err => logger.error(err));
    }, []);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);
                WsPackageMaps.AddPackage({
                    request: data
                })
                    .then(({ AddPackageResponse, Exceptions }) => {
                        if (AddPackageResponse?.status?.Code === 0) {
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            closeForm();
                            refreshData(true);
                            reset(defaultValues);
                        } else if (Exceptions) {
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
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
    }, [closeForm, handleSubmit, refreshData, reset]);

    React.useEffect(() => {
        reset({
            ...defaultValues,
            Target: targets?.filter(target => target.type === "roxie")[0]?.key as string ?? "*",
            Process: processes?.filter(target => target.type === "roxie")[0]?.key as string ?? "*"
        });
    }, [processes, reset, targets]);

    return <MessageBox title={nlsHPCC.AddProcessMap} show={showForm} setShow={closeForm} minWidth={500}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Submit} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
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
            <Controller
                control={control} name="RemoteStorage"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Dropdown
                        key={fieldName}
                        label={nlsHPCC.RemoteStorage}
                        options={remoteTargets}
                        selectedKey={value}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
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
    </MessageBox>;
};