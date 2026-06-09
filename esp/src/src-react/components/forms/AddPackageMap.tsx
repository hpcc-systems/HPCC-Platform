import * as React from "react";
import { IDropdownOption } from "./Fields";
import { Button, Checkbox, Dropdown, Field, Input, Option, Spinner, Textarea } from "@fluentui/react-components";
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
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Submit}</Button>
            <Button onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
        </>}>
        <div style={{ display: "flex", flexDirection: "column" }}>
            <Controller
                control={control} name="Info"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field label={nlsHPCC.PackageContent} required validationMessage={error?.message}>
                        <Textarea
                            name={fieldName}
                            value={value}
                            rows={16}
                            onChange={(_, data) => onChange(data.value)}
                        />
                    </Field>}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <Controller
                control={control} name="PackageMap"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field label={nlsHPCC.ID} required validationMessage={error?.message}>
                        <Input
                            name={fieldName}
                            value={value}
                            onChange={(_, data) => onChange(data.value)}
                        />
                    </Field>}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <Controller
                control={control} name="Target"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field label={nlsHPCC.Target}>
                        <Dropdown
                            key={fieldName}
                            selectedOptions={value ? [String(value)] : []}
                            onOptionSelect={(_evt, data) => {
                                onChange(data.optionValue);
                            }}
                        >
                            {targets?.map(opt => (
                                <Option key={String(opt.key)} text={opt.text} value={String(opt.key)}>{opt.text}</Option>
                            ))}
                        </Dropdown>
                    </Field>}
            />
            <Controller
                control={control} name="Process"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field label={nlsHPCC.ProcessFilter}>
                        <Dropdown
                            key={fieldName}
                            selectedOptions={value ? [String(value)] : []}
                            onOptionSelect={(_evt, data) => {
                                onChange(data.optionValue);
                            }}
                        >
                            {processes?.map(opt => (
                                <Option key={String(opt.key)} text={opt.text} value={String(opt.key)}>{opt.text}</Option>
                            ))}
                        </Dropdown>
                    </Field>}
            />
            <Controller
                control={control} name="DaliIp"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field label={nlsHPCC.RemoteDaliIP}>
                        <Input
                            name={fieldName}
                            value={value}
                            onChange={(_, data) => onChange(data.value)}
                        />
                    </Field>}
            />
            <Controller
                control={control} name="RemoteStorage"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field label={nlsHPCC.RemoteStorage}>
                        <Dropdown
                            key={fieldName}
                            selectedOptions={value ? [String(value)] : []}
                            onOptionSelect={(_evt, data) => {
                                onChange(data.optionValue);
                            }}
                        >
                            {remoteTargets?.map(opt => (
                                <Option key={String(opt.key)} text={opt.text} value={String(opt.key)}>{opt.text}</Option>
                            ))}
                        </Dropdown>
                    </Field>}
            />
            <div style={{ paddingTop: "15px" }}>
                <Controller
                    control={control} name="Activate"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.Activate} />}
                />
            </div>
            <div style={{ paddingTop: "10px" }}>
                <Controller
                    control={control} name="OverWrite"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.Overwrite} />}
                />
            </div>
        </div>
    </MessageBox>;
};