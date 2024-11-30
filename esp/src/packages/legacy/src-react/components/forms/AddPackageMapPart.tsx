import * as React from "react";
import { Checkbox, DefaultButton, PrimaryButton, Spinner, Stack, TextField, } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import { scopedLogger } from "@hpcc-js/util";
import * as WsPackageMaps from "src/WsPackageMaps";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("../components/forms/AddPackageMapPart.tsx");

interface AddPackageMapPartValues {
    PartName: string;
    Content: string;
    DaliIp: string;
    SourceProcess: string;
    DeletePrevious: boolean;
    AllowForeignFiles: boolean;
    PreloadAllPackages: boolean;
    UpdateSuperFiles: boolean;
    UpdateCloneFrom: boolean;
    AppendCluster: boolean;
}

const defaultValues: AddPackageMapPartValues = {
    PartName: "",
    Content: "",
    DaliIp: "",
    SourceProcess: "",
    DeletePrevious: true,
    AllowForeignFiles: true,
    PreloadAllPackages: true,
    UpdateSuperFiles: true,
    UpdateCloneFrom: true,
    AppendCluster: true,
};

interface AddPackageMapPartProps {
    showForm: boolean;
    setShowForm: (_: boolean) => void;
    packageMap: string;
    target: string;
    refreshData: () => void;
}

export const AddPackageMapPart: React.FunctionComponent<AddPackageMapPartProps> = ({
    showForm,
    setShowForm,
    packageMap,
    target,
    refreshData,
}) => {
    const { handleSubmit, control, reset } = useForm<AddPackageMapPartValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);
                WsPackageMaps.AddPartToPackageMap({
                    request: { ...data, Target: target, PackageMap: packageMap }
                })
                    .then(({ AddPartToPackageMapResponse, Exceptions }) => {
                        if (AddPartToPackageMapResponse?.status?.Code === 0) {
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            closeForm();
                            if (refreshData) refreshData();
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
    }, [closeForm, handleSubmit, packageMap, refreshData, reset, target]);

    return <MessageBox title={nlsHPCC.AddProcessMap} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Submit} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Stack>
            <Controller
                control={control} name="PartName"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        placeholder={nlsHPCC.PartName}
                        required={true}
                        label={nlsHPCC.PartName}
                        value={value}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <Controller
                control={control} name="Content"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        required={true}
                        label={nlsHPCC.Content}
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
                control={control} name="DaliIp"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        placeholder={nlsHPCC.DaliIP}
                        label={nlsHPCC.DaliIP}
                        value={value}
                    />}
            />
            <Controller
                control={control} name="SourceProcess"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        placeholder={nlsHPCC.SourceProcess}
                        label={nlsHPCC.SourceProcess}
                        value={value}
                    />}
            />
            <div style={{ paddingTop: "15px" }}>
                <Controller
                    control={control} name="DeletePrevious"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.DeletePrevious} />}
                />
            </div>
            <div style={{ paddingTop: "10px" }}>
                <Controller
                    control={control} name="AllowForeignFiles"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.AllowForeignFiles} />}
                />
            </div>
            <div style={{ paddingTop: "10px" }}>
                <Controller
                    control={control} name="PreloadAllPackages"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.PreloadAllPackages} />}
                />
            </div>
            <div style={{ paddingTop: "10px" }}>
                <Controller
                    control={control} name="UpdateSuperFiles"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.UpdateSuperFiles} />}
                />
            </div>
            <div style={{ paddingTop: "10px" }}>
                <Controller
                    control={control} name="UpdateCloneFrom"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.UpdateCloneFrom} />}
                />
            </div>
            <div style={{ paddingTop: "10px" }}>
                <Controller
                    control={control} name="AppendCluster"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.AppendCluster} />}
                />
            </div>
        </Stack>
    </MessageBox>;
};