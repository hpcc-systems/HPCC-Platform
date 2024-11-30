import * as React from "react";
import { Checkbox, DefaultButton, IDropdownOption, mergeStyleSets, MessageBar, MessageBarType, PrimaryButton, Spinner, Stack, TextField } from "@fluentui/react";
import { Controller, useForm } from "react-hook-form";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as FileSpray from "src/FileSpray";
import * as WsTopology from "src/WsTopology";
import * as FormStyles from "./landing-zone/styles";
import { TargetGroupTextField } from "./Fields";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/RemoteCopy.tsx");

interface RemoteCopyFormValues {
    sourceDali: string;
    srcusername: string;
    srcpassword: string;
    sourceLogicalName: string;
    destGroup: string;
    destLogicalName: string;
    overwrite: boolean;
    replicate: boolean;
    nosplit: boolean;
    compress: boolean;
    Wrap: boolean;
    superCopy: boolean;
}

const defaultValues: RemoteCopyFormValues = {
    sourceDali: "",
    srcusername: "",
    srcpassword: "",
    sourceLogicalName: "",
    destGroup: "",
    destLogicalName: "",
    overwrite: false,
    replicate: false,
    nosplit: false,
    compress: false,
    Wrap: false,
    superCopy: false
};

interface RemoteCopyProps {
    showForm: boolean;
    setShowForm: (_: boolean) => void;

    refreshGrid?: (_?: boolean) => void;
}

export const RemoteCopy: React.FunctionComponent<RemoteCopyProps> = ({
    showForm,
    setShowForm,
    refreshGrid
}) => {

    const { handleSubmit, control, reset } = useForm<RemoteCopyFormValues>({ defaultValues });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);
    const [selectedDestGroup, setSelectedDestGroup] = React.useState("");
    const [replicateDisabled, setReplicateDisabled] = React.useState(true);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);

                FileSpray.Copy({
                    request: data
                }).then(({ CopyResponse, Exceptions }) => {
                    if (Exceptions?.Exception) {
                        setSubmitDisabled(false);
                        setSpinnerHidden(true);
                        setShowError(true);
                        setErrorMessage(Exceptions?.Exception[0]?.Message);
                    } else {
                        setSubmitDisabled(false);
                        setSpinnerHidden(true);
                        setShowForm(false);
                        reset(defaultValues);
                        if (refreshGrid) refreshGrid(true);
                    }
                });
            },
            err => {
                logger.error(err);
            }
        )();
    }, [handleSubmit, refreshGrid, reset, setShowForm]);

    const componentStyles = mergeStyleSets(
        FormStyles.componentStyles,
        {
            container: {
                minWidth: 440,
            }
        }
    );

    React.useEffect(() => {
        WsTopology.TpGroupQuery({
            request: {}
        }).then(response => {
            const groups = response.TpGroupQueryResponse?.TpGroups?.TpGroup ?? [];
            for (const index in groups) {
                if (groups[index].Name === selectedDestGroup) {
                    if (groups[index].ReplicateOutputs === true) {
                        setReplicateDisabled(false);
                        break;
                    }
                    setReplicateDisabled(true);
                    break;
                }
            }
        }).catch(err => logger.error(err));
    }, [selectedDestGroup]);

    return <MessageBox title={nlsHPCC.RemoteCopy} show={showForm} setShow={setShowForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Copy} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => setShowForm(false)} />
        </>}>
        {showError &&
            <MessageBar messageBarType={MessageBarType.error} isMultiline={true} onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                {errorMessage}
            </MessageBar>
        }
        <Stack>
            <Controller
                control={control} name="sourceDali"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        key={fieldName}
                        label={nlsHPCC.Dali}
                        value={value}
                        onChange={onChange}
                    />}
            />
            <Controller
                control={control} name="srcusername"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        key={fieldName}
                        label={nlsHPCC.UserID}
                        value={value}
                        onChange={onChange}
                    />}
            />
            <Controller
                control={control} name="srcpassword"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        type="password"
                        key={fieldName}
                        label={nlsHPCC.Password}
                        value={value}
                        onChange={onChange}
                    />}
            />
            <Controller
                control={control} name="sourceLogicalName"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        key={fieldName}
                        label={`${nlsHPCC.Source} ${nlsHPCC.LogicalName}`}
                        required={true}
                        value={value}
                        onChange={onChange}
                        errorMessage={error && error.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
        </Stack>
        <Stack>
            <Controller
                control={control} name="destGroup"
                render={({
                    field: { onChange, name: fieldName },
                    fieldState: { error }
                }) => <TargetGroupTextField
                        key={fieldName}
                        label={nlsHPCC.Group}
                        required={true}
                        onChange={(evt, option: IDropdownOption) => {
                            setSelectedDestGroup(option.key.toString());
                            onChange(option.key);
                        }}
                        errorMessage={error && error.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <Controller
                control={control} name="destLogicalName"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        key={fieldName}
                        label={`${nlsHPCC.Target} ${nlsHPCC.LogicalName}`}
                        required={true}
                        value={value}
                        onChange={onChange}
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
                            control={control} name="nosplit"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.NoSplit} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="compress"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Compress} />}
                        /></td>
                        <td><Controller
                            control={control} name="Wrap"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Wrap} />}
                        /></td>
                    </tr>
                    <tr>
                        <td><Controller
                            control={control} name="replicate"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Replicate} disabled={replicateDisabled} />}
                        /></td>
                        <td><Controller
                            control={control} name="superCopy"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.RetainSuperfileStructure} />}
                        /></td>
                    </tr>
                </tbody>
            </table>
        </Stack>
    </MessageBox>;

};