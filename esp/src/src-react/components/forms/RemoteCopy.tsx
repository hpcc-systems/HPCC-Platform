import * as React from "react";
import { Button, Checkbox, Dropdown, Field, Input, makeStyles, MessageBar, MessageBarActions, MessageBarBody, Option, Spinner } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { Controller, useForm, useWatch } from "react-hook-form";
import { FileSprayService, TopologyService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { TargetGroupTextField } from "./Fields";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/RemoteCopy.tsx");

const fileSprayService = new FileSprayService({ baseUrl: "" });
const topologyService = new TopologyService({ baseUrl: "" });

interface RemoteCopyFormValues {
    sourceDali: string;
    srcusername: string;
    srcpassword: string;
    sourceLogicalName: string;
    remoteStorage: string;
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
    remoteStorage: "",
    destGroup: "",
    destLogicalName: "",
    overwrite: false,
    replicate: false,
    nosplit: false,
    compress: false,
    Wrap: false,
    superCopy: false
};

const useStyles = makeStyles({
    flex: {
        display: "flex",
        flexDirection: "column",
        flexWrap: "nowrap"
    },
    twoColumnTable: {
        marginTop: "14px",
        marginBottom: "6px",
        "& .fui-Checkbox__indicator": {
            margin: "2px 0 0 0"
        },
        "& .fui-Label": {
            padding: "0 0 0 10px"
        }
    }
});

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

    const styles = useStyles();

    const { handleSubmit, control, reset } = useForm<RemoteCopyFormValues>({ defaultValues });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);
    const [selectedDestGroup, setSelectedDestGroup] = React.useState("");
    const [replicateDisabled, setReplicateDisabled] = React.useState(true);
    const [remoteTargets, setRemoteTargets] = React.useState<{ key: string; text: string; }[]>([]);

    const selectedRemoteStorage = useWatch({ control, name: "remoteStorage" });
    const daliDisabled = !!selectedRemoteStorage;

    React.useEffect(() => {
        fileSprayService.GetRemoteTargets({}).then(response => {
            setRemoteTargets(response?.TargetNames?.Item?.map(item => ({ key: item, text: item })) ?? []);
        }).catch(err => logger.error(err));
    }, []);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);

                const { remoteStorage, sourceLogicalName, srcusername, srcpassword, sourceDali, ...rest } = data;
                const qualifiedName = (remoteStorage && !sourceLogicalName.startsWith("~remote::"))
                    ? `~remote::${remoteStorage}::${sourceLogicalName}`
                    : sourceLogicalName;
                const request = {
                    ...rest,
                    sourceLogicalName: qualifiedName,
                    ...(remoteStorage ? {} : { sourceDali, srcusername, srcpassword }),
                };

                fileSprayService.Copy(request).then((_response) => {
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                    setShowForm(false);
                    reset(defaultValues);
                    if (refreshGrid) refreshGrid(true);
                }).catch(err => {
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                    setShowError(true);
                    setErrorMessage(err.message);
                    logger.error(err);
                });
            },
            err => {
                logger.error(err);
            }
        )();
    }, [handleSubmit, refreshGrid, reset, setShowForm]);

    React.useEffect(() => {
        topologyService.TpGroupQuery({}).then(response => {
            const groups = response?.TpGroups?.TpGroup ?? [];
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
            <Spinner label={nlsHPCC.Loading} labelPosition="after" size="tiny" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={onSubmit}>{nlsHPCC.Copy}</Button>
            <Button onClick={() => { reset(defaultValues); setShowError(false); setShowForm(false); }}>{nlsHPCC.Cancel}</Button>
        </>}>
        {showError &&
            <MessageBar intent="error">
                <MessageBarBody>{errorMessage}</MessageBarBody>
                <MessageBarActions>
                    <Button onClick={() => setShowError(false)} aria-label="dismiss" appearance="transparent" icon={<DismissRegular />} />
                </MessageBarActions>
            </MessageBar>
        }
        <div className={styles.flex}>
            <Controller
                control={control} name="remoteStorage"
                render={({
                    field: { onChange, name: fieldName, value },
                }) => <Field label={nlsHPCC.RemoteStorage}>
                        <Dropdown
                            key={fieldName}
                            selectedOptions={value ? [value] : []}
                            title={remoteTargets.length === 0 ? nlsHPCC.NoRemoteStorageFound : ""}
                            disabled={remoteTargets.length === 0}
                            onOptionSelect={(evt, data) => {
                                onChange(data.optionValue);
                            }}
                        >
                            {remoteTargets.map((target, idx) => (
                                <Option key={target.text} text={target.text} value={target.key}>{target.text}</Option>
                            ))}
                        </Dropdown>
                    </Field>
                }
            />
            <Controller
                control={control} name="sourceDali"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field label={nlsHPCC.RemoteDali}>
                        <Input
                            key={fieldName}
                            value={value}
                            onChange={onChange}
                            disabled={daliDisabled}
                        />
                    </Field>
                }
            />
            <Controller
                control={control} name="srcusername"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field label={nlsHPCC.UserID}>
                        <Input
                            key={fieldName}
                            value={value}
                            onChange={onChange}
                            disabled={daliDisabled}
                        />
                    </Field>
                }
            />
            <Controller
                control={control} name="srcpassword"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field label={nlsHPCC.Password}>
                        <Input
                            type="password"
                            autoComplete="off"
                            key={fieldName}
                            value={value}
                            onChange={onChange}
                            disabled={daliDisabled}
                        />
                    </Field>
                }
            />
            <Controller
                control={control} name="sourceLogicalName"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field
                    label={`${nlsHPCC.Source} ${nlsHPCC.LogicalName}`}
                    validationState={error ? "error" : "none"}
                    validationMessage={error && error.message}
                >
                        <Input
                            key={fieldName}
                            required={true}
                            value={value}
                            onChange={onChange}
                        />
                    </Field>
                }
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
        </div>
        <div className={styles.flex}>
            <Controller
                control={control} name="destGroup"
                render={({
                    field: { onChange, name: fieldName },
                    fieldState: { error }
                }) => <TargetGroupTextField
                        key={fieldName}
                        label={nlsHPCC.Group}
                        required={true}
                        onChange={(evt, option: { key: string; text: string; }) => {
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
                }) => <Field
                    label={`${nlsHPCC.Target} ${nlsHPCC.LogicalName}`}
                    validationState={error ? "error" : "none"}
                    validationMessage={error && error.message}
                >
                        <Input
                            key={fieldName}
                            required={true}
                            value={value}
                            onChange={onChange}
                        />
                    </Field>}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
        </div>
        <div className={styles.flex}>
            <table className={styles.twoColumnTable}>
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
        </div>
    </MessageBox>;

};