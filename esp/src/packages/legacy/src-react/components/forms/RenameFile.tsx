import * as React from "react";
import { Checkbox, DefaultButton, mergeStyleSets, PrimaryButton, Spinner, Stack, TextField, } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import { FileSprayService, FileSprayStates } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";
import { pushUrl, replaceUrl } from "../../util/history";
import * as FormStyles from "./landing-zone/styles";

const logger = scopedLogger("src-react/components/forms/RenameFile.tsx");

interface RenameFileFormValues {
    targetRenameFile?: {
        name: string
    }[],
    overwrite: boolean;
}

const defaultValues: RenameFileFormValues = {
    overwrite: false
};

interface RenameFileProps {
    logicalFiles: string[];

    showForm: boolean;
    setShowForm: (_: boolean) => void;

    refreshGrid?: (_?: boolean) => void;
}

export const RenameFile: React.FunctionComponent<RenameFileProps> = ({
    logicalFiles,
    showForm,
    setShowForm,
    refreshGrid
}) => {

    const { handleSubmit, control, reset } = useForm<RenameFileFormValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const service = useConst(() => new FileSprayService({ baseUrl: "" }));

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            async (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);
                const renameRequests = [];
                const getDfuWuRequests = [];

                logicalFiles.forEach((logicalFile, idx) => {
                    const request = { ...data, srcname: logicalFile, dstname: data.targetRenameFile[idx].name, DFUServerQueue: "" };
                    renameRequests.push(service.Rename(request));
                });

                const renameResponses = await Promise.all(renameRequests);
                renameResponses.forEach(response => {
                    const wuid = response?.wuid ?? null;
                    if (wuid) {
                        getDfuWuRequests.push(service.GetDFUWorkunit({ wuid }));
                    }
                });

                const getDfuWuResponses = await Promise.all(getDfuWuRequests);
                getDfuWuResponses.forEach(response => {
                    const State = response?.result?.State ?? FileSprayStates.unknown;
                    const ID = response?.result?.ID;

                    if (State === FileSprayStates.failed) {
                        logger.error(response?.result?.SummaryMessage ?? "");
                        if (getDfuWuResponses.length === 1 && ID) {
                            pushUrl(`/dfuworkunits/${ID}`);
                        }
                    } else if (ID) {
                        if (getDfuWuResponses.length === 1) {
                            if (window.location.hash.match(/#\/files\//) === null) {
                                pushUrl(`/dfuworkunits/${ID}`);
                            } else {
                                replaceUrl(`/dfuworkunits/${ID}`);
                            }
                        } else {
                            window.setTimeout(function () { window.open(`#/dfuworkunits/${ID}`); }, 0);
                        }
                    }
                });
                setSubmitDisabled(false);
                setSpinnerHidden(true);
                closeForm();
                if (refreshGrid) refreshGrid(true);
            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, handleSubmit, logicalFiles, refreshGrid, service]);

    const componentStyles = mergeStyleSets(
        FormStyles.componentStyles,
        {
            container: {
                minWidth: 440,
            }
        }
    );

    React.useEffect(() => {
        const _files = [];
        logicalFiles.forEach(file => {
            _files.push({ name: file });
        });
        const newValues = { ...defaultValues, targetRenameFile: _files };
        reset(newValues);
    }, [logicalFiles, reset]);

    return <MessageBox title={nlsHPCC.Rename} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Rename} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Stack>
            {logicalFiles?.length === 1 &&
                <Controller
                    control={control} name="targetRenameFile.0.name"
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
            }
            {logicalFiles?.length > 1 &&
                <Stack>
                    <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`}>
                        <thead>
                            <tr>
                                <th>{nlsHPCC.TargetName}</th>
                            </tr>
                        </thead>
                        <tbody>
                            {logicalFiles.map((file, idx) => {
                                return <tr key={`File-${idx}`}>
                                    <td>
                                        <Controller
                                            control={control} name={`targetRenameFile.${idx}.name` as const}
                                            render={({
                                                field: { onChange, name: fieldName, value: file },
                                                fieldState: { error }
                                            }) => <TextField
                                                    name={fieldName}
                                                    onChange={onChange}
                                                    value={file}
                                                    errorMessage={error && error?.message}
                                                />}
                                            rules={{
                                                required: nlsHPCC.ValidationErrorTargetNameRequired
                                            }}
                                        />
                                    </td>
                                </tr>;
                            })}
                        </tbody>
                    </table>
                </Stack>
            }
            <div style={{ paddingTop: "15px" }}>
                <Controller
                    control={control} name="overwrite"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Overwrite} />}
                />
            </div>
        </Stack>
    </MessageBox>;
};
