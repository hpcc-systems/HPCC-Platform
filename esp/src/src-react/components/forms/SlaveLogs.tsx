import * as React from "react";
import { ContextualMenu, Dropdown, IconButton, IDragOptions, mergeStyleSets, Modal, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as FormStyles from "./landing-zone/styles";
import { useWorkunit } from "../../hooks/Workunit";

interface SlaveLogsValues {
    ThorProcess: string;
    SlaveNumber: string;
    FileFormat: string;
}

const defaultValues: SlaveLogsValues = {
    ThorProcess: "",
    SlaveNumber: "1",
    FileFormat: "1"
};

interface SlaveLogsProps {
    wuid?: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const SlaveLogs: React.FunctionComponent<SlaveLogsProps> = ({
    wuid,
    showForm,
    setShowForm
}) => {

    const [workunit] = useWorkunit(wuid);

    const [thorProcesses, setThorProcesses] = React.useState([]);
    const [maxThorSlaves, setMaxThorSlaves] = React.useState(1);
    const [thorLogDate, setThorLogDate] = React.useState("");
    const [clusterGroup, setClusterGroup] = React.useState("");

    const { handleSubmit, control, reset } = useForm<SlaveLogsValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const url = `/WsWorkunits/WUFile?Wuid=${wuid}&Type=ThorSlaveLog&Process=${data.ThorProcess}` +
                    `&ClusterGroup=${clusterGroup}&LogDate=${thorLogDate}&SlaveNumber=${data.SlaveNumber}&Option=${data.FileFormat}`;
                window.open(url);
                closeForm();
                reset(defaultValues);
            },
            err => {
                console.log(err);
            }
        )();
    }, [closeForm, clusterGroup, handleSubmit, reset, thorLogDate, wuid]);

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
                minWidth: 440,
            }
        }
    );

    React.useEffect(() => {
        if (!workunit?.ThorLogList) return;
        setThorProcesses(workunit?.ThorLogList?.ThorLogInfo.map(process => {
            return { key: process.ProcessName, text: process.ProcessName };
        }));
        setMaxThorSlaves(workunit?.ThorLogList?.ThorLogInfo[0].NumberSlaves || 1);
        setThorLogDate(workunit?.ThorLogList?.ThorLogInfo[0].LogDate);
        setClusterGroup(workunit?.ThorLogList?.ThorLogInfo[0].ProcessName);
    }, [workunit]);

    return <Modal
        titleAriaId={titleId}
        isOpen={showForm}
        onDismiss={closeForm}
        isBlocking={false}
        containerClassName={componentStyles.container}
        dragOptions={dragOptions}
    >
        <div className={componentStyles.header}>
            <span id={titleId}>{nlsHPCC.SlaveLogs}</span>
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
                    control={control} name="ThorProcess"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <Dropdown
                        key={fieldName}
                        label={nlsHPCC.ThorProcess}
                        options={thorProcesses}
                        required={true}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                        errorMessage={ error && error.message }
                    /> }
                    rules={{
                        required: nlsHPCC.ValidationErrorRequired
                    }}
                />
                <Controller
                    control={control} name="SlaveNumber"
                    render={({
                        field: { onChange, name: fieldName, value},
                        fieldState: { error }
                    }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.SlaveNumber}
                        value={value}
                        errorMessage={ error && error.message }
                    /> }
                    rules={{
                        pattern: {
                            value: /^[1-9]+$/i,
                            message: nlsHPCC.ValidationErrorEnterNumber
                        },
                        min: {
                            value: 1,
                            message: `${nlsHPCC.ValidationErrorNumberLess} 1`
                        },
                        max: {
                            value: maxThorSlaves,
                            message: `${nlsHPCC.ValidationErrorNumberGreater} ${maxThorSlaves}`
                        }
                    }}
                />
                <Controller
                    control={control} name="FileFormat"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <Dropdown
                        key={fieldName}
                        label={nlsHPCC.File}
                        options={[
                            { key: "1", text: nlsHPCC.OriginalFile },
                            { key: "2", text: nlsHPCC.Zip },
                            { key: "3", text: nlsHPCC.GZip },
                        ]}
                        defaultSelectedKey="1"
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                    /> }
                />
            </Stack>
            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={FormStyles.buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Download} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};