import * as React from "react";
import { Button, Dropdown, Field, Input, Option } from "@fluentui/react-components";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunit } from "../../hooks/workunit";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("../components/forms/SlaveLogs.tsx");

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

    const { workunit } = useWorkunit(wuid);

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
            logger.info
        )();
    }, [closeForm, clusterGroup, handleSubmit, reset, thorLogDate, wuid]);

    React.useEffect(() => {
        if (!workunit?.ThorLogList) return;
        setThorProcesses(workunit?.ThorLogList?.ThorLogInfo.map(process => {
            return { key: process.ProcessName, text: process.ProcessName };
        }));
        setMaxThorSlaves(workunit?.ThorLogList?.ThorLogInfo[0].NumberSlaves || 1);
        setThorLogDate(workunit?.ThorLogList?.ThorLogInfo[0].LogDate);
        setClusterGroup(workunit?.ThorLogList?.ThorLogInfo[0].ProcessName);
    }, [workunit]);

    return <MessageBox title={nlsHPCC.SlaveLogs} show={showForm} setShow={closeForm}
        footer={<>
            <Button appearance="primary" onClick={handleSubmit(onSubmit)}>{nlsHPCC.Download}</Button>
            <Button onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="ThorProcess"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.ThorProcess} required validationMessage={error?.message}>
                    <Dropdown
                        key={fieldName}
                        selectedOptions={value ? [value] : []}
                        onOptionSelect={(_evt, data) => {
                            onChange(data.optionValue);
                        }}
                    >
                        {thorProcesses.map((opt: { key: string; text: string; }) => (
                            <Option key={opt.key} text={opt.text} value={opt.key}>{opt.text}</Option>
                        ))}
                    </Dropdown>
                </Field>}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="SlaveNumber"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.SlaveNumber} validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
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
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.File}>
                    <Dropdown
                        key={fieldName}
                        selectedOptions={value ? [value] : []}
                        onOptionSelect={(_evt, data) => {
                            onChange(data.optionValue);
                        }}
                    >
                        <Option key="1" text={nlsHPCC.OriginalFile} value="1">{nlsHPCC.OriginalFile}</Option>
                        <Option key="2" text={nlsHPCC.Zip} value="2">{nlsHPCC.Zip}</Option>
                        <Option key="3" text={nlsHPCC.GZip} value="3">{nlsHPCC.GZip}</Option>
                    </Dropdown>
                </Field>}
        />
    </MessageBox>;
};