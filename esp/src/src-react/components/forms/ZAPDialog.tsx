import * as React from "react";
import { Checkbox, ContextualMenu, IconButton, IDragOptions, mergeStyleSets, Modal, PrimaryButton, Stack, TextField, } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { useForm, Controller } from "react-hook-form";
import { scopedLogger } from "@hpcc-js/util";
import * as WsWorkunits from "src/WsWorkunits";
import * as FormStyles from "./landing-zone/styles";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/forms/ZAPDialog.tsx");

interface ZAPDialogValues {
    ZAPFileName: string;
    Wuid: string;
    BuildVersion: string;
    ESPIPAddress: string;
    ThorIPAddress: string;
    ProblemDescription: string;
    WhatChanged: string;
    WhereSlow: string;
    Password: string;
    IncludeThorSlaveLog: boolean;
    SendEmail: boolean;
    EmailTo: string;
    EmailFrom: string;
    EmailSubject: string;
    EmailBody: string;
}

const defaultValues: ZAPDialogValues = {
    ZAPFileName: "",
    Wuid: "",
    BuildVersion: "",
    ESPIPAddress: "",
    ThorIPAddress: "",
    ProblemDescription: "",
    WhatChanged: "",
    WhereSlow: "",
    Password: "",
    IncludeThorSlaveLog: true,
    SendEmail: false,
    EmailTo: "",
    EmailFrom: "",
    EmailSubject: "",
    EmailBody: ""
};

interface ZAPDialogProps {
    wuid?: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const ZAPDialog: React.FunctionComponent<ZAPDialogProps> = ({
    wuid,
    showForm,
    setShowForm
}) => {

    const [emailDisabled, setEmailDisabled] = React.useState(true);

    const { handleSubmit, control, reset } = useForm<ZAPDialogValues>({ defaultValues });

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const formData = new FormData();

                for (const key in data) {
                    formData.append(key, data[key]);
                }

                fetch("/WsWorkunits/WUCreateAndDownloadZAPInfo", {
                    method: "POST",
                    body: formData
                })
                    .then(async response => ({
                        filename: response.headers.get("Content-Disposition"),
                        blob: await response.blob()
                    }))
                    .then(file => {
                        let filename = "";
                        const headers = file.filename.split(";");
                        for (const header of headers) {
                            if (header.trim().indexOf("filename=") > -1) {
                                filename = header.replace("filename=", "");
                            }
                        }
                        const urlObj = window.URL.createObjectURL(file.blob);

                        const link = document.createElement("a");
                        link.href = urlObj;
                        link.download = filename;
                        link.click();
                        link.remove();

                        closeForm();
                        reset(defaultValues);
                    })
                    .catch(logger.error)
                    ;
            },
            logger.info
        )();
    }, [closeForm, handleSubmit, reset]);

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
        WsWorkunits.WUGetZAPInfo({ request: { WUID: wuid } }).then(response => {
            setEmailDisabled(response?.WUGetZAPInfoResponse?.EmailTo === null);
            const newValues = { ...defaultValues, ...response?.WUGetZAPInfoResponse, ...{ Wuid: wuid } };
            for (const key in newValues) {
                if (newValues[key] === null) {
                    newValues[key] = "";
                }
            }
            reset(newValues);
        }).catch(logger.error);
    }, [wuid, reset]);

    return <Modal
        titleAriaId={titleId}
        isOpen={showForm}
        onDismiss={closeForm}
        isBlocking={false}
        containerClassName={componentStyles.container}
        dragOptions={dragOptions}
    >
        <div className={componentStyles.header}>
            <span id={titleId}>{nlsHPCC.ZippedAnalysisPackage}</span>
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
                    control={control} name="ZAPFileName"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.FileName}
                            value={value}
                        />}
                />
                <Controller
                    control={control} name="Wuid"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.WUID}
                            value={value}
                        />}
                />
                <Controller
                    control={control} name="BuildVersion"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.ESPBuildVersion}
                            value={value}
                        />}
                />
                <Controller
                    control={control} name="ESPIPAddress"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.ESPNetworkAddress}
                            value={value}
                        />}
                />
                <Controller
                    control={control} name="ThorIPAddress"
                    render={({
                        field: { onChange, name: fieldName, value },
                        fieldState: { error }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.ThorNetworkAddress}
                            value={value}
                        />}
                />
                <Controller
                    control={control} name="ProblemDescription"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.Description}
                            multiline={true}
                            value={value}
                        />}
                />
                <Controller
                    control={control} name="WhatChanged"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.History}
                            multiline={true}
                            value={value}
                        />}
                />
                <Controller
                    control={control} name="WhereSlow"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.Timings}
                            multiline={true}
                            value={value}
                        />}
                />
                <Controller
                    control={control} name="Password"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.PasswordOpenZAP}
                            value={value}
                            type="password"
                            canRevealPassword={true}
                            revealPasswordAriaLabel={nlsHPCC.ShowPassword}
                        />}
                />
                <div style={{ padding: "15px 0 7px 0" }}>
                    <div>
                        <Controller
                            control={control} name="IncludeThorSlaveLog"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.IncludeSlaveLogs} />}
                        />
                    </div>
                    <div style={{ paddingTop: "10px" }}>
                        <Controller
                            control={control} name="SendEmail"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox
                                    name={fieldName}
                                    checked={value}
                                    onChange={onChange}
                                    label={nlsHPCC.SendEmail}
                                    disabled={emailDisabled}
                                />}
                        />
                    </div>

                </div>
                <Controller
                    control={control} name="EmailTo"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.EmailTo}
                            value={value}
                            placeholder={nlsHPCC.SeeConfigurationManager}
                            disabled={emailDisabled}
                        />}
                />
                <Controller
                    control={control} name="EmailFrom"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.EmailFrom}
                            value={value}
                            placeholder={nlsHPCC.SeeConfigurationManager}
                            disabled={emailDisabled}
                        />}
                />
                <Controller
                    control={control} name="EmailSubject"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.EmailSubject}
                            value={value}
                            disabled={emailDisabled}
                        />}
                />
                <Controller
                    control={control} name="EmailBody"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.EmailBody}
                            multiline={true}
                            value={value}
                            disabled={emailDisabled}
                        />}
                />
            </Stack>
            <Stack horizontal horizontalAlign="space-between" verticalAlign="end" styles={FormStyles.buttonStackStyles}>
                <PrimaryButton text={nlsHPCC.Submit} onClick={handleSubmit(onSubmit)} />
            </Stack>
        </div>
    </Modal>;
};