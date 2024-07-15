import * as React from "react";
import { Checkbox, DefaultButton, Dropdown, Icon, IDropdownProps, IOnRenderComboBoxLabelProps, IStackTokens, ITextFieldProps, mergeStyleSets, PrimaryButton, Spinner, Stack, TextField, TooltipHost } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import { LogType } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import * as WsWorkunits from "src/WsWorkunits";
import { useBuildInfo } from "../../hooks/platform";
import { MessageBox } from "../../layouts/MessageBox";
import { CloudContainerNameField } from "../forms/Fields";
import nlsHPCC from "src/nlsHPCC";
import { useUserTheme } from "../../hooks/theme";

const logger = scopedLogger("../components/forms/ZAPDialog.tsx");

enum ColumnMode {
    MIN,
    DEFAULT,
    ALL,
    CUSTOM
}

enum LogFormat {
    CSV = "csv",
    JSON = "json",
    XML = "xml"
}

const stackTokens: IStackTokens = {
    childrenGap: 4,
    maxWidth: 300,
};

type CustomLabelProps = ITextFieldProps & IDropdownProps & IOnRenderComboBoxLabelProps & {
    tooltip: string;
}

const CustomLabel = (props: CustomLabelProps): JSX.Element => {
    return <Stack horizontal verticalAlign="center" tokens={stackTokens}>
        <label htmlFor={props.name} id={props.id} style={{ fontWeight: 600, display: "block", padding: "5px 0" }}>{props.label}</label>
        <TooltipHost content={props.tooltip}>
            <Icon iconName="Info" style={{ cursor: "default" }} />
        </TooltipHost>
    </Stack>;
};

interface ZAPDialogValues {
    ZAPFileName: string;
    Wuid: string;
    BuildVersion: string;
    ESPIPAddress: string;
    ESPApplication: string;
    ThorIPAddress: string;
    ThorProcesses: string;
    ProblemDescription: string;
    WhatChanged: string;
    WhereSlow: string;
    Password: string;
    IncludeThorSlaveLog: boolean;
    IncludeRelatedLogs: boolean;
    IncludePerComponentLogs: boolean;
    SendEmail: boolean;
    EmailTo: string;
    EmailFrom: string;
    EmailSubject: string;
    EmailBody: string;
    LogFilter: {
        WildcardFilter?: string;
        AbsoluteTimeRange?: {
            StartDate?: string;
            EndDate?: string;
        };
        RelativeTimeRangeBuffer?: string;
        LineLimit?: string;
        LineStartFrom?: string;
        SelectColumnMode?: ColumnMode;
        CustomColumns?: string;
        ComponentsFilter?: string;
        Format?: string;
        sortByTimeDirection?: string;
        LogEventType?: string;
    };
}

const defaultValues: ZAPDialogValues = {
    ZAPFileName: "",
    Wuid: "",
    BuildVersion: "",
    ESPIPAddress: "",
    ESPApplication: "",
    ThorIPAddress: "",
    ThorProcesses: "",
    ProblemDescription: "",
    WhatChanged: "",
    WhereSlow: "",
    Password: "",
    IncludeThorSlaveLog: true,
    IncludeRelatedLogs: true,
    IncludePerComponentLogs: false,
    SendEmail: false,
    EmailTo: "",
    EmailFrom: "",
    EmailSubject: "",
    EmailBody: "",
    LogFilter: {
        WildcardFilter: "",
        AbsoluteTimeRange: {
            StartDate: "",
            EndDate: "",
        },
        RelativeTimeRangeBuffer: "",
        LineLimit: "10000",
        LineStartFrom: "0",
        SelectColumnMode: ColumnMode.DEFAULT,
        CustomColumns: "",
        ComponentsFilter: "",
        Format: "CSV",
        sortByTimeDirection: "1",
        LogEventType: "ALL"
    }
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

    const { theme } = useUserTheme();

    const formClasses = React.useMemo(() => mergeStyleSets({
        label: {
            fontSize: 14,
            fontWeight: 600,
            color: theme.palette.neutralTertiary,
            boxSizing: "border-box",
            margin: 0,
            display: "block",
            padding: "5px 0px"
        },
        input: {
            fontSize: 14,
            fontWeight: 400,
            height: 32,
            margin: 0,
            padding: "0px 8px",
            boxSizing: "border-box",
            borderRadius: 2,
            border: `1px solid ${theme.palette.neutralSecondary}`,
            background: "none transparent",
            color: theme.palette.neutralPrimary,
            width: "100%",
            textOverflow: "ellipsis",
            outline: 0
        },
        "errorMessage": {
            fontSize: 12,
            margin: 0,
            paddingTop: 5,
            color: theme.semanticColors.errorText
        }
    }), [theme]);

    const [emailDisabled, setEmailDisabled] = React.useState(true);
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);
    const [columnMode, setColumnMode] = React.useState(ColumnMode.DEFAULT);
    const [logFormat, setLogFormat] = React.useState(LogFormat.CSV);
    const [showCustomColumns, setShowCustomColumns] = React.useState(false);
    const [logAccessorMessage, setLogAccessorMessage] = React.useState("");

    const { handleSubmit, control, reset } = useForm<ZAPDialogValues>({ defaultValues });

    const [, { isContainer }] = useBuildInfo();

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const formData = new FormData();
                const logFilter = data.LogFilter;

                delete data.LogFilter;
                setSubmitDisabled(true);
                setSpinnerHidden(false);

                for (const key in data) {
                    formData.append(key, data[key]);
                }
                for (const key in logFilter) {
                    if (key === "AbsoluteTimeRange") {
                        const startDate = logFilter.AbsoluteTimeRange.StartDate ? new Date(logFilter.AbsoluteTimeRange.StartDate).toISOString() : "";
                        const endDate = logFilter.AbsoluteTimeRange.EndDate ? new Date(logFilter.AbsoluteTimeRange.EndDate).toISOString() : "";
                        formData.append("LogFilter_AbsoluteTimeRange_StartDate", startDate);
                        formData.append("LogFilter_AbsoluteTimeRange_EndDate", endDate);
                    } else {
                        formData.append(`LogFilter_${key}`, logFilter[key]);
                    }
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
                        const headers = file?.filename?.split(";") ?? [];
                        for (const header of headers) {
                            if (header.trim().indexOf("filename=") > -1) {
                                filename = header.replace("filename=", "");
                            }
                        }
                        const urlObj = window.URL.createObjectURL(file?.blob);

                        const link = document.createElement("a");
                        link.href = urlObj;
                        link.download = filename;
                        link.click();
                        link.remove();

                        setSubmitDisabled(false);
                        setSpinnerHidden(true);
                        closeForm();

                        if (logAccessorMessage !== "") {
                            logger.warning(logAccessorMessage);
                        }
                    })
                    .catch(err => logger.error(err))
                    ;
            },
            logger.info
        )();
    }, [closeForm, handleSubmit, logAccessorMessage]);

    React.useEffect(() => {
        WsWorkunits.WUGetZAPInfo({ request: { WUID: wuid } }).then(response => {
            setEmailDisabled(response?.WUGetZAPInfoResponse?.EmailTo === null);
            setLogAccessorMessage(response?.WUGetZAPInfoResponse?.Message ?? "");
            delete response?.WUGetZAPInfoResponse?.Archive;
            const newValues = { ...defaultValues, ...response?.WUGetZAPInfoResponse, ...{ Wuid: wuid } };
            for (const key in newValues) {
                if (newValues[key] === null) {
                    newValues[key] = "";
                }
            }
            reset(newValues);
        }).catch(err => logger.error(err));
    }, [wuid, reset]);

    return <MessageBox title={nlsHPCC.ZippedAnalysisPackage} minWidth={440} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.LoadingData} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Submit} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Controller
            control={control} name="ZAPFileName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.FileName}
                    value={value}
                    errorMessage={error && error?.message}
                />}
            rules={{
                pattern: {
                    value: /^[-a-z0-9_\.]+$/i,
                    message: nlsHPCC.ValidationErrorTargetNameInvalid
                }
            }}
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
            control={control} name={isContainer ? "ESPApplication" : "ESPIPAddress"}
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={isContainer ? nlsHPCC.ESPProcessName : nlsHPCC.ESPNetworkAddress}
                    value={value}
                />}
        />
        <Controller
            control={control} name={isContainer ? "ThorProcesses" : "ThorIPAddress"}
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={isContainer ? nlsHPCC.ThorProcess : nlsHPCC.ThorNetworkAddress}
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
            {!isContainer
                ? <div>
                    <Controller
                        control={control} name="IncludeThorSlaveLog"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.IncludeSlaveLogs} />}
                    />
                </div>
                : <div>
                    <div style={{ marginBottom: 4 }}>
                        <Controller
                            control={control} name="IncludeRelatedLogs"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.IncludeRelatedLogs} />}
                        />
                    </div>
                    <div>
                        <Controller
                            control={control} name="IncludePerComponentLogs"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.IncludePerComponentLogs} />}
                        />
                    </div>
                </div>
            }
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
        <fieldset style={{ marginTop: 8 }}>
            <legend>{nlsHPCC.LogFilters}</legend>
            <Controller
                control={control} name="LogFilter.AbsoluteTimeRange.StartDate"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <div>
                        <Stack horizontal verticalAlign="center" tokens={stackTokens}>
                            <label htmlFor={fieldName} className={formClasses.label}>{nlsHPCC.FromDate}</label>
                            <TooltipHost content={nlsHPCC.LogFilterStartDateTooltip}>
                                <Icon iconName="Info" style={{ cursor: "default" }} />
                            </TooltipHost>
                        </Stack>
                        <TooltipHost content={nlsHPCC.LogFilterStartDateTooltip}>
                            <input
                                key={fieldName}
                                type="datetime-local"
                                name={fieldName}
                                className={formClasses.input}
                                defaultValue={value}
                                onChange={onChange}
                            />
                            <p className={formClasses.errorMessage}>{error && error?.message}</p>
                        </TooltipHost>
                    </div>
                }
                rules={{
                    validate: {
                        hasValue: (value, formValues) => {
                            if (value === "" && formValues.LogFilter.RelativeTimeRangeBuffer === "") {
                                return nlsHPCC.LogFilterTimeRequired;
                            }
                            return true;
                        }
                    }
                }}
            />
            <Controller
                control={control} name="LogFilter.AbsoluteTimeRange.EndDate"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <div>
                        <Stack horizontal verticalAlign="center" tokens={stackTokens}>
                            <label htmlFor={fieldName} className={formClasses.label}>{nlsHPCC.ToDate}</label>
                            <TooltipHost content={nlsHPCC.LogFilterEndDateTooltip}>
                                <Icon iconName="Info" style={{ cursor: "default" }} />
                            </TooltipHost>
                        </Stack>
                        <input
                            key={fieldName}
                            type="datetime-local"
                            name={fieldName}
                            className={formClasses.input}
                            defaultValue={value}
                            onChange={onChange}
                        />
                    </div>
                }
            />
            <Controller
                control={control} name="LogFilter.RelativeTimeRangeBuffer"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.RelativeTimeRange}
                        onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                            id={`${fieldName}_Label`}
                            tooltip={nlsHPCC.LogFilterRelativeTimeRangeTooltip}
                            {...props}
                        />}
                        value={value}
                        errorMessage={error && error?.message}
                    />
                }
                rules={{
                    validate: {
                        hasValue: (value, formValues) => {
                            if (value === "" && formValues.LogFilter.AbsoluteTimeRange.StartDate === "") {
                                return nlsHPCC.LogFilterTimeRequired;
                            }
                            return true;
                        }
                    }
                }}

            />
            <Controller
                control={control} name="LogFilter.LineLimit"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.LogLineLimit}
                        onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                            id={`${fieldName}_Label`}
                            tooltip={nlsHPCC.LogFilterLineLimitTooltip}
                            {...props}
                        />}
                        value={value}
                    />
                }
            />
            <Controller
                control={control} name="LogFilter.LineStartFrom"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.LogLineStartFrom}
                        onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                            id={`${fieldName}_Label`}
                            tooltip={nlsHPCC.LogFilterLineStartFromTooltip}
                            {...props}
                        />}
                        value={value}
                    />
                }
            />
            <Controller
                control={control} name="LogFilter.SelectColumnMode"
                render={({
                    field: { name: fieldName }
                }) => <Dropdown
                        key={fieldName}
                        label={nlsHPCC.ColumnMode}
                        options={[
                            { key: ColumnMode.MIN, text: "MIN" },
                            { key: ColumnMode.DEFAULT, text: "DEFAULT" },
                            { key: ColumnMode.ALL, text: "ALL" },
                            { key: ColumnMode.CUSTOM, text: "CUSTOM" }
                        ]}
                        selectedKey={columnMode}
                        onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                            id={`${fieldName}_Label`}
                            tooltip={nlsHPCC.LogFilterSelectColumnModeTooltip}
                            {...props}
                        />}
                        onChange={(evt, option) => {
                            setShowCustomColumns(option.key === ColumnMode.CUSTOM ? true : false);
                            setColumnMode(option.key as ColumnMode);
                        }}
                    />
                }
            />
            <div style={{ display: showCustomColumns ? "block" : "none" }}>
                <Controller
                    control={control} name="LogFilter.CustomColumns"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <TextField
                            name={fieldName}
                            onChange={onChange}
                            label={nlsHPCC.CustomLogColumns}
                            onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                                id={`${fieldName}_Label`}
                                tooltip={nlsHPCC.LogFilterCustomColumnsTooltip}
                                {...props}
                            />}
                            multiline={true}
                            value={value}
                        />
                    }
                />
            </div>
            <Controller
                control={control} name="LogFilter.ComponentsFilter"
                render={({
                    field: { onChange, name: fieldName }
                }) => <CloudContainerNameField
                        name={fieldName}
                        onChange={(_evt, option, _idx, value) => {
                            if (option?.key) {
                                onChange(option.key);
                            } else {
                                onChange(value);
                            }
                        }}
                        onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                            id={`${fieldName}_Label`}
                            label={nlsHPCC.ContainerName}
                            tooltip={nlsHPCC.LogFilterComponentsFilterTooltip}
                            {...props}
                        />}
                    />
                }
            />
            <Controller
                control={control} name="LogFilter.Format"
                render={({
                    field: { name: fieldName }
                }) => <Dropdown
                        key={fieldName}
                        label={nlsHPCC.LogFormat}
                        options={[
                            { key: LogFormat.CSV, text: "CSV" },
                            { key: LogFormat.JSON, text: "JSON" },
                            { key: LogFormat.XML, text: "XML" }
                        ]}
                        selectedKey={logFormat}
                        onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                            id={`${fieldName}_Label`}
                            tooltip={nlsHPCC.LogFilterFormatTooltip}
                            {...props}
                        />}
                        onChange={(evt, option) => {
                            setLogFormat(option.key as LogFormat);
                        }}
                    />
                }
            />
            <Controller
                control={control} name="LogFilter.WildcardFilter"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.WildcardFilter}
                        onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                            id={`${fieldName}_Label`}
                            tooltip={nlsHPCC.LogFilterWildcardFilterTooltip}
                            {...props}
                        />}
                        value={value}
                    />
                }
            />
            <Controller
                control={control} name="LogFilter.sortByTimeDirection"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Dropdown
                        key={fieldName}
                        label={`${nlsHPCC.Sort} (${nlsHPCC.TimeStamp})`}
                        options={[
                            { key: "0", text: "ASC" },
                            { key: "1", text: "DESC" },
                        ]}
                        defaultSelectedKey={"1"}
                        selectedKey={value}
                        onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                            id={`${fieldName}_Label`}
                            tooltip={nlsHPCC.LogFilterSortByTooltip}
                            {...props}
                        />}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                    />
                }
            />
            <Controller
                control={control} name="LogFilter.LogEventType"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Dropdown
                        key={fieldName}
                        label={nlsHPCC.LogEventType}
                        options={[
                            { key: "ALL", text: "All" },
                            { key: LogType.Disaster, text: "Disaster" },
                            { key: LogType.Error, text: "Error" },
                            { key: LogType.Warning, text: "Warning" },
                            { key: LogType.Information, text: "Information" },
                            { key: LogType.Progress, text: "Progress" },
                            { key: LogType.Metric, text: "Metric" },
                        ]}
                        selectedKey={value}
                        onRenderLabel={(props: CustomLabelProps) => <CustomLabel
                            id={`${fieldName}_Label`}
                            tooltip={nlsHPCC.LogFilterEventTypeTooltip}
                            {...props}
                        />}
                        onChange={(evt, option) => {
                            onChange(option.key);
                        }}
                    />
                }
            />
        </fieldset>
    </MessageBox>;
};