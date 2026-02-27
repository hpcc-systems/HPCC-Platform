import * as React from "react";
import { Button, Checkbox, Dropdown, Field, Input, Label, makeStyles, Option, Spinner, Textarea, Tooltip } from "@fluentui/react-components";
import { Info16Regular } from "@fluentui/react-icons";
import { useForm, Controller } from "react-hook-form";
import { LogType } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import * as WsWorkunits from "src/WsWorkunits";
import { useBuildInfo, useLogAccessInfo } from "../../hooks/platform";
import { MessageBox } from "../../layouts/MessageBox";
import { CloudContainerNameField } from "../forms/Fields";
import nlsHPCC from "src/nlsHPCC";

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

type CustomLabelProps = {
    label: string;
    name?: string;
    disabled?: boolean;
    id?: string;
    tooltip: string;
}

const useCustomLabelStyles = makeStyles({
    root: {
        display: "flex",
        alignItems: "center",
        gap: "4px",
        maxWidth: "300px"
    },
    label: {
        padding: "5px 0"
    },
    icon: {
        cursor: "default"
    }
});

const CustomLabel = (props: CustomLabelProps): React.JSX.Element => {
    const styles = useCustomLabelStyles();
    return <div className={styles.root}>
        <Label htmlFor={props.name} disabled={props.disabled} id={props.id} weight="semibold" className={styles.label}>{props.label}</Label>
        <Tooltip content={props.tooltip} relationship="label">
            <Info16Regular className={styles.icon} />
        </Tooltip>
    </div>;
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
        Format?: LogFormat;
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
        RelativeTimeRangeBuffer: "43200",
        LineLimit: "10000",
        LineStartFrom: "0",
        SelectColumnMode: ColumnMode.DEFAULT,
        CustomColumns: "",
        ComponentsFilter: "",
        Format: LogFormat.CSV,
        sortByTimeDirection: "1",
        LogEventType: "ALL"
    }
};

interface ZAPDialogProps {
    wuid?: string;

    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

const useFormStyles = makeStyles({
    input: {
        fontSize: "14px",
        fontWeight: "400",
        height: "32px",
        margin: 0,
        padding: "0px 8px",
        boxSizing: "border-box",
        borderRadius: "2px",
        border: "1px solid var(--colorNeutralStroke1)",
        backgroundColor: "transparent",
        color: "var(--colorNeutralForeground1)",
        width: "100%",
        outline: 0
    },
    disabled: {
        backgroundColor: "var(--colorNeutralBackgroundDisabled)",
        border: "1px solid var(--colorNeutralStrokeDisabled)"
    }
});

const useDialogStyles = makeStyles({
    spinner: {
        display: "flex"
    },
    spinnerHidden: {
        display: "none"
    },
    checkboxContainer: {
        padding: "15px 0 7px 0"
    },
    checkboxItem: {
        marginBottom: "4px"
    },
    emailCheckbox: {
        paddingTop: "10px"
    },
    fieldset: {
        marginTop: "8px"
    },
    fieldsetIcon: {
        cursor: "default",
        margin: "0 1px 0 4px"
    },
    customColumnsContainer: {
        display: "block"
    },
    customColumnsContainerHidden: {
        display: "none"
    }
});

export const ZAPDialog: React.FunctionComponent<ZAPDialogProps> = ({
    wuid,
    showForm,
    setShowForm
}) => {

    const formClasses = useFormStyles();
    const dialogStyles = useDialogStyles();

    const [emailDisabled, setEmailDisabled] = React.useState(true);
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);
    const [showCustomColumns, setShowCustomColumns] = React.useState(false);
    const [logAccessorMessage, setLogAccessorMessage] = React.useState("");

    const { handleSubmit, control, reset } = useForm<ZAPDialogValues>({ defaultValues });

    const [, { isContainer }] = useBuildInfo();

    const [logFiltersUnavailable, setLogFiltersUnavailable] = React.useState(false);
    const { logsEnabled, logsStatusMessage } = useLogAccessInfo();

    const logFiltersUnavailableMessage = React.useMemo(() => {
        let retVal = "";
        if (!isContainer) {
            retVal = nlsHPCC.UnavailableInBareMetal;
        }
        if (logsStatusMessage !== "") {
            retVal = logsStatusMessage;
        }
        return retVal;
    }, [isContainer, logsStatusMessage]);

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

                if (logsEnabled) {
                    for (const key in logFilter) {
                        if (key === "AbsoluteTimeRange") {
                            const startDate = logFilter.AbsoluteTimeRange.StartDate ? new Date(logFilter.AbsoluteTimeRange.StartDate) : null;
                            let endDate = logFilter.AbsoluteTimeRange.EndDate ? new Date(logFilter.AbsoluteTimeRange.EndDate) : null;
                            if (startDate) {
                                endDate = endDate === null ? new Date(startDate.getTime() + (8 * 3600 * 1000)) : endDate;
                                formData.append("LogFilter_AbsoluteTimeRange_StartDate", startDate.toISOString());
                                formData.append("LogFilter_AbsoluteTimeRange_EndDate", endDate.toISOString());
                                delete logFilter.RelativeTimeRangeBuffer;
                            }
                        } else {
                            formData.append(`LogFilter_${key}`, logFilter[key]);
                        }
                    }
                } else {
                    data.IncludePerComponentLogs = false;
                    data.IncludeRelatedLogs = false;
                    data.IncludeThorSlaveLog = false;
                }

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
    }, [closeForm, handleSubmit, logAccessorMessage, logsEnabled]);

    React.useEffect(() => {
        if (!showForm) return;
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
    }, [wuid, reset, showForm]);

    React.useEffect(() => {
        if (!logsEnabled) { setLogFiltersUnavailable(true); }
        else { setLogFiltersUnavailable(false); }
    }, [logsEnabled]);

    return <MessageBox title={nlsHPCC.ZippedAnalysisPackage} minWidth={440} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.LoadingData} labelPosition="after" className={spinnerHidden ? dialogStyles.spinnerHidden : dialogStyles.spinner} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Submit}</Button>
            <Button onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="ZAPFileName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field
                label={nlsHPCC.FileName}
                validationMessage={error?.message}
                validationState={error ? "error" : undefined}
            >
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
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
            }) => <Field label={nlsHPCC.WUID}>
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name="BuildVersion"
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.ESPBuildVersion}>
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name={isContainer ? "ESPApplication" : "ESPIPAddress"}
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={isContainer ? nlsHPCC.ESPProcessName : nlsHPCC.ESPNetworkAddress}>
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name={isContainer ? "ThorProcesses" : "ThorIPAddress"}
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={isContainer ? nlsHPCC.ThorProcess : nlsHPCC.ThorNetworkAddress}>
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name="ProblemDescription"
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.Description}>
                    <Textarea
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name="WhatChanged"
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.History}>
                    <Textarea
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name="WhereSlow"
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.Timings}>
                    <Textarea
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                    />
                </Field>}
        />
        <Controller
            control={control} name="Password"
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.PasswordOpenZAP}>
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                        type="password"
                        autoComplete="off"
                    />
                </Field>}
        />
        <div className={dialogStyles.checkboxContainer}>
            {!isContainer
                ? <div>
                    <Controller
                        control={control} name="IncludeThorSlaveLog"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.IncludeSlaveLogs} />}
                    />
                </div>
                : <div>
                    <div className={dialogStyles.checkboxItem}>
                        <Controller
                            control={control} name="IncludeRelatedLogs"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} disabled={!logsEnabled} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.IncludeRelatedLogs} />}
                        />
                    </div>
                    <div>
                        <Controller
                            control={control} name="IncludePerComponentLogs"
                            render={({
                                field: { onChange, name: fieldName, value }
                            }) => <Checkbox name={fieldName} checked={value} disabled={!logsEnabled} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.IncludePerComponentLogs} />}
                        />
                    </div>
                </div>
            }
            <div className={dialogStyles.emailCheckbox}>
                <Controller
                    control={control} name="SendEmail"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox
                            name={fieldName}
                            checked={value}
                            onChange={(_, data) => onChange(data.checked)}
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
            }) => <Field label={nlsHPCC.EmailTo}>
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                        placeholder={nlsHPCC.SeeConfigurationManager}
                        disabled={emailDisabled}
                    />
                </Field>}
        />
        <Controller
            control={control} name="EmailFrom"
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.EmailFrom}>
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                        placeholder={nlsHPCC.SeeConfigurationManager}
                        disabled={emailDisabled}
                    />
                </Field>}
        />
        <Controller
            control={control} name="EmailSubject"
            render={({
                field: { onChange, name: fieldName, value }
            }) => <Field label={nlsHPCC.EmailSubject}>
                    <Input
                        name={fieldName}
                        onChange={(_, data) => onChange(data.value)}
                        value={value}
                        disabled={emailDisabled}
                    />
                </Field>}
        />
        <fieldset className={dialogStyles.fieldset}>
            <legend>
                <span>{nlsHPCC.LogFilters}</span>
                {logFiltersUnavailable &&
                    <Tooltip content={logFiltersUnavailable ? `${nlsHPCC.LogFiltersUnavailable}: ${logFiltersUnavailableMessage}` : ""} relationship="label">
                        <Info16Regular className={dialogStyles.fieldsetIcon} />
                    </Tooltip>
                }
            </legend>
            <Controller
                control={control} name="LogFilter.AbsoluteTimeRange.StartDate"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field
                    label={<CustomLabel
                        label={nlsHPCC.FromDate}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterStartDateTooltip}
                    />}
                    validationMessage={error?.message}
                    validationState={error ? "error" : undefined}
                >
                        <Tooltip content={nlsHPCC.LogFilterStartDateTooltip} relationship="description">
                            <input
                                key={fieldName}
                                type="datetime-local"
                                name={fieldName}
                                disabled={logFiltersUnavailable}
                                className={[formClasses.input, logFiltersUnavailable ? formClasses.disabled : ""].join(" ")}
                                defaultValue={value}
                                onChange={onChange}
                            />
                        </Tooltip>
                    </Field>
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
                    field: { onChange, name: fieldName, value }
                }) => <Field
                    label={<CustomLabel
                        label={nlsHPCC.ToDate}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterEndDateTooltip}
                    />}
                >
                        <input
                            key={fieldName}
                            type="datetime-local"
                            name={fieldName}
                            disabled={logFiltersUnavailable}
                            className={[formClasses.input, logFiltersUnavailable ? formClasses.disabled : ""].join(" ")}
                            defaultValue={value}
                            onChange={onChange}
                        />
                    </Field>
                }
            />
            <Controller
                control={control} name="LogFilter.RelativeTimeRangeBuffer"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field
                    label={<CustomLabel
                        label={nlsHPCC.RelativeTimeRange}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterRelativeTimeRangeTooltip}
                    />}
                    validationMessage={error?.message}
                    validationState={error ? "error" : undefined}
                >
                        <Input
                            name={fieldName}
                            onChange={(_, data) => onChange(data.value)}
                            disabled={logFiltersUnavailable}
                            value={value}
                        />
                    </Field>
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
                }) => <Field
                    label={<CustomLabel
                        label={nlsHPCC.LogLineLimit}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterLineLimitTooltip}
                    />}
                >
                        <Input
                            name={fieldName}
                            onChange={(_, data) => onChange(data.value)}
                            disabled={logFiltersUnavailable}
                            value={value}
                        />
                    </Field>
                }
            />
            <Controller
                control={control} name="LogFilter.LineStartFrom"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field
                    label={<CustomLabel
                        label={nlsHPCC.LogLineStartFrom}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterLineStartFromTooltip}
                    />}
                >
                        <Input
                            name={fieldName}
                            onChange={(_, data) => onChange(data.value)}
                            disabled={logFiltersUnavailable}
                            value={value}
                        />
                    </Field>
                }
            />
            <Controller
                control={control} name="LogFilter.SelectColumnMode"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field
                    label={<CustomLabel
                        label={nlsHPCC.ColumnMode}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterSelectColumnModeTooltip}
                    />}
                >
                        <Dropdown
                            key={fieldName}
                            disabled={logFiltersUnavailable}
                            value={[ColumnMode.MIN, ColumnMode.DEFAULT, ColumnMode.ALL, ColumnMode.CUSTOM].indexOf(value) > -1 ? ["MIN", "DEFAULT", "ALL", "CUSTOM"][[ColumnMode.MIN, ColumnMode.DEFAULT, ColumnMode.ALL, ColumnMode.CUSTOM].indexOf(value)] : "DEFAULT"}
                            selectedOptions={[value?.toString()]}
                            onOptionSelect={(_, data) => {
                                const selectedValue = [ColumnMode.MIN, ColumnMode.DEFAULT, ColumnMode.ALL, ColumnMode.CUSTOM][["MIN", "DEFAULT", "ALL", "CUSTOM"].indexOf(data.optionValue)];
                                setShowCustomColumns(selectedValue === ColumnMode.CUSTOM);
                                onChange(selectedValue);
                            }}
                        >
                            <Option value="MIN">MIN</Option>
                            <Option value="DEFAULT">DEFAULT</Option>
                            <Option value="ALL">ALL</Option>
                            <Option value="CUSTOM">CUSTOM</Option>
                        </Dropdown>
                    </Field>
                }
            />
            <div className={showCustomColumns ? dialogStyles.customColumnsContainer : dialogStyles.customColumnsContainerHidden}>
                <Controller
                    control={control} name="LogFilter.CustomColumns"
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Field
                        label={<CustomLabel
                            label={nlsHPCC.CustomLogColumns}
                            id={`${fieldName}_Label`}
                            disabled={logFiltersUnavailable}
                            tooltip={nlsHPCC.LogFilterCustomColumnsTooltip}
                        />}
                    >
                            <Textarea
                                name={fieldName}
                                onChange={(_, data) => onChange(data.value)}
                                disabled={logFiltersUnavailable}
                                value={value}
                            />
                        </Field>
                    }
                />
            </div>
            <Controller
                control={control} name="LogFilter.ComponentsFilter"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field
                    label={<CustomLabel
                        id={`${fieldName}_Label`}
                        label={nlsHPCC.ContainerName}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterComponentsFilterTooltip}
                    />}
                >
                        <CloudContainerNameField
                            name={fieldName}
                            selectedKey={value}
                            disabled={logFiltersUnavailable}
                            onChange={onChange}
                        />
                    </Field>
                }
            />
            <Controller
                control={control} name="LogFilter.Format"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field
                    label={<CustomLabel
                        label={nlsHPCC.LogFormat}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterFormatTooltip}
                    />}
                >
                        <Dropdown
                            key={fieldName}
                            disabled={logFiltersUnavailable}
                            value={value === LogFormat.CSV ? "CSV" : value === LogFormat.JSON ? "JSON" : "XML"}
                            selectedOptions={[value]}
                            onOptionSelect={(_, data) => {
                                onChange(data.optionValue as LogFormat);
                            }}
                        >
                            <Option value={LogFormat.CSV}>CSV</Option>
                            <Option value={LogFormat.JSON}>JSON</Option>
                            <Option value={LogFormat.XML}>XML</Option>
                        </Dropdown>
                    </Field>
                }
            />
            <Controller
                control={control} name="LogFilter.WildcardFilter"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field
                    label={<CustomLabel
                        label={nlsHPCC.WildcardFilter}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterWildcardFilterTooltip}
                    />}
                >
                        <Input
                            name={fieldName}
                            onChange={(_, data) => onChange(data.value)}
                            disabled={logFiltersUnavailable}
                            value={value}
                        />
                    </Field>
                }
            />
            <Controller
                control={control} name="LogFilter.sortByTimeDirection"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field
                    label={<CustomLabel
                        label={`${nlsHPCC.Sort} (${nlsHPCC.TimeStamp})`}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterSortByTooltip}
                    />}
                >
                        <Dropdown
                            key={fieldName}
                            disabled={logFiltersUnavailable}
                            value={value === "0" ? "ASC" : "DESC"}
                            selectedOptions={[value]}
                            onOptionSelect={(_, data) => {
                                onChange(data.optionValue);
                            }}
                        >
                            <Option value="0">ASC</Option>
                            <Option value="1">DESC</Option>
                        </Dropdown>
                    </Field>
                }
            />
            <Controller
                control={control} name="LogFilter.LogEventType"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Field
                    label={<CustomLabel
                        label={nlsHPCC.LogEventType}
                        id={`${fieldName}_Label`}
                        disabled={logFiltersUnavailable}
                        tooltip={nlsHPCC.LogFilterEventTypeTooltip}
                    />}
                >
                        <Dropdown
                            key={fieldName}
                            disabled={logFiltersUnavailable}
                            value={value === "ALL" ? "All" : value === LogType.Disaster ? "Disaster" : value === LogType.Error ? "Error" : value === LogType.Warning ? "Warning" : value === LogType.Information ? "Information" : value === LogType.Progress ? "Progress" : "Metric"}
                            selectedOptions={[value]}
                            onOptionSelect={(_, data) => {
                                onChange(data.optionValue);
                            }}
                        >
                            <Option value="ALL">All</Option>
                            <Option value={LogType.Disaster}>Disaster</Option>
                            <Option value={LogType.Error}>Error</Option>
                            <Option value={LogType.Warning}>Warning</Option>
                            <Option value={LogType.Information}>Information</Option>
                            <Option value={LogType.Progress}>Progress</Option>
                            <Option value={LogType.Metric}>Metric</Option>
                        </Dropdown>
                    </Field>
                }
            />
        </fieldset>
    </MessageBox>;
};