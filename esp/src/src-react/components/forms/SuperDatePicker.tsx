import * as React from "react";
import { CommandBarButton, Callout, DirectionalHint, Text, DefaultButton, PrimaryButton, Separator, IButtonStyles, FontWeights, useTheme, IconButton } from "@fluentui/react";
import { makeStyles } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";
import { DateTimeInput } from "./Fields";
import { Flex } from "../../layouts/Flex";

export interface DateRange {
    startDate?: Date | string;
    endDate?: Date | string;
}

export interface RelativeTimeOption {
    label: string;
    value: string;
    startOffset: number; // milliseconds from now
    endOffset?: number;  // milliseconds from now, defaults to 0 (now)
}

export interface SuperDatePickerProps {
    startDate?: Date | string;
    endDate?: Date | string;
    onDateChange: (range: DateRange) => void;
    onRefresh?: () => void;
    autoRefresh?: boolean;
    onAutoRefreshChange?: (enabled: boolean) => void;
    autoRefreshInterval?: number; // seconds
    onAutoRefreshIntervalChange?: (interval: number) => void;
    showAutoRefresh?: boolean;
    showRefreshButton?: boolean;
    disabled?: boolean;
}

const defaultQuickOptions: RelativeTimeOption[] = [
    { label: `${nlsHPCC.Last} 15 ${nlsHPCC.Minutes.toLowerCase()}`, value: "15m", startOffset: -15 * 60 * 1000 },
    { label: `${nlsHPCC.Last} 30 ${nlsHPCC.Minutes.toLowerCase()}`, value: "30m", startOffset: -30 * 60 * 1000 },
    { label: `${nlsHPCC.Last} 1 ${nlsHPCC.Hour.toLowerCase()}`, value: "1h", startOffset: -60 * 60 * 1000 },
    { label: `${nlsHPCC.Last} 4 ${nlsHPCC.Hours.toLowerCase()}`, value: "4h", startOffset: -4 * 60 * 60 * 1000 },
    { label: `${nlsHPCC.Last} 12 ${nlsHPCC.Hours.toLowerCase()}`, value: "12h", startOffset: -12 * 60 * 60 * 1000 },
    { label: `${nlsHPCC.Last} 24 ${nlsHPCC.Hours.toLowerCase()}`, value: "24h", startOffset: -24 * 60 * 60 * 1000 },
    { label: `${nlsHPCC.Last} 7 ${nlsHPCC.Days.toLowerCase()}`, value: "7d", startOffset: -7 * 24 * 60 * 60 * 1000 },
    { label: `${nlsHPCC.Last} 30 ${nlsHPCC.Days.toLowerCase()}`, value: "30d", startOffset: -30 * 24 * 60 * 60 * 1000 },
    { label: `${nlsHPCC.Last} 90 ${nlsHPCC.Days.toLowerCase()}`, value: "90d", startOffset: -90 * 24 * 60 * 60 * 1000 }
];

const autoRefreshOptions = [
    { label: `${nlsHPCC.Off}`, value: 0 },
    { label: `30 ${nlsHPCC.Seconds.toLowerCase()}`, value: 30 },
    { label: `1 ${nlsHPCC.Minute.toLowerCase()}`, value: 60 },
    { label: `5 ${nlsHPCC.Minutes.toLowerCase()}`, value: 300 },
    { label: `15 ${nlsHPCC.Minutes.toLowerCase()}`, value: 900 }
];

const formatDateForDisplay = (date: Date | string | undefined): string => {
    if (!date) return "";
    const d = typeof date === "string" ? new Date(date) : date;
    return d.toLocaleString();
};

const formatRelativeTime = (startDate: Date | string | undefined, endDate: Date | string | undefined): string => {
    if (!startDate || !endDate) return "";

    const start = typeof startDate === "string" ? new Date(startDate) : startDate;
    const end = typeof endDate === "string" ? new Date(endDate) : endDate;
    const now = new Date();

    // check if this is a relative time range (end is "now")
    const isEndNow = Math.abs(end.getTime() - now.getTime()) < 60000; // within 1 minute of now

    if (isEndNow) {
        const diffMs = now.getTime() - start.getTime();
        const diffMinutes = Math.floor(diffMs / (60 * 1000));
        const diffHours = Math.floor(diffMs / (60 * 60 * 1000));
        const diffDays = Math.floor(diffMs / (24 * 60 * 60 * 1000));

        if (diffMinutes < 60) {
            return `${nlsHPCC.Last} ${diffMinutes} minute${diffMinutes === 1 ? "" : "s"}`;
        } else if (diffHours < 24) {
            return `${nlsHPCC.Last} ${diffHours} hour${diffHours === 1 ? "" : "s"}`;
        } else {
            return `${nlsHPCC.Last} ${diffDays} day${diffDays === 1 ? "" : "s"}`;
        }
    }

    return nlsHPCC.CustomRange;
};

const useStyles = makeStyles({
    dateButton: {
        padding: "4px 8px",
        borderRadius: "2px",
        minWidth: "200px"
    },
    optionButton: {
        fontSize: "12px",
        height: "28px",
        minWidth: "auto",
        padding: "0 12px"
    },
    textTruncate: {
        fontSize: "14px",
        fontWeight: FontWeights.regular,
        flex: 1,
        marginRight: "2px",
        textOverflow: "ellipsis",
        overflow: "hidden",
        whiteSpace: "nowrap"
    },
    chevronButton: {
        width: "16px",
        height: "16px"
    },
    sectionTitle: {
        fontWeight: FontWeights.semibold,
        fontSize: "14px"
    },
    fieldLabel: {
        fontSize: "12px",
        fontWeight: FontWeights.regular
    },
    calloutRoot: {
        padding: 0,
        "& .ms-Callout-main": {
            padding: "16px",
            minWidth: "400px",
            maxWidth: "500px"
        }
    }
});

export const SuperDatePicker: React.FunctionComponent<SuperDatePickerProps> = ({
    startDate,
    endDate,
    onDateChange,
    onRefresh,
    autoRefresh = false,
    onAutoRefreshChange,
    autoRefreshInterval = 0,
    onAutoRefreshIntervalChange,
    showAutoRefresh = false,
    showRefreshButton = true,
    disabled = false
}) => {
    const theme = useTheme();
    const [isCalloutVisible, setIsCalloutVisible] = React.useState(false);
    const [tempStartDate, setTempStartDate] = React.useState<string>("");
    const [tempEndDate, setTempEndDate] = React.useState<string>("");
    const datePickerButtonRef = React.useRef<HTMLDivElement>(null);

    // handles auto-refresh
    React.useEffect(() => {
        if (autoRefresh && autoRefreshInterval > 0 && onRefresh) {
            const interval = setInterval(onRefresh, autoRefreshInterval * 1000);
            return () => clearInterval(interval);
        }
    }, [autoRefresh, autoRefreshInterval, onRefresh]);

    const handleQuickSelect = React.useCallback((option: RelativeTimeOption) => {
        const now = new Date();
        const newStartDate = new Date(now.getTime() + option.startOffset);
        const newEndDate = option.endOffset ? new Date(now.getTime() + option.endOffset) : now;

        onDateChange({
            startDate: newStartDate,
            endDate: newEndDate
        });
        setIsCalloutVisible(false);
    }, [onDateChange]);

    const handleCustomDateApply = React.useCallback(() => {
        onDateChange({
            startDate: tempStartDate || undefined,
            endDate: tempEndDate || undefined
        });
        setIsCalloutVisible(false);
    }, [tempStartDate, tempEndDate, onDateChange]);

    const handleShowCallout = React.useCallback(() => {
        // initialize temp dates with current values
        setTempStartDate(startDate ? (typeof startDate === "string" ? startDate : startDate.toISOString().slice(0, 16)) : "");
        setTempEndDate(endDate ? (typeof endDate === "string" ? endDate : endDate.toISOString().slice(0, 16)) : "");
        setIsCalloutVisible(true);
    }, [startDate, endDate]);

    const handleAutoRefreshToggle = React.useCallback(() => {
        if (onAutoRefreshChange) {
            onAutoRefreshChange(!autoRefresh);
        }
    }, [autoRefresh, onAutoRefreshChange]);

    const handleAutoRefreshIntervalChange = React.useCallback((interval: number) => {
        if (onAutoRefreshIntervalChange) {
            onAutoRefreshIntervalChange(interval);
            if (interval === 0 && onAutoRefreshChange) {
                onAutoRefreshChange(false);
            } else if (interval > 0 && !autoRefresh && onAutoRefreshChange) {
                onAutoRefreshChange(true);
            }
        }
    }, [autoRefresh, onAutoRefreshChange, onAutoRefreshIntervalChange]);

    const currentDisplayText = React.useMemo(() => {
        if (startDate && endDate) {
            const relative = formatRelativeTime(startDate, endDate);
            if (relative && relative !== nlsHPCC.CustomRange) {
                return relative;
            }
            return `${formatDateForDisplay(startDate)} ~ ${formatDateForDisplay(endDate)}`;
        }
        if (startDate) {
            return `${nlsHPCC.From} ${formatDateForDisplay(startDate)}`;
        }
        if (endDate) {
            return `${nlsHPCC.Until} ${formatDateForDisplay(endDate)}`;
        }
        return nlsHPCC.SelectDateRange;
    }, [startDate, endDate]);

    const styles = useStyles();

    const buttonStyles: IButtonStyles = {
        root: {
            height: 32,
            minWidth: "auto",
            padding: "0 8px"
        }
    };

    return <Flex direction="row" align="center" gap={8}>
        <Flex ref={datePickerButtonRef} direction="row">
            <Flex
                direction="row"
                align="center"
                justify="between"
                gap={4}
                className={styles.dateButton}
                style={{
                    border: `1px solid ${theme.palette.neutralTertiary}`,
                    backgroundColor: theme.palette.white,
                    cursor: disabled ? "not-allowed" : "pointer",
                    opacity: disabled ? 0.6 : 1
                }}
                onClick={disabled ? undefined : handleShowCallout}
            >
                <Text
                    styles={{ root: { color: disabled ? theme.palette.neutralTertiary : theme.palette.neutralPrimary } }}
                    className={styles.textTruncate}
                >
                    {currentDisplayText}
                </Text>
                <IconButton
                    iconProps={{ iconName: "ChevronDown" }}
                    styles={{ root: { color: disabled ? theme.palette.neutralTertiary : theme.palette.neutralSecondary } }}
                    className={styles.chevronButton}
                    disabled={disabled}
                />
            </Flex>
        </Flex>

        {showRefreshButton && (
            <CommandBarButton text={nlsHPCC.Refresh} iconProps={{ iconName: "Refresh" }} onClick={onRefresh} disabled={disabled} styles={buttonStyles} />
        )}

        {showAutoRefresh && (
            <Flex direction="row" align="center" gap={4}>
                <CommandBarButton
                    text={autoRefresh ? `${nlsHPCC.AutoRefresh}: ${nlsHPCC.On.toUpperCase()}` : `${nlsHPCC.AutoRefresh}: ${nlsHPCC.Off.toUpperCase()}`}
                    iconProps={{ iconName: autoRefresh ? "Play" : "Pause" }}
                    onClick={handleAutoRefreshToggle}
                    disabled={disabled}
                    styles={{
                        root: {
                            height: "32px",
                            minWidth: "auto",
                            padding: "0 8px",
                            backgroundColor: autoRefresh ? theme.palette.themeLighterAlt : undefined
                        }
                    }}
                />
            </Flex>
        )}

        <Callout
            target={datePickerButtonRef}
            isBeakVisible
            directionalHint={DirectionalHint.bottomLeftEdge}
            hidden={!isCalloutVisible}
            onDismiss={() => setIsCalloutVisible(false)}
            className={styles.calloutRoot}
        >
            <Flex direction="column" gap={12}>
                <Flex direction="column" gap={12}>
                    <Text className={styles.sectionTitle}>
                        {nlsHPCC.QuickSelect}
                    </Text>
                    <Flex direction="row" wrap gap={8}>
                        {defaultQuickOptions.map((option) => (
                            <DefaultButton
                                key={option.value}
                                text={option.label}
                                onClick={() => handleQuickSelect(option)}
                                className={styles.optionButton}
                            />
                        ))}
                    </Flex>
                </Flex>

                <Separator />

                <Flex direction="column" gap={12}>
                    <Text className={styles.sectionTitle}>
                        {nlsHPCC.CustomRange}
                    </Text>
                    <Flex direction="row" gap={12} align="center">
                        <Flex direction="column" gap={4} grow fullWidth>
                            <Text className={styles.fieldLabel}>
                                {nlsHPCC.FromDate}
                            </Text>
                            <DateTimeInput value={tempStartDate} onChange={setTempStartDate} style={{ width: "100%" }} />
                        </Flex>
                        <Flex direction="column" gap={4} grow fullWidth>
                            <Text className={styles.fieldLabel}>
                                {nlsHPCC.ToDate}
                            </Text>
                            <DateTimeInput value={tempEndDate} onChange={setTempEndDate} style={{ width: "100%" }} />
                        </Flex>
                    </Flex>
                </Flex>

                {showAutoRefresh && (
                    <>
                        <Separator />
                        <Flex direction="column" gap={12}>
                            <Text className={styles.sectionTitle}>
                                {nlsHPCC.AutoRefresh}
                            </Text>
                            <Flex direction="row" wrap gap={8}>
                                {autoRefreshOptions.map((option) => (
                                    <DefaultButton
                                        key={option.value}
                                        text={option.label}
                                        onClick={() => handleAutoRefreshIntervalChange(option.value)}
                                        className={styles.optionButton}
                                        styles={autoRefreshInterval === option.value ? { root: { backgroundColor: theme.palette.themeLighter, outline: `1px solid ${theme.palette.themePrimary}` } } : undefined}
                                    />
                                ))}
                            </Flex>
                        </Flex>
                    </>
                )}

                <Flex direction="row" justify="end" gap={8}>
                    <DefaultButton text={nlsHPCC.Cancel} onClick={() => setIsCalloutVisible(false)} />
                    <PrimaryButton text={nlsHPCC.Apply} onClick={handleCustomDateApply} />
                </Flex>
            </Flex>
        </Callout>
    </Flex>;
};
