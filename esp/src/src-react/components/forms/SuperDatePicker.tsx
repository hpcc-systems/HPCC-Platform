import * as React from "react";
import { Divider, Popover, PopoverTrigger, PopoverSurface, Button, Text, tokens } from "@fluentui/react-components";
import { ArrowClockwise20Regular, Play20Regular, Pause20Regular, ChevronDown16Regular } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";
import { DateTimeInput } from "./Fields";

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
    const [isCalloutVisible, setIsCalloutVisible] = React.useState(false);
    const [tempStartDate, setTempStartDate] = React.useState<string>("");
    const [tempEndDate, setTempEndDate] = React.useState<string>("");

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

    const buttonStyle: React.CSSProperties = {
        height: 32,
        minWidth: "auto",
        padding: "0 8px"
    };

    return <div style={{ display: "flex", flexDirection: "row", alignItems: "center", gap: "8px" }}>
        <Popover
            open={isCalloutVisible}
            onOpenChange={(_, data) => {
                if (data.open) {
                    handleShowCallout();
                }
                setIsCalloutVisible(data.open);
            }}
            positioning="below-start"
            withArrow
        >
            <PopoverTrigger disableButtonEnhancement>
                <button
                    disabled={disabled}
                    type="button"
                    style={{
                        display: "flex",
                        flexDirection: "row",
                        alignItems: "center",
                        justifyContent: "space-between",
                        padding: "4px 8px",
                        border: `1px solid ${tokens.colorNeutralStroke1}`,
                        borderRadius: "2px",
                        backgroundColor: tokens.colorNeutralBackground1,
                        cursor: disabled ? "not-allowed" : "pointer",
                        opacity: disabled ? 0.6 : 1,
                        minWidth: "200px"
                    }}
                >
                    <Text
                        size={300}
                        weight="regular"
                        style={{
                            color: disabled ? tokens.colorNeutralForeground3 : tokens.colorNeutralForeground1,
                            flex: 1,
                            marginRight: 4,
                            textOverflow: "ellipsis",
                            overflow: "hidden",
                            whiteSpace: "nowrap"
                        }}
                    >
                        {currentDisplayText}
                    </Text>
                    <ChevronDown16Regular
                        style={{
                            color: disabled ? tokens.colorNeutralForeground3 : tokens.colorNeutralForeground2
                        }}
                    />
                </button>
            </PopoverTrigger>
            <PopoverSurface style={{ padding: "16px", minWidth: "400px", maxWidth: "500px" }}>
                <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
                    <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                        <Text size={300} weight="semibold">
                            {nlsHPCC.QuickSelect}
                        </Text>
                        <div style={{ display: "flex", flexDirection: "row", flexWrap: "wrap", gap: "8px" }}>
                            {defaultQuickOptions.map((option) => (
                                <Button
                                    key={option.value}
                                    onClick={() => handleQuickSelect(option)}
                                    style={{ fontSize: "12px", height: "28px", minWidth: "auto", padding: "0 12px" }}
                                >{option.label}</Button>
                            ))}
                        </div>
                    </div>

                    <Divider />

                    <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                        <Text size={300} weight="semibold">
                            {nlsHPCC.CustomRange}
                        </Text>
                        <div style={{ display: "flex", flexDirection: "row", gap: "12px", alignItems: "flex-end" }}>
                            <div style={{ display: "flex", flexDirection: "column", gap: "4px", flex: 1 }}>
                                <Text size={200} weight="regular">
                                    {nlsHPCC.FromDate}
                                </Text>
                                <DateTimeInput value={tempStartDate} onChange={setTempStartDate} style={{ width: "100%" }} />
                            </div>
                            <div style={{ display: "flex", flexDirection: "column", gap: "4px", flex: 1 }}>
                                <Text size={200} weight="regular">
                                    {nlsHPCC.ToDate}
                                </Text>
                                <DateTimeInput value={tempEndDate} onChange={setTempEndDate} style={{ width: "100%" }} />
                            </div>
                        </div>
                    </div>

                    {showAutoRefresh && (
                        <>
                            <Divider />
                            <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                                <Text size={300} weight="semibold">
                                    {nlsHPCC.AutoRefresh}
                                </Text>
                                <div style={{ display: "flex", flexDirection: "row", flexWrap: "wrap", gap: "8px" }}>
                                    {autoRefreshOptions.map((option) => (
                                        <Button
                                            key={option.value}
                                            onClick={() => handleAutoRefreshIntervalChange(option.value)}
                                            style={{
                                                fontSize: "12px",
                                                height: "28px",
                                                minWidth: "auto",
                                                padding: "0 12px",
                                                backgroundColor: autoRefreshInterval === option.value ? tokens.colorBrandBackground2 : undefined,
                                                borderColor: autoRefreshInterval === option.value ? tokens.colorBrandBackground : undefined
                                            }}
                                        >{option.label}</Button>
                                    ))}
                                </div>
                            </div>
                        </>
                    )}

                    <div style={{ display: "flex", flexDirection: "row", justifyContent: "flex-end", gap: "8px" }}>
                        <Button onClick={() => setIsCalloutVisible(false)}>{nlsHPCC.Cancel}</Button>
                        <Button appearance="primary" onClick={handleCustomDateApply}>{nlsHPCC.Apply}</Button>
                    </div>
                </div>
            </PopoverSurface>
        </Popover>

        {showRefreshButton && (
            <Button appearance="subtle" icon={<ArrowClockwise20Regular />} onClick={onRefresh} disabled={disabled} style={buttonStyle}>{nlsHPCC.Refresh}</Button>
        )}

        {showAutoRefresh && (
            <div style={{ display: "flex", flexDirection: "row", alignItems: "center", gap: "4px" }}>
                <Button
                    appearance="subtle"
                    icon={autoRefresh ? <Play20Regular /> : <Pause20Regular />}
                    onClick={handleAutoRefreshToggle}
                    disabled={disabled}
                    style={{ ...buttonStyle, backgroundColor: autoRefresh ? tokens.colorBrandBackground2 : undefined }}
                >{autoRefresh ? `${nlsHPCC.AutoRefresh}: ${nlsHPCC.On.toUpperCase()}` : `${nlsHPCC.AutoRefresh}: ${nlsHPCC.Off.toUpperCase()}`}</Button>
            </div>
        )}
    </div>;
};
