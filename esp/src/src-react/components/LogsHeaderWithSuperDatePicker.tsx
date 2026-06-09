import * as React from "react";
import { Button, tokens } from "@fluentui/react-components";
import { Filter20Regular, Filter20Filled } from "@fluentui/react-icons";
import { CommandBar, ICommandBarItemProps } from "./CommandBarV9";
import nlsHPCC from "src/nlsHPCC";
import { SuperDatePicker, DateRange } from "./forms/Fields";

interface LogsHeaderWithSuperDatePickerProps {
    startDate: Date | string | undefined;
    endDate: Date | string | undefined;
    onDateChange: (range: DateRange) => void;
    onRefresh: () => void;
    onShowFilter: () => void;
    hasFilter: boolean;
    copyButtons: ICommandBarItemProps[];
    autoRefresh?: boolean;
    onAutoRefreshChange?: (enabled: boolean) => void;
    autoRefreshInterval?: number;
    onAutoRefreshIntervalChange?: (interval: number) => void;
}

export const LogsHeaderWithSuperDatePicker: React.FunctionComponent<LogsHeaderWithSuperDatePickerProps> = ({
    startDate,
    endDate,
    onDateChange,
    onRefresh,
    onShowFilter,
    hasFilter,
    copyButtons,
    autoRefresh = false,
    onAutoRefreshChange,
    autoRefreshInterval = 0,
    onAutoRefreshIntervalChange
}) => {
    return <div style={{ display: "flex", flexDirection: "row", alignItems: "center", padding: "0px 6px", borderBottom: `1px solid ${tokens.colorNeutralStroke2}` }}>
        <div style={{ display: "flex", flexDirection: "row", gap: "16px", flex: 1, alignItems: "center" }}>
            <Button
                appearance="subtle"
                icon={hasFilter ? <Filter20Filled /> : <Filter20Regular />}
                onClick={onShowFilter}
            >{nlsHPCC.Filter}</Button>
            <SuperDatePicker
                startDate={startDate}
                endDate={endDate}
                onDateChange={onDateChange}
                onRefresh={onRefresh}
                showAutoRefresh={true}
                autoRefresh={autoRefresh}
                onAutoRefreshChange={onAutoRefreshChange}
                autoRefreshInterval={autoRefreshInterval}
                onAutoRefreshIntervalChange={onAutoRefreshIntervalChange}
            />
        </div>
        <div>
            <CommandBar items={[]} farItems={copyButtons} />
        </div>
    </div>;
};
