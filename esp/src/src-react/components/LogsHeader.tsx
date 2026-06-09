import * as React from "react";
import { Button, Label, tokens } from "@fluentui/react-components";
import { Filter20Regular, Filter20Filled, ArrowClockwise20Regular } from "@fluentui/react-icons";
import { CommandBar, ICommandBarItemProps } from "./CommandBarV9";
import nlsHPCC from "src/nlsHPCC";
import { DateTimeInput } from "./forms/Fields";

interface LogsHeaderProps {
    startDate: Date | string | undefined;
    endDate: Date | string | undefined;
    onStartDateChange: (date: string) => void;
    onEndDateChange: (date: string) => void;
    onRefresh: () => void;
    onShowFilter: () => void;
    hasFilter: boolean;
    copyButtons: ICommandBarItemProps[];
}

export const LogsHeader: React.FunctionComponent<LogsHeaderProps> = ({
    startDate,
    endDate,
    onStartDateChange,
    onEndDateChange,
    onRefresh,
    onShowFilter,
    hasFilter,
    copyButtons
}) => {
    return <div style={{ display: "flex", flexDirection: "row", alignItems: "center", padding: "0px 6px", borderBottom: `1px solid ${tokens.colorNeutralStroke2}` }}>
        <div style={{ display: "flex", flexDirection: "row", gap: "16px", flex: 1, alignItems: "center" }}>
            <Button
                appearance="subtle"
                icon={hasFilter ? <Filter20Filled /> : <Filter20Regular />}
                onClick={onShowFilter}
            >{nlsHPCC.Filter}</Button>
            <div style={{ display: "flex", flexDirection: "row", gap: "8px", alignItems: "center" }}>
                <Label>{nlsHPCC.FromDate}:</Label>
                <DateTimeInput
                    value={startDate}
                    onChange={onStartDateChange}
                    style={{ padding: "4px 8px", border: `1px solid ${tokens.colorNeutralStroke1}`, borderRadius: "2px" }}
                />
            </div>
            <div style={{ display: "flex", flexDirection: "row", gap: "8px", alignItems: "center" }}>
                <Label>{nlsHPCC.ToDate}:</Label>
                <DateTimeInput
                    value={endDate}
                    onChange={onEndDateChange}
                    style={{ padding: "4px 8px", border: `1px solid ${tokens.colorNeutralStroke1}`, borderRadius: "2px" }}
                />
            </div>
            <Button
                appearance="subtle"
                icon={<ArrowClockwise20Regular />}
                onClick={onRefresh}
            >{nlsHPCC.Refresh}</Button>
        </div>
        <div>
            <CommandBar items={[]} farItems={copyButtons} />
        </div>
    </div>;
};
