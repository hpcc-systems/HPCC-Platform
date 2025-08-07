import * as React from "react";
import { CommandBar, ICommandBarItemProps, Stack, Label, CommandBarButton, useTheme } from "@fluentui/react";
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
    const theme = useTheme();

    return <Stack horizontal verticalAlign="center" styles={{ root: { padding: "0px 6px", borderBottom: `1px solid ${theme.palette.neutralLight}` } }}>
        <Stack horizontal tokens={{ childrenGap: 16 }} styles={{ root: { flex: 1, alignItems: "center" } }}>
            <CommandBarButton
                text={nlsHPCC.Filter}
                style={{ padding: 8 }}
                iconProps={{ iconName: hasFilter ? "FilterSolid" : "Filter" }}
                onClick={onShowFilter}
            />
            <Stack horizontal tokens={{ childrenGap: 8 }} verticalAlign="center">
                <Label>{nlsHPCC.FromDate}:</Label>
                <DateTimeInput
                    value={startDate}
                    onChange={onStartDateChange}
                    style={{ padding: "4px 8px", border: `1px solid ${theme.palette.neutralTertiary}`, borderRadius: "2px" }}
                />
            </Stack>
            <Stack horizontal tokens={{ childrenGap: 8 }} verticalAlign="center">
                <Label>{nlsHPCC.ToDate}:</Label>
                <DateTimeInput
                    value={endDate}
                    onChange={onEndDateChange}
                    style={{ padding: "4px 8px", border: `1px solid ${theme.palette.neutralTertiary}`, borderRadius: "2px" }}
                />
            </Stack>
            <CommandBarButton
                text={nlsHPCC.Refresh}
                style={{ padding: 8 }}
                iconProps={{ iconName: "Refresh" }}
                onClick={onRefresh}
            />
        </Stack>
        <div>
            <CommandBar items={[]} farItems={copyButtons} />
        </div>
    </Stack>;
};
