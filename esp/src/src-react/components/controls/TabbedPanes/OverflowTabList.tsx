import * as React from "react";
import { Overflow, OverflowItem, SelectTabData, SelectTabEvent, Tab, TabList, Tooltip } from "@fluentui/react-components";
import { Count } from "./Count";
import { TabInfo } from "./TabInfo";
import { OverflowMenu } from "../OverflowMenu";

export interface OverflowTabListProps {
    tabs: TabInfo[];
    selected: string;
    onTabSelect: (tab: TabInfo) => void;
    size?: "small" | "medium" | "large";
}

export const OverflowTabList: React.FunctionComponent<OverflowTabListProps> = ({
    tabs,
    selected,
    onTabSelect,
    size = "medium"
}) => {

    const state = `${window.location.hash}${window.location.search}`;

    const [overflowItems, tabsIndex] = React.useMemo(() => {
        const tabsIndex = {};
        return [tabs.map(tab => {
            tabsIndex[tab.id] = tab;
            if (tab.id === selected) {
                tab.__state = state;
            }
            return <OverflowItem key={tab.id} id={tab.id} priority={tab.id === selected ? 2 : 1}>
                <Tooltip content={tab.tooltipText || tab.label} relationship="label">
                    <Tab value={tab.id} icon={tab.icon} disabled={tab.disabled}>{tab.label}<Count value={tab.count} /></Tab>
                </Tooltip>
            </OverflowItem>;
        }), tabsIndex];
    }, [selected, state, tabs]);

    const localTabSelect = React.useCallback((evt: SelectTabEvent, data: SelectTabData) => {
        onTabSelect(tabsIndex[data.value as string]);
    }, [onTabSelect, tabsIndex]);

    return <Overflow minimumVisible={2}>
        <TabList selectedValue={selected} onTabSelect={localTabSelect} size={size}>
            {...overflowItems}
            <OverflowMenu onMenuSelect={onTabSelect} menuItems={tabs} />
        </TabList>
    </Overflow>;
};
