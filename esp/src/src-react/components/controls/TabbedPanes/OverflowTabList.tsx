import * as React from "react";
import { Overflow, OverflowItem, SelectTabData, SelectTabEvent, Tab, TabList } from "@fluentui/react-components";
import { Count } from "./Count";
import { TabInfo } from "./TabInfo";
import { OverflowMenu } from "./OverflowMenu";

export interface OverflowTabListProps {
    tabs: TabInfo[];
    selectedTab: string;
    onTabSelect: (tab: TabInfo) => void;
    size?: "small" | "medium" | "large";
}

export const OverflowTabList: React.FunctionComponent<OverflowTabListProps> = ({
    tabs,
    selectedTab,
    onTabSelect,
    size = "medium"
}) => {

    const state = `${window.location.hash}${window.location.search}`;

    const [overflowItems, tabsIndex] = React.useMemo(() => {
        const tabsIndex = {};
        return [tabs.map(tab => {
            tabsIndex[tab.id] = tab;
            if (tab.id === selectedTab) {
                tab.__state = state;
            }
            return <OverflowItem key={tab.id} id={tab.id} priority={tab.id === selectedTab ? 2 : 1}>
                <Tab value={tab.id} icon={tab.icon} disabled={tab.disabled}>{tab.label}<Count value={tab.count} /></Tab>
            </OverflowItem>;
        }), tabsIndex];
    }, [selectedTab, state, tabs]);

    const localTabSelect = React.useCallback((evt: SelectTabEvent, data: SelectTabData) => {
        onTabSelect(tabsIndex[data.value as string]);
    }, [onTabSelect, tabsIndex]);

    return <Overflow minimumVisible={2}>
        <TabList selectedValue={selectedTab} onTabSelect={localTabSelect} size={size}>
            {...overflowItems}
            <OverflowMenu onMenuSelect={onTabSelect} tabs={tabs} />
        </TabList>
    </Overflow>;
};
