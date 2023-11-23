import * as React from "react";
import { makeStyles, tokens, Button, Menu, MenuList, MenuPopover, MenuTrigger, useOverflowMenu, useIsOverflowItemVisible, MenuItem, } from "@fluentui/react-components";
import { MoreHorizontalRegular, MoreHorizontalFilled, bundleIcon, } from "@fluentui/react-icons";
import type { ARIAButtonElement } from "@fluentui/react-aria";
import { TabInfo } from "./TabInfo";
import { Count } from "./Count";

const MoreHorizontal = bundleIcon(MoreHorizontalFilled, MoreHorizontalRegular);

type OverflowMenuItemProps = {
    tab: TabInfo;
    onClick: React.MouseEventHandler<ARIAButtonElement<"div">>;
};

const OverflowMenuItem = (props: OverflowMenuItemProps) => {
    const { tab, onClick } = props;
    const isVisible = useIsOverflowItemVisible(tab.id);

    if (isVisible) {
        return <></>;
    }

    return <MenuItem key={tab.id} icon={tab.icon} disabled={tab.disabled} onClick={onClick}>
        <div>{tab.label}<Count value={tab.count} /></div>
    </MenuItem>;
};

const useOverflowMenuStyles = makeStyles({
    menu: {
        backgroundColor: tokens.colorNeutralBackground1,
    },
    menuButton: {
        alignSelf: "center",
    },
});

export type OverflowMenuProps = {
    tabs: TabInfo[];
    onMenuSelect: (tab: TabInfo) => void;
};

export const OverflowMenu: React.FunctionComponent<OverflowMenuProps> = ({
    tabs,
    onMenuSelect
}) => {
    const { ref, isOverflowing, overflowCount } = useOverflowMenu<HTMLButtonElement>();

    const styles = useOverflowMenuStyles();

    if (!isOverflowing) {
        return <></>;
    }

    return <Menu hasIcons>
        <MenuTrigger disableButtonEnhancement>
            <Button
                appearance="transparent"
                className={styles.menuButton}
                ref={ref}
                icon={<MoreHorizontal />}
                aria-label={`${overflowCount} more tabs`}
                role="tab"
            />
        </MenuTrigger>
        <MenuPopover>
            <MenuList className={styles.menu}>
                {tabs.map((tab) => (
                    <OverflowMenuItem
                        key={tab.id}
                        tab={tab}
                        onClick={() => onMenuSelect(tab)}
                    />
                ))}
            </MenuList>
        </MenuPopover>
    </Menu>;
};
