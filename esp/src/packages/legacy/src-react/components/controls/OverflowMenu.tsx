import * as React from "react";
import { makeStyles, tokens, Button, Menu, MenuList, MenuPopover, MenuTrigger, useOverflowMenu, useIsOverflowItemVisible, MenuItem, } from "@fluentui/react-components";
import { MoreHorizontalRegular, MoreHorizontalFilled, bundleIcon, } from "@fluentui/react-icons";
import type { ARIAButtonElement } from "@fluentui/react-aria";
import { Count } from "./TabbedPanes/Count";

const MoreHorizontal = bundleIcon(MoreHorizontalFilled, MoreHorizontalRegular);

export interface MenuItem {
    id: string;
    icon?: React.ReactElement;
    label: string;
    count?: string | number;
    disabled?: boolean;
}

type OverflowMenuItemProps = {
    item: MenuItem;
    onClick: React.MouseEventHandler<ARIAButtonElement<"div">>;
};

const OverflowMenuItem: React.FunctionComponent<OverflowMenuItemProps> = ({
    item,
    onClick
}) => {
    const isVisible = useIsOverflowItemVisible(item.id);

    if (isVisible) {
        return <></>;
    }

    return <MenuItem key={item.id} icon={item.icon} disabled={item.disabled} onClick={onClick}>
        <div>{item.label}<Count value={item.count} /></div>
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

export interface OverflowMenuProps {
    menuItems: readonly MenuItem[];
    onMenuSelect: (menuItem: MenuItem) => void;
}

export const OverflowMenu: React.FunctionComponent<OverflowMenuProps> = ({
    menuItems,
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
                aria-label={`${overflowCount} more menu items`}
                role="menuItem"
            />
        </MenuTrigger>
        <MenuPopover>
            <MenuList className={styles.menu}>
                {menuItems.map((menuItem) => (
                    <OverflowMenuItem
                        key={menuItem.id}
                        item={menuItem}
                        onClick={() => onMenuSelect(menuItem)}
                    />
                ))}
            </MenuList>
        </MenuPopover>
    </Menu>;
};
