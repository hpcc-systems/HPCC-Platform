/**
 * CommandBarV9 — drop-in v9 replacement for v8 `<CommandBar items={...} farItems={...} />`.
 *
 * Accepts the same `ICommandBarItemProps[]` shape as v8 CommandBar so consumers
 * only need to flip the import line. Renders v9 `<Toolbar>` with ToolbarButton /
 * ToolbarToggleButton / ToolbarDivider / Menu+MenuItem children.
 *
 * The v8 string-iconName → v9 react-icon mapping is deferred to Phase N: for now
 * we render icons via v8 `<Icon iconName=... />` so the existing initializeIcons()
 * font registration keeps working unchanged.
 *
 * Re-exports `ICommandBarItemProps` and `ContextualMenuItemType` so consumers can
 * fully drop their `@fluentui/react` import in one swap.
 */
import * as React from "react";
import {
    makeStyles,
    Menu,
    MenuItem,
    MenuList,
    MenuPopover,
    MenuTrigger,
    Overflow,
    OverflowItem,
    Toolbar,
    ToolbarButton,
    ToolbarDivider,
    ToolbarGroup,
    tokens,
    useIsOverflowItemVisible,
    useOverflowMenu,
} from "@fluentui/react-components";
import {
    ArrowClockwiseRegular, ArrowDownloadRegular, ArrowLeftRegular, ArrowResetRegular,
    ArrowRightRegular, ArrowUploadRegular, AutoFitWidthRegular, CheckmarkCircleRegular,
    CodeBlockRegular, CommentTextRegular, ContactCardRegular, CopyRegular,
    DataBarVerticalRegular, DeleteRegular, DismissRegular, DocumentTextRegular,
    EyeRegular, EyeTrackingRegular, FilterFilled, FilterRegular, LockClosedRegular,
    LockOpenRegular, MaximizeRegular, MoreHorizontalRegular, SaveRegular, SearchRegular,
    SettingsRegular, TableLinkRegular, TableRegular, TextBulletListRegular,
    TimelineRegular, TopSpeedRegular, WindowEditRegular, ZoomFitRegular, ZoomInRegular,
    ZoomOutRegular,
} from "@fluentui/react-icons";

const useStyles = makeStyles({
    toolbar: {
        justifyContent: "space-between",
        gap: "0",
    },
    itemsGroup: {
        flex: "1 1 0",
        overflow: "hidden",
        minWidth: "0",
        gap: "0",
    },
    farItemsGroup: {
        flex: "0 0 auto",
        gap: "0",
    },
    button: {
        paddingInline: "6px",
        minWidth: "auto",
    },
});

// ─── Local type replacing @fluentui/react ICommandBarItemProps ────────────────
export interface ICommandBarItemProps {
    key: string;
    text?: string;
    title?: string;
    disabled?: boolean;
    className?: string;
    iconProps?: { iconName?: string };
    /** Custom SVG / react-icon element; takes precedence over iconProps.iconName. */
    iconElement?: React.ReactElement;
    iconOnly?: boolean;
    canCheck?: boolean;
    checked?: boolean;
    hidden?: boolean;
    href?: string;
    subMenuProps?: { items: ICommandBarItemProps[] };
    itemType?: number;
    onRender?: (item: any, dismissMenu?: () => void) => React.ReactNode;
    onClick?: (ev?: React.MouseEvent<HTMLElement>, item?: any) => boolean | void;
    role?: string;
}

// ─── v8 icon name → v9 SVG element mapping ───────────────────────────────────
const ICON_MAP: Record<string, React.ReactElement> = {
    AnalyticsView: <DataBarVerticalRegular />,
    BarChartVertical: <DataBarVerticalRegular />,
    BulletedTreeList: <TextBulletListRegular />,
    Cancel: <DismissRegular />,
    CommentText: <CommentTextRegular />,
    ComplianceAudit: <CheckmarkCircleRegular />,
    Contact: <ContactCardRegular />,
    Copy: <CopyRegular />,
    Delete: <DeleteRegular />,
    DocumentText: <DocumentTextRegular />,
    Download: <ArrowDownloadRegular />,
    EyeTracking: <EyeTrackingRegular />,
    FileHTML: <CodeBlockRegular />,
    Filter: <FilterRegular />,
    FilterSolid: <FilterFilled style={{ color: tokens.colorBrandForeground1 }} />,
    FitPage: <ZoomFitRegular />,
    FitWidth: <AutoFitWidthRegular />,
    Lock: <LockClosedRegular />,
    NavigateBack: <ArrowLeftRegular />,
    NavigateBackMirrored: <ArrowRightRegular />,
    Refresh: <ArrowClockwiseRegular />,
    Reset: <ArrowResetRegular />,
    Relationship: <TableLinkRegular />,
    Save: <SaveRegular />,
    ScaleVolume: <MaximizeRegular />,
    SearchRegular: <SearchRegular />,
    Settings: <SettingsRegular />,
    SpeedHigh: <TopSpeedRegular />,
    Table: <TableRegular />,
    TimelineProgress: <TimelineRegular />,
    Unlock: <LockOpenRegular />,
    Upload: <ArrowUploadRegular />,
    View: <EyeRegular />,
    WindowEdit: <WindowEditRegular />,
    ZoomIn: <ZoomInRegular />,
    ZoomOut: <ZoomOutRegular />,
    ZoomToFit: <ZoomFitRegular />,
};

/**
 * Local replacement for v8 ContextualMenuItemType enum — same numeric values.
 * This lets consumers drop their @fluentui/react import for the item-type constant.
 */
export const ContextualMenuItemType = { Normal: 0, Divider: 1, Header: 2, Section: 3 } as const;

interface CommandBarV9Props {
    items: ICommandBarItemProps[];
    farItems?: ICommandBarItemProps[];
}

function renderIcon(iconName?: string, iconElement?: React.ReactElement): React.ReactElement | undefined {
    if (iconElement) return iconElement;
    if (!iconName) return undefined;
    return ICON_MAP[iconName];
}

function renderSubMenuItem(sub: ICommandBarItemProps): React.ReactNode {
    if (sub.hidden) return null;
    if (sub.itemType === ContextualMenuItemType.Divider) {
        // MenuList separator is not directly available; render a horizontal rule.
        return <div key={sub.key} style={{ height: 1, background: "var(--colorNeutralStroke2)", margin: "4px 0" }} />;
    }
    return <MenuItem
        key={sub.key}
        icon={renderIcon(sub.iconProps?.iconName, sub.iconElement)}
        disabled={sub.disabled}
        onClick={sub.onClick as any}
    >
        {sub.text}
    </MenuItem>;
}

function renderItem(item: ICommandBarItemProps, buttonClassName?: string): React.ReactNode {
    if (item.hidden) return null;
    if (item.onRender) {
        return <React.Fragment key={item.key}>{item.onRender(item as any, () => undefined) as any}</React.Fragment>;
    }
    if (item.itemType === ContextualMenuItemType.Divider) {
        return <ToolbarDivider key={item.key} />;
    }
    const icon = renderIcon(item.iconProps?.iconName, item.iconElement);
    const title = item.title ?? (item.iconOnly ? item.text : undefined);
    const label = item.iconOnly ? undefined : item.text;

    if (item.subMenuProps) {
        return <Menu key={item.key}>
            <MenuTrigger disableButtonEnhancement>
                <ToolbarButton role="menuitem" icon={icon} disabled={item.disabled} disabledFocusable={item.disabled} title={title} className={buttonClassName} style={{ whiteSpace: "nowrap" }}>{label}</ToolbarButton>
            </MenuTrigger>
            <MenuPopover>
                <MenuList>
                    {item.subMenuProps.items.map(renderSubMenuItem)}
                </MenuList>
            </MenuPopover>
        </Menu>;
    }
    if (item.canCheck) {
        return <ToolbarButton
            key={item.key}
            role="menuitem"
            icon={icon}
            disabled={item.disabled}
            disabledFocusable={item.disabled}
            title={title}
            appearance={item.checked ? "primary" : undefined}
            className={buttonClassName}
            style={{ whiteSpace: "nowrap" }}
            onClick={item.onClick as any}
        >
            {label}
        </ToolbarButton>;
    }
    if (item.href) {
        return <ToolbarButton
            key={item.key}
            role="menuitem"
            as="a"
            href={item.href}
            icon={icon}
            disabled={item.disabled}
            disabledFocusable={item.disabled}
            title={title}
            className={buttonClassName}
            style={{ whiteSpace: "nowrap" }}
        >
            {label}
        </ToolbarButton>;
    }
    return <ToolbarButton
        key={item.key}
        role="menuitem"
        icon={icon}
        disabled={item.disabled}
        disabledFocusable={item.disabled}
        appearance="transparent"
        title={title}
        className={buttonClassName}
        style={{ whiteSpace: "nowrap" }}
        onClick={item.onClick as any}
    >
        {label}
    </ToolbarButton>;
}

const OverflowMenuEntry: React.FunctionComponent<{ item: ICommandBarItemProps }> = ({ item }) => {
    const isVisible = useIsOverflowItemVisible(item.key);
    if (isVisible || item.hidden || item.itemType === ContextualMenuItemType.Divider) return null;
    return (
        <MenuItem
            icon={renderIcon(item.iconProps?.iconName, item.iconElement)}
            disabled={item.disabled}
            onClick={item.onClick as any}
        >
            {item.text}
        </MenuItem>
    );
};

const ToolbarOverflowMenu: React.FunctionComponent<{ items: ICommandBarItemProps[] }> = ({ items }) => {
    const { ref, isOverflowing } = useOverflowMenu<HTMLButtonElement>();
    return (
        <Menu>
            <MenuTrigger disableButtonEnhancement>
                <ToolbarButton
                    ref={ref}
                    icon={<MoreHorizontalRegular />}
                    aria-label="More items"
                    style={{ display: isOverflowing ? undefined : "none" }}
                />
            </MenuTrigger>
            {isOverflowing && (
                <MenuPopover>
                    <MenuList>
                        {items.map(item => <OverflowMenuEntry key={item.key} item={item} />)}
                    </MenuList>
                </MenuPopover>
            )}
        </Menu>
    );
};

export const CommandBar: React.FunctionComponent<CommandBarV9Props> = ({ items, farItems }) => {
    const styles = useStyles();
    return <Toolbar role="menubar" className={styles.toolbar}>
        <Overflow padding={40}>
            <ToolbarGroup role="presentation" className={styles.itemsGroup}>
                {items.map(item => {
                    if (item.hidden || item.onRender) {
                        return renderItem(item, styles.button);
                    }
                    if (item.subMenuProps) {
                        return (
                            <OverflowItem key={item.key} id={item.key}>
                                <span style={{ display: "inline-flex" }}>
                                    {renderItem(item, styles.button)}
                                </span>
                            </OverflowItem>
                        );
                    }
                    return (
                        <OverflowItem key={item.key} id={item.key}>
                            {renderItem(item, styles.button) as React.ReactElement}
                        </OverflowItem>
                    );
                })}
                <ToolbarOverflowMenu key="overflow-menu" items={items} />
            </ToolbarGroup>
        </Overflow>
        <ToolbarGroup role="presentation" className={styles.farItemsGroup}>
            {farItems?.map(item => renderItem(item, styles.button))}
        </ToolbarGroup>
    </Toolbar>;
};
