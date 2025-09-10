import * as React from "react";
import { Card, Text, makeStyles, tokens, mergeClasses, Toolbar, ToolbarButton, ToolbarDivider, Overflow, OverflowItem } from "@fluentui/react-components";
import { OverflowMenu as ActionsOverflowMenu } from "../controls/OverflowMenu";
import { ArrowMinimize20Regular, ArrowMaximize20Regular } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";

const useStyles = makeStyles({
    card: {
        display: "flex",
        flexDirection: "column",
        margin: 0,
        overflow: "hidden",
        position: "relative",
        "@media (prefers-color-scheme: dark)": {
            border: `1px solid ${tokens.colorNeutralStroke1}`
        }
    },
    headerContainer: {
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        columnGap: tokens.spacingHorizontalS,
        marginBottom: 0
    },
    headerOverlay: {
        position: "absolute",
        top: tokens.spacingVerticalXS,
        right: tokens.spacingHorizontalXS,
        zIndex: 1
    },
    headerActions: {
        display: "flex",
        alignItems: "center",
        columnGap: tokens.spacingHorizontalXS,
        flexShrink: 0
    },
    headerActionBtn: {
        padding: "2px",
        minWidth: "24px",
        height: "24px"
    },
    headerActionIcon: {
        width: "16px",
        height: "16px"
    },
    headerLeft: {
        display: "flex",
        alignItems: "center",
        columnGap: tokens.spacingHorizontalS,
        flex: 1,
        minWidth: 0
    },
    headerName: {
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
        minWidth: 0,
        flex: 1,
        selectors: {
            ".fui-Text": {
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                minWidth: 0,
                display: "block"
            }
        }
    },
    headerIconBase: {
        width: "20px",
        height: "20px",
        flexShrink: 0
    },
    content: {
        display: "flex",
        flexDirection: "column",
        rowGap: tokens.spacingVerticalXS,
        flex: 1,
        minHeight: 0,
        overflow: "auto"
    },
    footerRow: {
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        columnGap: tokens.spacingHorizontalS,
        marginTop: tokens.spacingVerticalXS,
        minWidth: 0
    },
    footerLeft: {
        display: "flex",
        alignItems: "center",
        columnGap: tokens.spacingHorizontalS,
        minWidth: 0,
        overflow: "hidden"
    },
    footerText: {
        color: tokens.colorNeutralForeground3,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap"
    }
});

export interface GenericCardProps {
    headerIcon?: React.ReactNode;
    headerText?: React.ReactNode;
    headerActions?: React.ReactNode;
    headerActionsMinVisible?: number;
    headerOverlay?: boolean;
    minimizable?: boolean;
    minimized?: boolean;
    onToggleMinimize?: () => void;
    children?: React.ReactNode;
    contentClassName?: string;
    footerText?: React.ReactNode;
    footerExtraInfo?: React.ReactNode;
    className?: string;
    style?: React.CSSProperties;
    expandInGrid?: boolean;
}

export const GenericCard: React.FunctionComponent<GenericCardProps> = ({
    headerIcon,
    headerText,
    headerActions,
    headerActionsMinVisible = 2,
    headerOverlay = false,
    minimizable = false,
    minimized = false,
    onToggleMinimize,
    children,
    contentClassName,
    footerText,
    footerExtraInfo,
    className,
    style,
    expandInGrid = false
}) => {
    const styles = useStyles();
    const cardClass = mergeClasses(styles.card, className);

    const computedStyle: React.CSSProperties = React.useMemo(() => {
        const base: React.CSSProperties = { width: "100%", height: "100%" };
        if (expandInGrid && !minimized) {
            base.gridColumn = "span 2";
            base.gridRow = "span 2";
        }
        return { ...base, ...style };
    }, [expandInGrid, minimized, style]);
    const actionNodes = React.useMemo(() => {
        const flatten = (nodes: React.ReactNode): React.ReactElement[] => {
            const out: React.ReactElement[] = [];
            React.Children.forEach(nodes, (child) => {
                if (!child) return;
                if (React.isValidElement(child) && child.type === React.Fragment) {
                    out.push(...flatten(child.props.children));
                } else if (React.isValidElement(child)) {
                    out.push(child);
                }
            });
            return out;
        };
        const builtinToggle = minimizable && typeof onToggleMinimize === "function" ? (
            <OverflowItem id="minmax" key="minmax">
                <ToolbarButton
                    icon={minimized ? <ArrowMaximize20Regular /> : <ArrowMinimize20Regular />}
                    aria-label={minimized ? nlsHPCC.Maximize : nlsHPCC.Minimize}
                    title={minimized ? nlsHPCC.Maximize : nlsHPCC.Minimize}
                    onClick={() => onToggleMinimize?.()}
                    className={styles.headerActionBtn}
                />
            </OverflowItem>
        ) : null;
        return flatten(<>{headerActions}{builtinToggle}</>);
    }, [headerActions, minimizable, minimized, onToggleMinimize, styles.headerActionBtn]);

    const [toolbarItems, menuItems, idToOnClick] = React.useMemo(() => {
        type MenuItem = { id: string; icon?: React.ReactElement; label: string; disabled?: boolean };
        const items: React.ReactNode[] = [];
        const menu: MenuItem[] = [];
        const handlers = new Map<string, () => void>();
        const seenIds = new Set<string>();

        const asSmallIcon = (el?: React.ReactElement) =>
            el ? React.cloneElement(el, { className: mergeClasses(el.props?.className, styles.headerActionIcon) }) : undefined;

        const getLabelFromProps = (props: any): string => {
            const title: string | undefined = props?.title;
            const ariaLabel: string | undefined = props?.["aria-label"];
            const textChild = typeof props?.children === "string" ? props.children : undefined;
            return title || ariaLabel || textChild || "";
        };

        const pushMenu = (id: string, icon: React.ReactElement | undefined, label: string, disabled: boolean, onClick?: () => void) => {
            menu.push({ id, icon: asSmallIcon(icon), label, disabled });
            if (onClick) handlers.set(id, onClick);
        };

        actionNodes.forEach((child, idx) => {
            if (!React.isValidElement(child)) return;
            const childEl = child as React.ReactElement<any>;

            if (child.type === ToolbarDivider) {
                items.push(child);
                return;
            }

            if (childEl.type === OverflowItem) {
                const id = (childEl.props?.id as string) || `hdr-action-${idx}`;
                if (seenIds.has(id)) return;
                seenIds.add(id);
                items.push(React.cloneElement(childEl as any, { id, key: id } as any));

                const innerArr = React.Children.toArray(childEl.props?.children ?? []);
                const inner = (innerArr[0] as React.ReactElement<any> | undefined);
                if (inner && React.isValidElement(inner)) {
                    if ((inner as React.ReactElement<any>).type === ToolbarButton) {
                        const innerEl = inner as React.ReactElement<any>;
                        const label = getLabelFromProps(innerEl.props);
                        const icon = asSmallIcon(innerEl.props?.icon as React.ReactElement | undefined);
                        const disabled = !!innerEl.props?.disabled;
                        const onClick = innerEl.props?.onClick as undefined | (() => void);
                        pushMenu(id, icon, label, disabled, onClick);
                    } else {
                        const ip = (inner as any)?.props ?? {};
                        pushMenu(id, undefined, getLabelFromProps(ip), !!ip?.disabled, ip?.onClick);
                    }
                }
                return;
            }

            if (childEl.type === ToolbarButton) {
                let id = (childEl.props?.id as string) || `hdr-action-${idx}`;
                while (seenIds.has(id)) id = `${id}-${idx}`;
                seenIds.add(id);
                const icon = asSmallIcon(childEl.props?.icon as React.ReactElement | undefined);
                const label = getLabelFromProps(childEl.props);
                const disabled = !!childEl.props?.disabled;
                const onClick = childEl.props?.onClick as undefined | (() => void);
                items.push(
                    <OverflowItem id={id} key={id}>
                        {React.cloneElement(childEl as any, { icon, className: mergeClasses(childEl.props?.className, styles.headerActionBtn) } as any)}
                    </OverflowItem>
                );
                pushMenu(id, icon, label, disabled, onClick);
                return;
            }

            let id = (childEl.props?.id as string) || `hdr-action-${idx}`;
            while (seenIds.has(id)) id = `${id}-${idx}`;
            seenIds.add(id);

            const icon = childEl.props?.icon as React.ReactElement | undefined;
            const disabled = !!childEl.props?.disabled;
            const label = getLabelFromProps(childEl.props);
            const onClick = childEl.props?.onClick as undefined | ((e?: any) => void);
            items.push(
                <OverflowItem id={id} key={id}>
                    <ToolbarButton
                        icon={asSmallIcon(icon)}
                        title={childEl.props?.title}
                        aria-label={childEl.props?.["aria-label"] || label || undefined}
                        disabled={disabled}
                        onClick={() => onClick?.()}
                        className={styles.headerActionBtn}
                    />
                </OverflowItem>
            );
            pushMenu(id, icon, label, disabled, onClick);
        });

        return [items, menu, handlers] as const;
    }, [actionNodes, styles.headerActionBtn, styles.headerActionIcon]);

    const renderHeaderText = React.useCallback((node?: React.ReactNode) => {
        if (!node) return null;
        if (typeof node === "string") {
            return <Text className={styles.headerName} truncate weight="semibold" title={node}>
                {node}
            </Text>;
        }
        if (React.isValidElement(node)) {
            const isTooltipLike = (node.props && ("content" in node.props || node.props?.relationship));
            if (isTooltipLike) {
                const trigger = node.props?.children as React.ReactNode;
                if (React.isValidElement(trigger)) {
                    const triggerChildren = trigger.props?.children;
                    const title = typeof triggerChildren === "string" ? triggerChildren : undefined;
                    const clonedTrigger = React.cloneElement(trigger as React.ReactElement<any>, {
                        truncate: true,
                        className: mergeClasses(trigger.props?.className, styles.headerName),
                        title: trigger.props?.title ?? title
                    });
                    return React.cloneElement(node as React.ReactElement<any>, {}, clonedTrigger);
                }
            }
        }
        return <div className={styles.headerName}>{node}</div>;
    }, [styles.headerName]);

    return <Card className={cardClass} style={computedStyle}>
        {headerOverlay && actionNodes.length > 0 && (
            <div className={styles.headerOverlay}>
                <div className={styles.headerActions}>
                    <Overflow minimumVisible={headerActionsMinVisible}>
                        <Toolbar size="small" aria-label="Card actions">
                            {toolbarItems}
                            <ActionsOverflowMenu
                                menuItems={menuItems}
                                onMenuSelect={(mi) => {
                                    const handler = idToOnClick.get(mi.id);
                                    handler?.();
                                }}
                            />
                        </Toolbar>
                    </Overflow>
                </div>
            </div>
        )}

        {!headerOverlay && (headerIcon || headerText || headerActions) && (
            <div className={styles.headerContainer}>
                <div className={styles.headerLeft}>
                    {headerIcon && <span className={styles.headerIconBase}>{headerIcon}</span>}
                    {renderHeaderText(headerText)}
                </div>
                {actionNodes.length > 0 && (
                    <div className={styles.headerActions}>
                        <Overflow minimumVisible={headerActionsMinVisible}>
                            <Toolbar size="small" aria-label="Card actions">
                                {toolbarItems}
                                <ActionsOverflowMenu
                                    menuItems={menuItems}
                                    onMenuSelect={(mi) => {
                                        const handler = idToOnClick.get(mi.id);
                                        handler?.();
                                    }}
                                />
                            </Toolbar>
                        </Overflow>
                    </div>
                )}
            </div>
        )}

        {children && (
            <div className={mergeClasses(styles.content, contentClassName)}>
                {children}
            </div>
        )}

        {(footerText || footerExtraInfo) && (
            <div className={styles.footerRow}>
                <div className={styles.footerLeft}>
                    {footerText && (
                        typeof footerText === "string" ? (
                            <Text className={styles.footerText} truncate title={footerText}>{footerText}</Text>
                        ) : (
                            <div className={styles.footerText}>{footerText}</div>
                        )
                    )}
                    {footerExtraInfo}
                </div>
            </div>
        )}
    </Card>;
};
