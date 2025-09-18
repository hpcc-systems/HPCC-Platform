import * as React from "react";
import { Button, Tooltip, makeStyles, tokens, Text, Link, List, ListItem, Menu, MenuDivider, MenuGroup, MenuGroupHeader, MenuItem, MenuList, MenuPopover, MenuTrigger, OverflowItem, InfoLabel, ToolbarButton } from "@fluentui/react-components";
import { Pause16Regular, Play16Regular, DatabaseWindow20Regular, FolderOpen16Regular, Delete16Regular, MoreHorizontal16Regular } from "@fluentui/react-icons";
import { useServerJobQueues, type ServerJobQueue } from "../../hooks/queue";
import nlsHPCC from "src/nlsHPCC";
import { getStateImage } from "src/ESPWorkunit";
import * as Utility from "src/Utility";
import { getStateImage as getDFUStateImage } from "src/ESPDFUWorkunit";
import { useBuildInfo } from "../../hooks/platform";
import { GenericCard } from "./GenericCard";
import { CardGroup } from "./CardGroup";

const useStyles = makeStyles({
    jobsList: {
        display: "flex",
        flexDirection: "column",
        rowGap: tokens.spacingVerticalXS,
        flex: 1,
        minHeight: 0,
        overflowY: "auto",
        overflowX: "hidden",
        marginTop: 0,
        marginBottom: 0,
        "@media (prefers-color-scheme: dark)": {
            scrollbarColor: `${tokens.colorNeutralStroke1} ${tokens.colorNeutralBackground2}`,
            scrollbarWidth: "thin",
            selectors: {
                "::-webkit-scrollbar": {
                    width: "8px",
                    height: "8px"
                },
                "::-webkit-scrollbar-track": {
                    backgroundColor: tokens.colorNeutralBackground2
                },
                "::-webkit-scrollbar-thumb": {
                    backgroundColor: tokens.colorNeutralStroke1,
                    border: `2px solid ${tokens.colorNeutralBackground2}`,
                    borderRadius: "8px"
                }
            }
        }
    },
    gridCell: {
        width: "100%"
    },
    jobRow: {
        display: "flex",
        alignItems: "center",
        columnGap: tokens.spacingHorizontalS,
        minWidth: 0,
        width: "100%"
    },
    jobIcon: {
        width: "14px",
        height: "14px",
        objectFit: "contain"
    },
    jobTexts: {
        display: "flex",
        alignItems: "center",
        columnGap: tokens.spacingHorizontalS,
        minWidth: 0,
        flex: 1
    },
    jobRight: {
        display: "flex",
        alignItems: "center",
        columnGap: "2px",
        marginLeft: "auto",
        minWidth: 0
    },
    jobWuid: {
        whiteSpace: "nowrap"
    },
    jobName: {
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap"
    },
    muted: {
        color: tokens.colorNeutralForeground3
    },

});

export type UIWorkunit = {
    Wuid: string;
    isDFU?: boolean;
    StateID?: number;
    Jobname?: string;
    GraphName?: string;
    GID?: string;
    Owner?: string;
    State?: string;
    Duration?: string;
    Instance?: string;
    isPaused?: boolean;
    Priority?: "high" | "normal" | "low" | string;
};

interface ActiveWorkunitMenuProps {
    wu: UIWorkunit;
    canUp: boolean;
    canDown: boolean;
    setPriority: (wu: UIWorkunit, priority: "high" | "normal" | "low") => void;
    moveTop: (wu: UIWorkunit) => void;
    moveUp: (wu: UIWorkunit) => void;
    moveDown: (wu: UIWorkunit) => void;
    moveBottom: (wu: UIWorkunit) => void;
}

const ActiveWorkunitMenu: React.FunctionComponent<ActiveWorkunitMenuProps> = ({ wu, canUp, canDown, setPriority, moveTop, moveUp, moveDown, moveBottom }) => {
    return <Menu>
        <MenuTrigger disableButtonEnhancement>
            <Button appearance="transparent" size="small" aria-label={nlsHPCC.Open} title={nlsHPCC.Open} icon={<MoreHorizontal16Regular />} style={{ padding: 0, minWidth: 0 }} />
        </MenuTrigger>
        <MenuPopover>
            <MenuList>
                <MenuGroup>
                    <MenuGroupHeader>{nlsHPCC.Priority}</MenuGroupHeader>
                    <MenuItem onClick={() => setPriority(wu, "high")}>
                        {nlsHPCC.High}
                    </MenuItem>
                    <MenuItem onClick={() => setPriority(wu, "normal")}>
                        {nlsHPCC.Normal}
                    </MenuItem>
                    <MenuItem onClick={() => setPriority(wu, "low")}>
                        {nlsHPCC.Low}
                    </MenuItem>
                </MenuGroup>
                <MenuDivider />
                <MenuGroup>
                    <MenuGroupHeader>{nlsHPCC.Move}</MenuGroupHeader>
                    <MenuItem disabled={!canUp} onClick={() => { moveTop(wu); }}>
                        {nlsHPCC.Top}
                    </MenuItem>
                    <MenuItem disabled={!canUp} onClick={() => { moveUp(wu); }}>
                        {nlsHPCC.Up}
                    </MenuItem>
                    <MenuItem disabled={!canDown} onClick={() => { moveDown(wu); }}>
                        {nlsHPCC.Down}
                    </MenuItem>
                    <MenuItem disabled={!canDown} onClick={() => { moveBottom(wu); }}>
                        {nlsHPCC.Bottom}
                    </MenuItem>
                </MenuGroup>
            </MenuList>
        </MenuPopover>
    </Menu>;
};

interface ActiveWorkunitProps {
    wu: UIWorkunit;
    idx: number;
    listLength: number;
    setPriority: (wu: UIWorkunit, priority: "high" | "normal" | "low") => void;
    moveTop: (wu: UIWorkunit) => void;
    moveUp: (wu: UIWorkunit) => void;
    moveDown: (wu: UIWorkunit) => void;
    moveBottom: (wu: UIWorkunit) => void;
    wuPause: (wu: UIWorkunit, now: boolean) => void;
    wuResume: (wu: UIWorkunit) => void;
};

const ActiveWorkunit: React.FunctionComponent<ActiveWorkunitProps> = ({ wu, idx, listLength, setPriority, moveTop, moveUp, moveDown, moveBottom, wuPause, wuResume }) => {
    const styles = useStyles();
    const wuid = wu.Wuid;
    const wimg = wu.isDFU ? getDFUStateImage(wu.StateID) : getStateImage(wu.StateID, false, false);
    const jobName = wu.Jobname ?? "";
    const graphName = wu.GraphName ?? "";
    const gid = wu.GID;
    const owner = wu.Owner ?? "";
    const rawState = wu.State ?? "";
    const duration = wu.Duration ?? "";
    const instance = wu.Instance ?? "";
    let stateText = rawState;
    if (duration) {
        stateText = stateText ? `${stateText} (${duration})` : duration;
    } else if (instance && !stateText.includes(instance)) {
        stateText = stateText ? `${stateText} [${instance}]` : instance;
    }
    const tooltipContent = (
        <div style={{ display: "grid", rowGap: tokens.spacingVerticalXXS }}>
            <Text weight="semibold">{wuid}</Text>
            {stateText && (
                <div>
                    <Text weight="semibold">{nlsHPCC.State}: </Text>
                    <Text>{stateText}</Text>
                </div>
            )}
            {owner && (
                <div>
                    <Text weight="semibold">{nlsHPCC.Owner}: </Text>
                    <Text>{owner}</Text>
                </div>
            )}
            {jobName && (
                <div>
                    <Text weight="semibold">{nlsHPCC.JobName}: </Text>
                    <Text>{jobName}</Text>
                </div>
            )}
            {graphName && (
                <div>
                    <Text weight="semibold">{nlsHPCC.Graph}: </Text>
                    <Text>{gid ? `${graphName}-${gid}` : graphName}</Text>
                </div>
            )}
        </div>
    );
    const canUp = idx > 0;
    const canDown = idx < (listLength - 1);
    const priorityIcon = wu.Priority === "high" ? Utility.getImageURL("priority_high.png") : wu.Priority === "low" ? Utility.getImageURL("priority_low.png") : undefined;
    return <ListItem key={wuid}>
        <div role="gridcell" className={styles.gridCell}>
            <Tooltip content={tooltipContent} relationship="label">
                <div className={styles.jobRow}>
                    {wimg && <img className={styles.jobIcon} alt="state" src={wimg} />}
                    <div className={styles.jobTexts}>
                        <Link className={styles.jobWuid} href={wu.isDFU ? `#/dfuworkunits/${wuid}` : `#/workunits/${wuid}`}>{wuid}</Link>
                        {priorityIcon && <img className={styles.jobIcon} alt="priority" src={priorityIcon} />}
                        <div className={styles.jobRight}>
                            <Link className={styles.jobName} href={`#/workunits/${wuid}/metrics/${graphName}`}>
                                {gid ? `${graphName}-${gid}` : graphName}
                            </Link>
                            <Button
                                appearance="transparent"
                                size="small"
                                aria-label={nlsHPCC.Resume}
                                title={`${nlsHPCC.Resume} (Ctrl+Click = ${nlsHPCC.PauseNow || "Pause Now"})`}
                                icon={wu.isPaused ? <Play16Regular className={styles.jobIcon} /> : <Pause16Regular className={styles.jobIcon} />}
                                onClick={(e) => {
                                    if (wu.isPaused) {
                                        wuResume(wu);
                                    } else {
                                        wuPause(wu, (e as React.MouseEvent).ctrlKey);
                                    }
                                }}
                            />
                            <ActiveWorkunitMenu
                                wu={wu}
                                canUp={canUp}
                                canDown={canDown}
                                setPriority={setPriority}
                                moveTop={moveTop}
                                moveUp={moveUp}
                                moveDown={moveDown}
                                moveBottom={moveBottom}
                            />
                        </div>
                    </div>
                </div>
            </Tooltip>
        </div>
    </ListItem>;
};

interface ActiveWorkunitListProps {
    list: UIWorkunit[];
    setPriority: (wu: UIWorkunit, priority: "high" | "normal" | "low") => void;
    moveTop: (wu: UIWorkunit) => void;
    moveUp: (wu: UIWorkunit) => void;
    moveDown: (wu: UIWorkunit) => void;
    moveBottom: (wu: UIWorkunit) => void;
    wuPause: (wu: UIWorkunit, now: boolean) => void;
    wuResume: (wu: UIWorkunit) => void;
};

const ActiveWorkunitList: React.FunctionComponent<ActiveWorkunitListProps> = ({
    list,
    setPriority,
    moveTop,
    moveUp,
    moveDown,
    moveBottom,
    wuPause,
    wuResume
}) => {
    const styles = useStyles();
    return <List className={styles.jobsList} navigationMode="composite">
        {list.length === 0 && <Text className={styles.muted}>{nlsHPCC.Empty || ""}</Text>}
        {list.map((wu, idx) => (
            <ActiveWorkunit
                key={wu.Wuid}
                wu={wu}
                idx={idx}
                listLength={list.length}
                setPriority={setPriority}
                moveTop={moveTop}
                moveUp={moveUp}
                moveDown={moveDown}
                moveBottom={moveBottom}
                wuPause={wuPause}
                wuResume={wuResume}
            />
        ))}
    </List>;
};

interface StatusDetailsProps {
    details?: string;
}

const StatusDetails: React.FunctionComponent<StatusDetailsProps> = ({ details }) => {
    const d = details ?? "";
    const lines = d.split(";").map(s => s.trim()).filter(Boolean);
    if (lines.length === 0) return null;
    return <div style={{ display: "grid", rowGap: tokens.spacingVerticalXXS }}>
        {lines.map((line, i) => {
            if (i === 0) {
                const idx = line.indexOf(":");
                if (idx >= 0) {
                    const lhs = line.slice(0, idx).trim();
                    const rhs = line.slice(idx + 1).trim();
                    return <div key={i}>
                        <Text weight="semibold">{lhs}: </Text>
                        <Text>{rhs}</Text>
                    </div>;
                }
                return <div key={i}>
                    <Text>{line}</Text>
                </div>;
            }
            const parts: React.ReactNode[] = [];
            let lastIndex = 0;
            const regex = /<([^>]+)>/g;
            let m: RegExpExecArray | null;
            while ((m = regex.exec(line)) !== null) {
                const pre = line.slice(lastIndex, m.index);
                if (pre) parts.push(<Text key={`${i}-n-${lastIndex}`}>{pre}</Text>);
                parts.push(<Text key={`${i}-b-${m.index}`} weight="semibold">{m[1]}</Text>);
                lastIndex = m.index + m[0].length;
            }
            const tail = line.slice(lastIndex);
            if (tail) parts.push(<Text key={`${i}-n-tail`}>{tail}</Text>);
            if (parts.length === 0) parts.push(<Text key={`${i}-only`}>{line}</Text>);
            return <div key={i}>{parts}</div>;
        })}
    </div>;
};

export interface QueueCardProps {
    serverJobQueue: ServerJobQueue;
    onOpen: (q: ServerJobQueue) => void;
    pause: (q: ServerJobQueue) => void;
    resume: (q: ServerJobQueue) => void;
    clear: (q: ServerJobQueue) => void;
    setPriority: (wu: UIWorkunit, priority: "high" | "normal" | "low") => void;
    moveTop: (wu: UIWorkunit) => void;
    moveUp: (wu: UIWorkunit) => void;
    moveDown: (wu: UIWorkunit) => void;
    moveBottom: (wu: UIWorkunit) => void;
    wuPause: (wu: UIWorkunit, now: boolean) => void;
    wuResume: (wu: UIWorkunit) => void;
}

export const QueueCard: React.FunctionComponent<QueueCardProps> = ({
    serverJobQueue,
    onOpen,
    pause,
    resume,
    clear,
    setPriority,
    moveTop,
    moveUp,
    moveDown,
    moveBottom,
    wuPause,
    wuResume
}) => {
    const [, { isContainer }] = useBuildInfo();

    const displayName = serverJobQueue.title;
    const key = `${serverJobQueue.kind}:${serverJobQueue.targetCluster?.ClusterName || serverJobQueue.serverJobQueue?.ServerName || serverJobQueue.serverJobQueue?.QueueName}`;
    const actions = React.useMemo(() => {
        const items: React.ReactNode[] = [
            (
                <OverflowItem id={`queue-${key}-toggle`} key="toggle">
                    <ToolbarButton icon={serverJobQueue.paused ? <Play16Regular /> : <Pause16Regular />} aria-label={serverJobQueue.paused ? nlsHPCC.Resume : nlsHPCC.Pause} title={serverJobQueue.paused ? nlsHPCC.Resume : nlsHPCC.Pause} onClick={() => (serverJobQueue.paused ? resume(serverJobQueue) : pause(serverJobQueue))} />
                </OverflowItem>
            )
        ];
        if (!isContainer) {
            items.push(
                <OverflowItem id={`queue-${key}-open`} key="open">
                    <ToolbarButton icon={<FolderOpen16Regular />} aria-label={nlsHPCC.Open} title={nlsHPCC.Open} onClick={() => onOpen(serverJobQueue)} />
                </OverflowItem>
            );
        }
        items.push(
            <OverflowItem id={`queue-${key}-clear`} key="clear">
                <ToolbarButton icon={<Delete16Regular />} aria-label={nlsHPCC.Clear} title={nlsHPCC.Clear} onClick={() => clear(serverJobQueue)} />
            </OverflowItem>
        );
        return items;
    }, [serverJobQueue, key, isContainer, resume, pause, onOpen, clear]);

    return <GenericCard key={key} headerActionsMinVisible={3} style={{ width: "100%", height: "100%" }}
        headerIcon={
            <Tooltip content={serverJobQueue.paused ? nlsHPCC.Stopped : nlsHPCC.Active} relationship="label">
                <DatabaseWindow20Regular style={{ color: serverJobQueue.paused ? tokens.colorStatusDangerForeground1 : tokens.colorStatusSuccessForeground1 }} />
            </Tooltip>
        }
        headerText={
            <Tooltip content={displayName} relationship="label">
                <Text weight="semibold">{displayName}</Text>
            </Tooltip>
        }
        headerActions={actions} footerText={serverJobQueue.paused ? nlsHPCC.Stopped : undefined} footerExtraInfo={serverJobQueue.paused ? <InfoLabel size="medium" info={<StatusDetails details={serverJobQueue.statusDetails} />}></InfoLabel> : undefined}>
        <ActiveWorkunitList list={serverJobQueue.workunits as UIWorkunit[]} setPriority={setPriority} moveTop={moveTop} moveUp={moveUp} moveDown={moveDown} moveBottom={moveBottom} wuPause={wuPause} wuResume={wuResume} />
    </GenericCard>;
};

export interface QueueCardsProps {
    refreshToken?: number;
}

export const QueueCards: React.FunctionComponent<QueueCardsProps> = ({
    refreshToken
}) => {
    const { queues, refresh, pause, resume, clear, setPriority, moveTop, moveUp, moveDown, moveBottom, wuPause, wuResume } = useServerJobQueues();

    // Prevent continuous re-renders  ---
    const refreshRef = React.useRef<typeof refresh>();
    React.useEffect(() => {
        refreshRef.current = refresh;
    }, [refresh]);

    React.useEffect(() => {
        refreshRef.current?.();
    }, [refreshToken]);

    const onOpen = React.useCallback((q: ServerJobQueue) => {
        const cluster = q.kind === "ServerJobQueue" ? q.serverJobQueue?.ServerName : q.targetCluster?.ClusterName;
        const url = `#/operations/clusters/${cluster}`;
        window.location.href = url;
    }, []);

    return <CardGroup>
        {queues.length === 0 ? (
            <Text>{nlsHPCC.FetchingData}</Text>
        ) : (
            queues.map((q: ServerJobQueue) => (
                <QueueCard
                    key={`${q.kind}:${q.targetCluster?.ClusterName || q.serverJobQueue?.ServerName || q.serverJobQueue?.QueueName}`}
                    serverJobQueue={q}
                    onOpen={onOpen}
                    pause={pause}
                    resume={resume}
                    clear={clear}
                    setPriority={setPriority}
                    moveTop={moveTop}
                    moveUp={moveUp}
                    moveDown={moveDown}
                    moveBottom={moveBottom}
                    wuPause={wuPause}
                    wuResume={wuResume}
                />
            ))
        )}
    </CardGroup>;
};
