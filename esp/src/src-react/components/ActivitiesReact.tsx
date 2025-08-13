import * as React from "react";
import { Button, Card, Tooltip, makeStyles, tokens, Text, Link, List, ListItem, Toolbar, ToolbarButton, Menu, MenuDivider, MenuGroup, MenuGroupHeader, MenuItem, MenuList, MenuPopover, MenuTrigger, mergeClasses } from "@fluentui/react-components";
import { Pause20Regular, Play20Regular, DatabaseWindow20Regular, FolderOpen20Regular, ArrowClockwise20Regular, Delete20Regular, MoreHorizontal20Regular } from "@fluentui/react-icons";
import { type ServerJobQueue, useServerJobQueues } from "../hooks/queue";
import nlsHPCC from "src/nlsHPCC";
import { getStateImage } from "src/ESPWorkunit";
import * as Utility from "src/Utility";
import { getStateImage as getDFUStateImage } from "src/ESPDFUWorkunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { useBuildInfo } from "../hooks/platform";
import { Summary } from "./DiskUsage";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "react-reflex";
import { classNames, styles } from "src-react/layouts/react-reflex";

const useStyles = makeStyles({
    queuesRoot: {
        display: "flex",
        flexDirection: "column",
        flex: 1,
        height: "100%",
        minHeight: 0,
        overflow: "hidden"
    },
    toolbar: {
        display: "flex",
        justifyContent: "flex-start",
        alignItems: "center",
        paddingBlock: tokens.spacingVerticalXXS,
        paddingInline: tokens.spacingHorizontalM
    },
    container: {
        display: "grid",
        gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
        columnGap: tokens.spacingHorizontalM,
        rowGap: tokens.spacingHorizontalM,
        alignItems: "start",
        alignContent: "start",
        justifyContent: "start",
        placeContent: "start",
        gridAutoRows: "320px",
        paddingBlock: tokens.spacingVerticalXXS,
        paddingInline: tokens.spacingHorizontalM,
        // Allow local vertical scrolling within Queues area
        flex: 1,
        minHeight: 0,
        overflowY: "auto"
    },
    card: {
        display: "flex",
        flexDirection: "column",
        height: "320px",
        minHeight: "320px",
        maxHeight: "320px",
        margin: 0,
        overflow: "hidden",
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
    headerActions: {
        display: "flex",
        alignItems: "center",
        columnGap: tokens.spacingHorizontalXS
    },
    headerLeft: {
        display: "flex",
        alignItems: "center",
        columnGap: tokens.spacingHorizontalS,
        flex: 1,
        minWidth: 0
    },
    headerRow: {
        display: "flex",
        alignItems: "center",
        justifyContent: "flex-start",
        columnGap: tokens.spacingHorizontalS,
        flex: 1,
        minWidth: 0,
        maxWidth: "100%"
    },
    headerTitle: {
        display: "flex",
        alignItems: "center",
        columnGap: tokens.spacingHorizontalS,
        flex: 1,
        minWidth: 0
    },
    headerName: {
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap"
    },
    headerNameWrapper: {
        flex: 1,
        minWidth: 0,
        overflow: "hidden",
        maxWidth: "100%"
    },
    headerIcon: {
        width: "16px",
        height: "16px",
        objectFit: "contain"
    },
    headerIconWrap: {
        position: "relative",
        width: "16px",
        height: "16px",
        flexShrink: 0
    },
    headerIconBase: {
        width: "20px",
        height: "20px",
        flexShrink: 0
    },
    headerIconOverlay: {
        position: "absolute",
        right: "-2px",
        bottom: "-2px",
        width: "10px",
        height: "10px"
    },
    previewImg: {
        width: "32px",
        height: "32px",
        objectFit: "contain",
        borderRadius: tokens.borderRadiusMedium
    },
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
    footerRow: {
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        columnGap: tokens.spacingHorizontalS,
        marginTop: tokens.spacingVerticalXS,
        minWidth: 0
    },
    footerStatusWrap: {
        flex: 1,
        minWidth: 0,
        overflow: "hidden",
        maxWidth: "100%"
    },
    footerStatus: {
        flex: 1,
        minWidth: 0,
        display: "block",
        width: "100%",
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap"
    },
    muted: {
        color: tokens.colorNeutralForeground3
    },
    jobActionIcon: {
        width: "14px",
        height: "14px"
    }
});

// Local UI type for workunit rows used by the queue card
type UIWorkunit = {
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

type QueueCardProps = {
    q: ServerJobQueue;
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
};

type WorkunitListProps = {
    list: UIWorkunit[];
    setPriority: (wu: UIWorkunit, priority: "high" | "normal" | "low") => void;
    moveTop: (wu: UIWorkunit) => void;
    moveUp: (wu: UIWorkunit) => void;
    moveDown: (wu: UIWorkunit) => void;
    moveBottom: (wu: UIWorkunit) => void;
    wuPause: (wu: UIWorkunit, now: boolean) => void;
    wuResume: (wu: UIWorkunit) => void;
};

type WorkunitListItemProps = {
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

const WorkunitListItem: React.FC<WorkunitListItemProps> = ({ wu, idx, listLength, setPriority, moveTop, moveUp, moveDown, moveBottom, wuPause, wuResume }) => {
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
    } else if (instance && !(stateText && (stateText as any).indexOf && (stateText as any).indexOf(instance) !== -1)) {
        stateText = stateText ? `${stateText} [${instance}]` : instance;
    }
    const tooltipContent = (
        <div style={{ display: "grid", rowGap: 2 }}>
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
    return (
        <ListItem key={wuid}>
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
                                    icon={wu.isPaused ? <Play20Regular className={styles.jobActionIcon} /> : <Pause20Regular className={styles.jobActionIcon} />}
                                    onClick={(e) => {
                                        if (wu.isPaused) {
                                            wuResume(wu);
                                        } else {
                                            wuPause(wu, e.ctrlKey);
                                        }
                                    }}
                                />
                                <Menu>
                                    <MenuTrigger disableButtonEnhancement>
                                        <Button
                                            appearance="transparent"
                                            size="small"
                                            aria-label={nlsHPCC.Open}
                                            title={nlsHPCC.Open}
                                            icon={<MoreHorizontal20Regular />}
                                            style={{ padding: 0, minWidth: 0 }}
                                        />
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
                                </Menu>
                            </div>
                        </div>
                    </div>
                </Tooltip>
            </div>
        </ListItem>
    );
};

const WorkunitList: React.FC<WorkunitListProps> = ({ list, setPriority, moveTop, moveUp, moveDown, moveBottom, wuPause, wuResume }) => {
    const styles = useStyles();
    return (
        <List className={styles.jobsList} navigationMode="composite">
            {list.length === 0 && <Text className={styles.muted}>{nlsHPCC.Empty || ""}</Text>}
            {list.map((wu, idx) => (
                <WorkunitListItem
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
        </List>
    );
};

const QueueCard: React.FC<QueueCardProps> = ({ q, onOpen, pause, resume, clear, setPriority, moveTop, moveUp, moveDown, moveBottom, wuPause, wuResume }) => {
    const styles = useStyles();
    const displayName = q.title;
    const key = `${q.kind}:${q.targetCluster?.ClusterName || q.serverJobQueue?.ServerName || q.serverJobQueue?.QueueName}`;
    return (
        <Card key={key} className={styles.card}>
            <div className={styles.headerContainer}>
                <div className={styles.headerLeft}>
                    <Tooltip content={q.paused ? nlsHPCC.Stopped : nlsHPCC.Active} relationship="label">
                        <DatabaseWindow20Regular
                            className={styles.headerIconBase}
                            style={{ color: q.paused ? tokens.colorStatusDangerForeground1 : tokens.colorStatusSuccessForeground1 }}
                        />
                    </Tooltip>
                    <Tooltip content={displayName} relationship="label">
                        <Text className={styles.headerName} truncate weight="semibold">{displayName}</Text>
                    </Tooltip>
                </div>
                <div className={styles.headerActions}>
                    <Button appearance="transparent" size="small" icon={q.paused ? <Play20Regular /> : <Pause20Regular />} aria-label={q.paused ? nlsHPCC.Resume : nlsHPCC.Pause} title={q.paused ? nlsHPCC.Resume : nlsHPCC.Pause} onClick={() => (q.paused ? resume(q) : pause(q))} />
                    <Button appearance="transparent" size="small" icon={<FolderOpen20Regular />} aria-label={nlsHPCC.Open} title={nlsHPCC.Open} onClick={() => onOpen(q)} />
                    <Button appearance="transparent" size="small" icon={<Delete20Regular />} aria-label={nlsHPCC.Clear} title={nlsHPCC.Clear} onClick={() => clear(q)} />
                </div>
            </div>
            <WorkunitList
                list={q.workunits as UIWorkunit[]}
                setPriority={setPriority}
                moveTop={moveTop}
                moveUp={moveUp}
                moveDown={moveDown}
                moveBottom={moveBottom}
                wuPause={wuPause}
                wuResume={wuResume}
            />
            <div className={styles.footerRow}>
                <div className={styles.footerStatusWrap}>
                    {(() => {
                        return (
                            <Tooltip content={q.statusDetails} relationship="label">
                                <Text className={mergeClasses(styles.footerStatus, styles.muted)} truncate>{q.paused ? nlsHPCC.Stopped : ""}</Text>
                            </Tooltip>
                        );
                    })()}
                </div>
            </div>
        </Card>
    );
};

const Queues: React.FC = () => {
    const styles = useStyles();
    const { queues, refresh, pause, resume, clear, setPriority, moveTop, moveUp, moveDown, moveBottom, wuPause, wuResume } = useServerJobQueues();

    const onOpen = React.useCallback((q: ServerJobQueue) => {
        const cluster = q.kind === "ServerJobQueue" ? q.serverJobQueue?.ServerName : q.targetCluster?.ClusterName;
        const url = `#/operations/clusters/${cluster}`;
        window.location.href = url;
    }, []);

    return <div className={styles.queuesRoot}>
        <div className={styles.toolbar}>
            <Toolbar>
                <ToolbarButton appearance="subtle" icon={<ArrowClockwise20Regular />} aria-label={nlsHPCC.Refresh} onClick={() => refresh?.()}>
                    {nlsHPCC.Refresh}
                </ToolbarButton>
            </Toolbar>
        </div>
        <div className={styles.container}>
            {queues.length === 0 ? (
                <Text>{nlsHPCC.FetchingData}</Text>
            ) : (
                queues.map((q: ServerJobQueue) => (
                    <QueueCard
                        key={`${q.kind}:${q.targetCluster?.ClusterName || q.serverJobQueue?.ServerName || q.serverJobQueue?.QueueName}`}
                        q={q}
                        onOpen={onOpen}
                        pause={pause}
                        resume={resume}
                        clear={clear}
                        setPriority={setPriority as any}
                        moveTop={moveTop as any}
                        moveUp={moveUp as any}
                        moveDown={moveDown as any}
                        moveBottom={moveBottom as any}
                        wuPause={wuPause as any}
                        wuResume={wuResume as any}
                    />
                ))
            )}
        </div>
    </div>;
};

export const Activities: React.FC = () => {
    const [, { isContainer }] = useBuildInfo();

    if (isContainer) {
        return <HolyGrail
            main={
                <Queues />
            }
        />;
    }

    return <HolyGrail
        main={
            <ReflexContainer orientation="horizontal">
                <ReflexElement size={100} minSize={100} style={{ overflow: "hidden" }}>
                    <Summary />
                </ReflexElement>
                <ReflexSplitter style={styles.reflexSplitter}>
                    <div className={classNames.reflexSplitterDiv}></div>
                </ReflexSplitter>
                <ReflexElement>
                    <Queues />
                </ReflexElement>
            </ReflexContainer>
        }
    />;
};
