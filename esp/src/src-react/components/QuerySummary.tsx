import * as React from "react";
import { ScrollablePane, ScrollbarVisibility } from "./controls/ScrollablePane";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { Button, makeStyles, MessageBar, MessageBarActions, MessageBarBody, tokens } from "@fluentui/react-components";
import { CheckmarkCircleRegular, CopyRegular, DismissRegular, PauseRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import { pushUrl } from "../util/history";
import { TableGroup } from "./forms/Groups";
import { useConfirm } from "../hooks/confirm";
import { useQuery } from "../hooks/query";
import * as WsWorkunits from "src/WsWorkunits";
import nlsHPCC from "src/nlsHPCC";
import { copyToClipboard } from "src/Utility";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem } from "../layouts/DockPanel";

const logger = scopedLogger("../components/QuerySummary.tsx");

const useStyles = makeStyles({
    querySummaryHeader: {
        display: "flex",
        flexDirection: "row",
        flexWrap: "wrap",
        alignItems: "center",
        gap: "6px",
        margin: "4px 0 10px 0",
        containerType: "inline-size",
        "& h2": {
            margin: 0,
            display: "flex",
            alignItems: "center",
            gap: "6px"
        }
    },
    copyButton: {
        minWidth: "24px",
        maxWidth: "24px",
        height: "23px",
        margin: "0 0 0 6px",
        "& .fui-Button__icon": {
            height: "16px",
            width: "16px"
        }
    },
    cardsWrapper: {
        display: "grid",
        gridTemplateColumns: "2fr 1fr",
        gap: "12px",
        margin: "0 4px 4px 4px",
        containerType: "inline-size",
        "@container (max-width: 700px)": {
            gridTemplateColumns: "1fr"
        }
    },
    detailsPanel: {
        overflowX: "auto",
        "& a": {
            fontSize: tokens.fontSizeBase300
        }
    }
});

interface QuerySummaryProps {
    querySet: string;
    queryId: string;
    isSuspended?: boolean;
    isActivated?: boolean;
}

export const QuerySummary: React.FunctionComponent<QuerySummaryProps> = ({
    querySet,
    queryId,
    isSuspended = false,
    isActivated = false
}) => {

    const [query, , refreshQuery] = useQuery(querySet, queryId);
    const [suspended, setSuspended] = React.useState(isSuspended);
    const [activated, setActivated] = React.useState(isActivated);

    const styles = useStyles();

    const [showMessageBar, setShowMessageBar] = React.useState(false);
    const dismissMessageBar = React.useCallback(() => setShowMessageBar(false), []);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedWorkunits + "\n" + query?.QueryName,
        onSubmit: React.useCallback(() => {
            const selection = [{ QuerySetId: querySet, Id: queryId }];
            WsWorkunits.WUQuerysetQueryAction(selection, "Delete")
                .then(() => pushUrl("/queries"))
                .catch(err => logger.error(err))
                ;
        }, [queryId, querySet])
    });

    const [ResetConfirm, setShowResetConfirm] = useConfirm({
        title: nlsHPCC.Reset,
        message: nlsHPCC.ResetThisQuery,
        onSubmit: React.useCallback(() => {
            const selection = [{ QuerySetId: querySet, Id: queryId, Name: query?.QueryName }];
            WsWorkunits.WUQuerysetQueryAction(selection, "ResetQueryStats")
                .then((responses) => {
                    const result = responses[0].WUQuerySetQueryActionResponse.Results.Result[0];
                    if (result.Success === false) {
                        logger.error(`${nlsHPCC.Exception} (${result.Code}): ${result.Message}`);
                    } else {
                        logger.notice(`${result.Message}`);
                    }
                    refreshQuery();
                })
                .catch(err => logger.error(err))
                ;
        }, [query?.QueryName, queryId, querySet, refreshQuery])
    });

    const canSave = query && (
        suspended !== query?.Suspended ||
        activated !== query?.Activated
    );

    React.useEffect(() => {
        setActivated(isActivated);
        setSuspended(isSuspended);
    }, [isActivated, isSuspended]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => { refreshQuery(); }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                const selection = [{ QuerySetId: querySet, Id: queryId, Name: query?.QueryName }];
                const actions = [];
                if (suspended !== query?.Suspended) {
                    actions.push(WsWorkunits.WUQuerysetQueryAction(selection, suspended ? "Suspend" : "Unsuspend"));
                }
                if (activated !== query?.Activated) {
                    actions.push(WsWorkunits.WUQuerysetQueryAction(selection, activated ? "Activate" : "Deactivate"));
                }
                Promise
                    .all(actions)
                    .then(() => {
                        refreshQuery();
                        setShowMessageBar(true);
                        const t = window.setTimeout(function () {
                            setShowMessageBar(false);
                            window.clearTimeout(t);
                        }, 2400);
                    })
                    .catch(err => logger.error(err))
                    ;
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, iconProps: { iconName: "Delete" },
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider },
        {
            key: "reset", text: nlsHPCC.Reset, onClick: () => setShowResetConfirm(true)
        },
    ], [activated, canSave, query, queryId, querySet, refreshQuery, setShowDeleteConfirm, setShowResetConfirm, suspended]);

    return <HolyGrail
        header={<>
            <CommandBar items={buttons} />
            {showMessageBar &&
                <MessageBar intent="success">
                    <MessageBarBody>{nlsHPCC.SuccessfullySaved}</MessageBarBody>
                    <MessageBarActions containerAction={<Button onClick={dismissMessageBar} aria-label={nlsHPCC.Close} appearance="transparent" icon={<DismissRegular />} />} />
                </MessageBar>
            }
        </>}
        main={<>
            <DockPanel hideSingleTabs>
                <DockPanelItem key="summary" title="Summary">
                    <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
                        <div className="pane-content">
                            <div className={styles.querySummaryHeader}>
                                <h2>
                                    {suspended && <PauseRegular aria-label={nlsHPCC.Suspended} title={nlsHPCC.Suspended} />}
                                    {activated && <CheckmarkCircleRegular aria-label={nlsHPCC.Activated} title={nlsHPCC.Activated} />}
                                    {query?.QueryName}
                                    <Button title={nlsHPCC.CopyToClipboard} aria-label={nlsHPCC.CopyToClipboard} className={styles.copyButton} icon={<CopyRegular />}
                                        onClick={() => copyToClipboard(query?.QueryName)}
                                    />
                                </h2>
                            </div>
                            <div className={styles.cardsWrapper}>
                                <div className={styles.detailsPanel}>
                                    <TableGroup fields={{
                                        "querySet": { label: nlsHPCC.QuerySet, type: "string", value: query?.QuerySet, readonly: true },
                                        "priority": { label: nlsHPCC.Priority, type: "string", value: query?.Priority || "", readonly: true },
                                        "publishedBy": { label: nlsHPCC.PublishedBy, type: "link", value: query?.PublishedBy || "", href: query?.PublishedBy ? `#/queries?PublishedBy=${encodeURIComponent(query?.PublishedBy)}` : "", readonly: true },
                                        "suspended": { label: nlsHPCC.Suspended, type: "checkbox", value: suspended },
                                        "suspendedBy": { label: nlsHPCC.SuspendedBy, type: "string", value: query?.SuspendedBy || "", readonly: true },
                                        "activated": { label: nlsHPCC.Activated, type: "checkbox", value: activated },
                                        "comment": { label: nlsHPCC.Comment, type: "string", value: query?.Comment || "", readonly: true },
                                        "wuid": { label: nlsHPCC.WUID, type: "link", value: query?.Wuid, href: `#/workunits/${query?.Wuid}`, readonly: true, onCopy: () => copyToClipboard(query?.Wuid) },
                                        "dll": { label: nlsHPCC.Dll, type: "string", value: query?.Dll, readonly: true },
                                        "isLibrary": { label: nlsHPCC.IsLibrary, type: "string", value: query?.IsLibrary ? "true" : "false", readonly: true },
                                    }} onChange={(id, value) => {
                                        switch (id) {
                                            case "suspended":
                                                setSuspended(value);
                                                break;
                                            case "activated":
                                                setActivated(value);
                                                break;
                                            default:
                                                logger.debug(`${id}:  ${value}`);
                                        }
                                    }} />
                                </div>
                            </div>
                        </div>
                    </ScrollablePane>
                </DockPanelItem>
            </DockPanel>
            <DeleteConfirm />
            <ResetConfirm />
        </>}
    />;

};