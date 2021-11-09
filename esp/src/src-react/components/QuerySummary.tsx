import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Sticky, StickyPositionType } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { pushUrl } from "../util/history";
import { TableGroup } from "./forms/Groups";
import { useConfirm } from "../hooks/confirm";
import { ShortVerticalDivider } from "./Common";
import * as ESPQuery from "src/ESPQuery";
import * as WsWorkunits from "src/WsWorkunits";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("../components/QuerySummary.tsx");

interface QuerySummaryProps {
    querySet: string;
    queryId: string;
}

export const QuerySummary: React.FunctionComponent<QuerySummaryProps> = ({
    querySet,
    queryId
}) => {

    const [query, setQuery] = React.useState<any>();
    const [suspended, setSuspended] = React.useState(false);
    const [activated, setActivated] = React.useState(false);

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
            query?.doReset().catch(err => logger.error(err));
        }, [query])
    });

    const canSave = query && (
        suspended !== query?.Suspended ||
        activated !== query?.Activated
    );

    React.useEffect(() => {
        setQuery(ESPQuery.Get(querySet, queryId));
    }, [setQuery, queryId, querySet]);

    React.useEffect(() => {
        query?.getDetails().then(({ WUQueryDetailsResponse }) => {
            setSuspended(query.Suspended);
            setActivated(query.Activated);
        });
    }, [setActivated, setSuspended, query]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => { query?.refresh(); }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "save", text: nlsHPCC.Save, iconProps: { iconName: "Save" }, disabled: !canSave,
            onClick: () => {
                const selection = [{ QuerySetId: querySet, Id: queryId }];
                const actions = [];
                if (suspended !== query?.Suspended) {
                    actions.push(WsWorkunits.WUQuerysetQueryAction(selection, suspended ? "Suspend" : "Unsuspend"));
                }
                if (activated !== query?.Activated) {
                    actions.push(WsWorkunits.WUQuerysetQueryAction(selection, activated ? "Activate" : "Deactivate"));
                }
                Promise
                    .all(actions)
                    .then(() => query?.refresh())
                    .catch(err => logger.error(err))
                    ;
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, iconProps: { iconName: "Delete" },
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "reset", text: nlsHPCC.Reset, onClick: () => setShowResetConfirm(true)
        },
    ], [activated, canSave, query, queryId, querySet, setShowDeleteConfirm, setShowResetConfirm, suspended]);

    return <>
        <Sticky stickyPosition={StickyPositionType.Header}>
            <CommandBar items={buttons} />
        </Sticky>
        <TableGroup fields={{
            "name": { label: nlsHPCC.Name, type: "string", value: query?.QueryName, readonly: true },
            "querySet": { label: nlsHPCC.QuerySet, type: "string", value: query?.QuerySet, readonly: true },
            "priority": { label: nlsHPCC.Priority, type: "string", value: query?.Priority || "", readonly: true },
            "publishedBy": { label: nlsHPCC.PublishedBy, type: "string", value: query?.PublishedBy || "", readonly: true },
            "suspended": { label: nlsHPCC.Suspended, type: "checkbox", value: suspended },
            "suspendedBy": { label: nlsHPCC.SuspendedBy, type: "string", value: query?.SuspendedBy || "", readonly: true },
            "activated": { label: nlsHPCC.Activated, type: "checkbox", value: activated },
            "comment": { label: nlsHPCC.Comment, type: "string", value: query?.Comment || "", readonly: true },
        }} onChange={(id, value) => {
            switch (id) {
                case "suspended":
                    setSuspended(value);
                    break;
                case "activated":
                    setActivated(value);
                    break;
                default:
                    console.log(id, value);
            }
        }} />
        <hr />
        <TableGroup fields={{
            "wuid": { label: nlsHPCC.WUID, type: "string", value: query?.Wuid, readonly: true },
            "dll": { label: nlsHPCC.Dll, type: "string", value: query?.Dll, readonly: true },
            "wuSnapShot": { label: nlsHPCC.WUSnapShot, type: "string", value: query?.WUSnapShot, readonly: true },
        }} />
        <hr />
        <TableGroup fields={{
            "isLibrary": { label: nlsHPCC.IsLibrary, type: "string", value: query?.IsLibrary ? "true" : "false", readonly: true },
        }} />
        <DeleteConfirm />
        <ResetConfirm />
    </>;

};