import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Pivot, PivotItem, Sticky, StickyPositionType } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as WsWorkunits from "src/WsWorkunits";
import * as ESPQuery from "src/ESPQuery";
import { useConfirm } from "../hooks/confirm";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { QueryErrors } from "./QueryErrors";
import { QueryGraphs } from "./QueryGraphs";
import { QueryLibrariesUsed } from "./QueryLibrariesUsed";
import { QueryLogicalFiles } from "./QueryLogicalFiles";
import { QuerySummaryStats } from "./QuerySummaryStats";
import { QuerySuperFiles } from "./QuerySuperFiles";
import { Resources } from "./Resources";
import { TableGroup } from "./forms/Groups";

const logger = scopedLogger("../components/QueryDetails.tsx");

interface QueryDetailsProps {
    querySet: string;
    queryId: string;
    tab?: string;
}

export const QueryDetails: React.FunctionComponent<QueryDetailsProps> = ({
    querySet,
    queryId,
    tab = "summary"
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
                .catch(logger.error)
                ;
        }, [queryId, querySet])
    });

    const [ResetConfirm, setShowResetConfirm] = useConfirm({
        title: nlsHPCC.Reset,
        message: nlsHPCC.ResetThisQuery,
        onSubmit: React.useCallback(() => {
            query?.doReset().catch(logger.error);
        }, [query])
    });

    const canSave = query && (
        suspended !== query?.Suspended ||
        activated !== query?.Activated
    );

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
                    .catch(logger.error)
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

    const rightButtons = React.useMemo((): ICommandBarItemProps[] => [
    ], []);

    React.useEffect(() => {
        setQuery(ESPQuery.Get(querySet, queryId));
    }, [setQuery, queryId, querySet]);

    React.useEffect(() => {
        query?.getDetails().then(({ WUQueryDetailsResponse }) => {
            setSuspended(query.Suspended);
            setActivated(query.Activated);
        });
    }, [setActivated, setSuspended, query]);

    return <>
        <SizeMe monitorHeight>{({ size }) =>
            <Pivot
                overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
                onLinkClick={evt => {
                    if (evt.props.itemKey === "workunit") {
                        pushUrl(`/workunits/${query?.Wuid}`);
                    } else {
                        pushUrl(`/queries/${querySet}/${queryId}/${evt.props.itemKey}`);
                    }
                }}
            >
                <PivotItem headerText={queryId} itemKey="summary" style={pivotItemStyle(size)} >
                    <Sticky stickyPosition={StickyPositionType.Header}>
                        <CommandBar items={buttons} farItems={rightButtons} />
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
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Errors} itemKey="errors" style={pivotItemStyle(size, 0)}>
                    <QueryErrors queryId={queryId} querySet={querySet} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.LogicalFiles} itemKey="logicalFiles" itemCount={query?.LogicalFiles?.Item?.length || 0} style={pivotItemStyle(size, 0)}>
                    <QueryLogicalFiles queryId={queryId} querySet={querySet} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.SuperFiles} itemKey="superfiles" itemCount={query?.SuperFiles?.SuperFile.length || 0} style={pivotItemStyle(size, 0)}>
                    <QuerySuperFiles queryId={queryId} querySet={querySet} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.LibrariesUsed} itemKey="librariesUsed" itemCount={query?.LibrariesUsed?.Item?.length || 0} style={pivotItemStyle(size, 0)}>
                    <QueryLibrariesUsed queryId={queryId} querySet={querySet} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.SummaryStatistics} itemKey="summaryStatistics" style={pivotItemStyle(size, 0)}>
                    <QuerySummaryStats queryId={queryId} querySet={querySet} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Graphs} itemKey="graphs" itemCount={query?.WUGraphs?.ECLGraph?.length || 0} style={pivotItemStyle(size, 0)}>
                    <QueryGraphs queryId={queryId} querySet={querySet} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Resources} itemKey="resources" style={pivotItemStyle(size, 0)}>
                    <Resources wuid={query?.Wuid} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.TestPages} itemKey="testPages" style={pivotItemStyle(size, 0)}>
                    <DojoAdapter widgetClassID="QueryTestWidget" params={{ Id: queryId, QuerySetId: querySet }} />
                </PivotItem>
                <PivotItem headerText={query?.Wuid} itemKey="workunit"></PivotItem>
            </Pivot>
        }</SizeMe>
        <DeleteConfirm />
        <ResetConfirm />
    </>;
};
