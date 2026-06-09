import * as React from "react";
import { Button, makeStyles, Skeleton, SkeletonItem, Tooltip } from "@fluentui/react-components";
import nlsHPCC from "../nlsHPCC";
import { useGet } from "./hooks/useWsStore";

interface RecentFilterProps {
    ws_key: string;
    widget: { NewPage };
    filter: object;
}

const useRecentFilterStyles = makeStyles({
    root: {
        width: "100%",
        margin: "0 0 10px 0",
        "& thead": {
            fontWeight: "bold"
        },
        "& td": {
            padding: "8px 24px 8px 16px",
            borderBottom: "1px solid #e0e0e0"
        }
    },
    placeholder: {
        margin: "0 0 10px 0"
    }
});

export const RecentFilters: React.FunctionComponent<RecentFilterProps> = ({
    ws_key, widget, filter
}) => {
    const recentFilterStyles = useRecentFilterStyles();
    const { data, loading } = useGet(ws_key, filter);

    const handleClick = (e) => {
        const tempObj = JSON.parse(decodeURIComponent(e.currentTarget.value));
        widget.NewPage.onClick(e, tempObj);
    };

    const cleanUpFilter = (value: string) => {
        const result = value.replace(/[{}'"]+/g, "");
        return result;
    };

    return (
        <>
            <h4>{nlsHPCC.RecentFilters}</h4>
            {loading ? (
                <div className={recentFilterStyles.placeholder}><Skeleton><SkeletonItem size={48} /></Skeleton></div>
            ) : (data ?
                <table aria-label={nlsHPCC.RecentFiltersTable} className={recentFilterStyles.root}>
                    <thead>
                        <tr>
                            <td align="left">{nlsHPCC.FilterDetails}</td>
                            <td align="center">{nlsHPCC.OpenInNewPage}</td>
                        </tr>
                    </thead>
                    <tbody>
                        {data.map((row, idx) => (
                            <tr key={idx}>
                                <td align="left">
                                    <Tooltip content={cleanUpFilter(JSON.stringify(row))} relationship="label">
                                        <span>{cleanUpFilter(JSON.stringify(row))}</span>
                                    </Tooltip>
                                </td>
                                <td align="center"><Button appearance="primary" value={encodeURIComponent(JSON.stringify(row))} onClick={handleClick}>Open</Button></td>
                            </tr>
                        ))}
                    </tbody>
                </table> : <h6>{nlsHPCC.NoRecentFiltersFound}</h6>)}
        </>
    );
};