import * as React from "react";
import { PrimaryButton, Shimmer, ShimmerElementType, TooltipHost } from "@fluentui/react";
import { mergeStyleSets } from "@fluentui/style-utilities";
import nlsHPCC from "../nlsHPCC";
import { useGet } from "./hooks/useWsStore";

interface RecentFilterProps {
    ws_key: string;
    widget: { NewPage };
    filter: object;
}

const recentFilterStyles = mergeStyleSets({
    root: {
        width: "100%",
        margin: "0 0 10px 0",
        "thead": {
            fontWeight: "bold"
        },
        "td": {
            padding: "8px 24px 8px 16px",
            borderBottom: "1px solid #e0e0e0"
        }
    },
    placeholder: {
        margin: "0 0 10px 0",
        ".ms-Shimmer-shimmerWrapper": {
            marginBottom: "6px"
        }
    }
});

export const RecentFilters: React.FunctionComponent<RecentFilterProps> = ({
    ws_key, widget, filter
}) => {
    const { data, loading } = useGet(ws_key, filter);

    const handleClick = (e) => {
        const tempObj = JSON.parse(decodeURIComponent(e.currentTarget.value));
        widget.NewPage.onClick(tempObj);
    };

    const shimmerElements = React.useMemo(() => [
        { type: ShimmerElementType.line, height: 48 }
    ], []);

    const cleanUpFilter = (value: string) => {
        const result = value.replace(/[{}'"]+/g, "");
        return result;
    };

    return (
        <>
            <h4>{nlsHPCC.RecentFilters}</h4>
            {loading ? (
                <div className={recentFilterStyles.placeholder}><Shimmer shimmerElements={shimmerElements} /></div>
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
                                    <TooltipHost content={cleanUpFilter(JSON.stringify(row))}>
                                        <span>{cleanUpFilter(JSON.stringify(row))}</span>
                                    </TooltipHost>
                                </td>
                                <td align="center"><PrimaryButton value={encodeURIComponent(JSON.stringify(row))} onClick={handleClick}>Open</PrimaryButton></td>
                            </tr>
                        ))}
                    </tbody>
                </table> : <h6>{nlsHPCC.NoRecentFiltersFound}</h6>)}
        </>
    );
};