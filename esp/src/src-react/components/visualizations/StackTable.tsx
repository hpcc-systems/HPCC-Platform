import * as React from "react";

import { Tooltip, tokens, useId } from "@fluentui/react-components";

interface StackTableProps {
    label?: string;
    data: [string | number, string | number][];
    rowCount?: number;
    overflowLabel?: string;
    overflowValue?: string;
    tableStyle?: React.CSSProperties;
    tableRowStyle?: React.CSSProperties;
    headerStyle?: React.CSSProperties;
    labelStyle?: React.CSSProperties;
    valueStyle?: React.CSSProperties;
}

export const StackTable: React.FunctionComponent<StackTableProps> = ({
    label = "",
    data,
    rowCount = 9,
    overflowLabel = "Other...",
    overflowValue = "",
    tableStyle = {
        marginLeft: 6
    },
    tableRowStyle = {
        marginTop: 6
    },
    headerStyle = {
        height: tokens.fontSizeBase300,
        fontWeight: tokens.fontWeightBold
    },
    labelStyle = {
        height: tokens.fontSizeBase300,
    },
    valueStyle = {
        height: tokens.fontSizeBase200,
        paddingLeft: 6,
        minWidth: 50,
        maxWidth: 200,
        textAlign: "right"
    },
}) => {

    const tooltipId = useId("tooltip");

    const tooltipContent = (
        <div style={{ display: "flex", flexDirection: "column", margin: 10, padding: 0 }}>
            {
                data
                    .filter((n, i) => i >= rowCount)
                    .map((row, rowIdx) => {
                        return <div key={rowIdx}>
                            <div style={{ display: "flex", flexDirection: "row", ...tableRowStyle }}>
                                {
                                    row.map((n, i) => {
                                        return <div
                                            key={i}
                                            style={{ flexGrow: i === 0 ? 1 : 0, ...(i === 0 ? labelStyle : valueStyle) }}
                                        >
                                            {n}
                                        </div>
                                            ;
                                    })
                                }
                            </div>
                        </div>
                            ;
                    })
            }
        </div>
    );
    const labelStackItem = label === "" ? undefined : <div
        key="label"
        style={{ flexGrow: 0, ...headerStyle }}
    >
        {label}
    </div>
        ;

    const dataStackItem = <div key="data" style={{ flexGrow: 1 }}>
        {
            data
                .filter((n, i) => i < rowCount)
                .map((row, rowIdx) => {
                    return <div key={rowIdx}>
                        <div style={{ display: "flex", flexDirection: "row", ...tableRowStyle }}>
                            {
                                row.map((n, i) => {
                                    return <div
                                        key={i}
                                        style={{ flexGrow: i === 0 ? 1 : 0, ...(i === 0 ? labelStyle : valueStyle) }}
                                    >
                                        {n}
                                    </div>
                                        ;
                                })
                            }
                        </div>
                    </div>
                        ;
                })
        }
    </div>
        ;

    const footerStackItem = data.length <= rowCount ? undefined : <div
        key="tooltip-wrapper"
        style={{ flexGrow: 0 }}
    >
        <Tooltip
            relationship="description"
            positioning="above"
            withArrow
            content={{ children: tooltipContent, id: tooltipId }}
        >
            <div style={{ display: "flex", flexDirection: "row", ...tableRowStyle }}>
                <div key="label" style={{ flexGrow: 1, ...labelStyle }}>
                    {overflowLabel}
                </div>
                <div key="value" style={{ flexGrow: 0, ...valueStyle }}>
                    {overflowValue}
                </div>
            </div>
        </Tooltip>
    </div>
        ;

    return <div style={{ display: "flex", flexDirection: "column", ...tableStyle }}>
        {labelStackItem}
        {dataStackItem}
        {footerStackItem}
    </div>
        ;
};
