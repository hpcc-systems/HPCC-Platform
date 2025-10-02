import * as React from "react";

import { FontSizes, FontWeights, IStackItemStyles, IStackStyles, ITooltipProps, TooltipHost } from "@fluentui/react";
import { StackShim, StackItemShim } from "@fluentui/react-migration-v8-v9";
import { useId } from "@fluentui/react-hooks";

interface StackTableProps {
    label?: string;
    data: [string | number, string | number][];
    rowCount?: number;
    overflowLabel?: string;
    overflowValue?: string;
    tableStyles?: IStackStyles;
    tableRowStyles?: IStackStyles;
    headerStyles?: IStackItemStyles;
    labelStyles?: IStackItemStyles;
    valueStyles?: IStackItemStyles;
}

export const StackTable: React.FunctionComponent<StackTableProps> = ({
    label = "",
    data,
    rowCount = 9,
    overflowLabel = "Other...",
    overflowValue = "",
    tableStyles = {
        root: {
            marginLeft: 6
        }
    },
    tableRowStyles = {
        root: {
            marginTop: 6
        }
    },
    headerStyles = {
        root: {
            height: FontSizes.medium,
            fontWeight: FontWeights.bold
        }
    },
    labelStyles = {
        root: {
            height: FontSizes.medium,
        }
    },
    valueStyles = {
        root: {
            height: FontSizes.small,
            paddingLeft: 6,
            minWidth: 50,
            maxWidth: 200,
            textAlign: "right"
        }
    },
}) => {

    const tooltipId = useId("tooltip");

    const tooltipProps: ITooltipProps = {
        onRenderContent: () => (
            <StackShim style={{ margin: 10, padding: 0 }}>
                {
                    data
                        .filter((n, i) => i >= rowCount)
                        .map((row, rowIdx) => {
                            return <StackItemShim key={rowIdx}>
                                <StackShim horizontal styles={tableRowStyles}>
                                    {
                                        row.map((n, i) => {
                                            return <StackItemShim
                                                key={i}
                                                grow={i === 0 ? 1 : 0}
                                                styles={i === 0 ? labelStyles : valueStyles}
                                            >
                                                {n}
                                            </StackItemShim>
                                                ;
                                        })
                                    }
                                </StackShim>
                            </StackItemShim>
                                ;
                        })
                }
            </StackShim>
        ),
    };
    const labelStackItem = label === "" ? undefined : <StackItemShim
        key="label"
        grow={0}
        styles={headerStyles}
    >
        {label}
    </StackItemShim>
        ;

    const dataStackItem = <StackItemShim key="data" grow={1}>
        {
            data
                .filter((n, i) => i < rowCount)
                .map((row, rowIdx) => {
                    return <StackItemShim key={rowIdx}>
                        <StackShim horizontal styles={tableRowStyles}>
                            {
                                row.map((n, i) => {
                                    return <StackItemShim
                                        key={i}
                                        grow={i === 0 ? 1 : 0}
                                        styles={i === 0 ? labelStyles : valueStyles}
                                    >
                                        {n}
                                    </StackItemShim>
                                        ;
                                })
                            }
                        </StackShim>
                    </StackItemShim>
                        ;
                })
        }
    </StackItemShim>
        ;

    const footerStackItem = data.length <= rowCount ? undefined : <StackItemShim
        key="tooltip-wrapper"
        grow={0}
    >
        <TooltipHost
            id={tooltipId}
            tooltipProps={tooltipProps}
        >
            <StackShim horizontal styles={tableRowStyles}>
                <StackItemShim key="label" grow={1} styles={labelStyles}>
                    {overflowLabel}
                </StackItemShim>
                <StackItemShim key="value" grow={0} styles={valueStyles}>
                    {overflowValue}
                </StackItemShim>
            </StackShim>
        </TooltipHost>
    </StackItemShim>
        ;

    return <StackShim styles={tableStyles}>
        {labelStackItem}
        {dataStackItem}
        {footerStackItem}
    </StackShim>
        ;
};
