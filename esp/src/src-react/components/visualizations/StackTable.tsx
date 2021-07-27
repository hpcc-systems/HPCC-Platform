import * as React from "react";

import { FontSizes, FontWeights, IStackItemStyles, IStackStyles, ITooltipProps, Stack, TooltipHost } from "@fluentui/react";
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
            marginTop: 6,
            textOverflow: "nowrap",
            overflow: "hidden",
            lineHeight: FontSizes.small,
            fontSize: FontSizes.small
        }
    },
    headerStyles = {
        root: {
            height: FontSizes.medium,
            lineHeight: FontSizes.small,
            fontSize: FontSizes.small,
            fontWeight: FontWeights.bold 
        }
    },
    labelStyles = {
        root: {
            height: FontSizes.medium,
            lineHeight: FontSizes.small,
            fontSize: FontSizes.small,
        }
    },
    valueStyles = {
        root: {
            height:  FontSizes.small,
            paddingLeft: 6,
            minWidth: 50,
            maxWidth: 200,
            textAlign: "right",
            lineHeight: FontSizes.small,
            fontSize: FontSizes.small
        }
    },
}) => {

    const tooltipId = useId("tooltip");

    const tooltipProps: ITooltipProps = {
        onRenderContent: () => (
            <Stack style={{ margin: 10, padding: 0 }}>
                {
                    data
                        .filter((n, i) => i >= rowCount)
                        .map((row, rowIdx) => {
                            return <Stack.Item key={rowIdx}>
                                    <Stack horizontal styles={tableRowStyles} grow={rowIdx ? 1 : 0}>
                                    {
                                        row.map((n: any, i) => {
                                            return rowStackItem(n, i);
                                        })
                                    }
                                </Stack>
                            </Stack.Item>
                            ;
                        })
                }
            </Stack>
        ),
    };
    const labelStackItem = label === "" ? undefined : <Stack.Item
            key="label"
            grow={0}
            styles={headerStyles}
        >
            {label}
        </Stack.Item>
        ;

    const dataStackItem = <Stack.Item key="data" grow={1}>
            {
                data
                    .filter((n, i) => i < rowCount)
                    .map((row, rowIdx) => {
                        return <Stack.Item key={rowIdx}>
                        <Stack horizontal styles={tableRowStyles}>
                            {
                                row.map((n, i) => {
                                    return rowStackItem(n, i);
                                })
                            }
                        </Stack>
                        </Stack.Item>
                        ;
                    })
            }
        </Stack.Item>
        ;
    const footerStackItem = data.length <= rowCount ? undefined : <Stack.Item 
            key="tooltip-wrapper"
            grow={0}
        >
            <TooltipHost
                id={tooltipId}
                tooltipProps={tooltipProps}
            >
                <Stack horizontal styles={tableRowStyles}>
                    <Stack.Item key="label" grow={1} styles={labelStyles}>
                        {overflowLabel}
                    </Stack.Item>
                    <Stack.Item key="value" grow={0} styles={valueStyles}>
                        {overflowValue}
                    </Stack.Item>
                </Stack>
            </TooltipHost>
        </Stack.Item>
        ;
    
    return <Stack styles={tableStyles}>
            {labelStackItem}
            {dataStackItem}
            {footerStackItem}
        </Stack>
        ;

    function rowStackItem(n, i) {
        if(n.styles || n.text) {
            const styles = n.styles ?? (i === 0 ? labelStyles : valueStyles);
            return <Stack.Item
                key={i}
                grow={i === 0 ? 1 : 0}
                styles={styles}
            >
                {n.text}
            </Stack.Item>
                ;
        } else {
            return <Stack.Item
                key={i}
                grow={i === 0 ? 1 : 0}
                styles={i === 0 ? labelStyles : valueStyles}
            >
                {n}
            </Stack.Item>
                ;
        }
    }
};