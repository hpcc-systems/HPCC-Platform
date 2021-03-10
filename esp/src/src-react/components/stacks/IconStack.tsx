import * as React from "react";

import { mergeStyles, IStackStyles, FontIcon, Stack, FontSizes, Label } from "@fluentui/react";
import { ICellObject, StackTable } from "../visualizations/StackTable";

interface IconStackProps {
    label?: string;
    iconName: string;
    data?: [string | number | ICellObject, string | number | ICellObject][];
}

export const IconStack: React.FunctionComponent<IconStackProps> = ({
    label = "",
    iconName,
    data
}) => {
    const iconClass = mergeStyles({
        fontSize: 38,
        height: 38,
        margin: "4px 4px 0 0"
    });

    const labelStyles: IStackStyles = {
        root: {
            fontWeight: "bold",
            height: 20
        },
    };
    const labelStackItem = label === "" ? undefined : <Stack.Item
            key="label"
            grow={0}
            styles={labelStyles}
        >
            <Label>{label}</Label>
        </Stack.Item>
        ;
    return <Stack>
            {labelStackItem}
            <Stack.Item grow={0}>
                <Stack horizontal>
                    <Stack.Item key="icon" grow={0}>
                        <FontIcon
                            iconName={iconName}
                            className={iconClass}
                        />
                    </Stack.Item>
                    <Stack.Item key="data" grow={1}>
                        <StackTable
                            data={data}
                            labelStyles={{
                                root: {
                                    height: FontSizes.medium,
                                    fontSize: FontSizes.small,
                                }
                            }}
                        />
                    </Stack.Item>
                </Stack> 
            </Stack.Item>
        </Stack>
        ;
};
