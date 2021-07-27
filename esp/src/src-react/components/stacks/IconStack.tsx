import * as React from "react";

import { mergeStyles, IStackStyles, FontIcon, Stack, FontSizes, Label, DefaultPalette } from "@fluentui/react";
import { StackTable } from "../visualizations/StackTable";

interface IconStackProps {
    label?: string;
    iconName: string;
    data?: [string | number | any, string | number | any][];
    labelStyles?: IStackStyles;
    valueStyles?: IStackStyles;
}

export const IconStack: React.FunctionComponent<IconStackProps> = ({
    label = "",
    iconName,
    data,
    labelStyles = {
        root: {
            height: FontSizes.medium,
            fontSize: FontSizes.small,
        }
    },
    valueStyles = {
        root: {
            height: FontSizes.medium,
            fontSize: FontSizes.small,
        }
    }
}) => {
    const iconStyles = mergeStyles({
        fontSize: FontSizes.large,
        height: 38,
        margin: "4px 4px 0 0",
        color: DefaultPalette.themePrimary
    });

    const titleStyles: IStackStyles = {
        root: {
            fontWeight: "bold",
            fontSize: FontSizes.xLargePlus,
            height: 20,
            marginBottom: 12
        },
    };
    const titleStackItem = label === "" ? undefined : <Stack.Item
            key="label"
            grow={0}
            styles={titleStyles}
        >
            <Label styles={
                {
                    root: {
                        fontSize: FontSizes.large
                    }
                }
            }>{label}</Label>
        </Stack.Item>
        ;
    return <Stack>
            {titleStackItem}
            <Stack.Item grow={0}>
                <Stack horizontal>
                    <Stack.Item key="icon" grow={0}>
                        <FontIcon
                            iconName={iconName}
                            className={iconStyles}
                        />
                    </Stack.Item>
                    <Stack.Item key="data" grow={1}>
                        <StackTable
                            data={data}
                            labelStyles={labelStyles}
                            valueStyles={valueStyles}
                        />
                    </Stack.Item>
                </Stack> 
            </Stack.Item>
        </Stack>
        ;
};