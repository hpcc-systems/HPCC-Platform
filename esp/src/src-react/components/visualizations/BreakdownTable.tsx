import * as React from "react";

import { StackTable } from "./StackTable";

interface VizProps {
    label?: string;
    data: [string | number, number][];
    rowCount?: number;
    usePercentage?: boolean;
}

export const BreakdownTable: React.FunctionComponent<VizProps> = ({
    label,
    data,
    rowCount = 9,
    usePercentage
}) => {
    
    data.sort((a, b) => {
        return b[1] - a[1];
    });
    let overflowValue = 0;
    let total = 0;
    if (usePercentage) {
        data.forEach((row, i) => {
            total += row[1];
            if (i > rowCount - 1) {
                overflowValue += row[1];
            }
        });
    }
    
    let overflowValueText = usePercentage ? Math.floor(overflowValue / total * 100) + "%" : overflowValue + "";
    if (overflowValueText === "0%") {
        overflowValueText = "~0%";
    }

    return <StackTable
        label={label}
        data={data}
        rowCount={rowCount}
        overflowLabel="Other..."
        overflowValue={overflowValueText}
    />;
};
