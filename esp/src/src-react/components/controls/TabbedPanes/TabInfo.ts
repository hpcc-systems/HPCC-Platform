import * as React from "react";

export interface TabInfo {
    id: string;
    icon?: React.ReactElement;
    label: string;
    count?: string | number;
    disabled?: boolean;
    tooltipText?: string;
    __state?: any;
}
