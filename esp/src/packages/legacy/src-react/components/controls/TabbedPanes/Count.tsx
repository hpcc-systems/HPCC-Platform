import * as React from "react";

interface CountProps {
    value: string | number;
}

export const Count: React.FunctionComponent<CountProps> = ({
    value
}) => {

    if (!value) return <></>;
    return <span> ({value})</span>;
};
