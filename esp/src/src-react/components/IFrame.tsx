import * as React from "react";

interface IFrameProps {
    src: string;
    height?: string;
}

export const IFrame: React.FunctionComponent<IFrameProps> = ({
    src,
    height = "100%"
}) => {

    return <iframe src={src} width="100%" height={height} style={{ border: "none" }} ></iframe>;
};
