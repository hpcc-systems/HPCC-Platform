import * as React from "react";

interface IFrameProps {
    src: string;
}

export const IFrame: React.FunctionComponent<IFrameProps> = ({
    src
}) => {

    return <iframe src={src} width="100%" height="100%" style={{ border: "none" }} ></iframe>;
};
