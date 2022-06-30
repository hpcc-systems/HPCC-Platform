import * as React from "react";
import nlsHPCC from "src/nlsHPCC";

interface IFrameProps {
    src: string;
    height?: string;
}

export const IFrame: React.FunctionComponent<IFrameProps> = ({
    src,
    height = "100%"
}) => {

    const [loading, setLoading] = React.useState(true);
    const [loadingMsg, setLoadingMsg] = React.useState(nlsHPCC.Loading);

    const onLoad = React.useCallback(() => {
        setLoading(false);
    }, []);

    const onError = React.useCallback(() => {
        setLoadingMsg(nlsHPCC.IFrameErrorMsg);
    }, []);

    return <>
        <span style={{ padding: 10, display: loading ? "block" : "none" }}>{loadingMsg}</span>
        <iframe src={src} width="100%" height={height} style={{ border: "none" }} onLoad={onLoad} onError={onError}></iframe>
    </>;
};
