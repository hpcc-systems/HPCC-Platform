import * as React from "react";
import { getBuildInfo, BuildInfo } from "src/Session";

declare const dojoConfig;

export function useBuildInfo(): [BuildInfo, { isContainer: boolean, currencyCode: string }] {

    const [buildInfo, setBuildInfo] = React.useState<BuildInfo>({});
    const [isContainer, setIsContainer] = React.useState<boolean>(dojoConfig.isContainer);
    const [currencyCode, setCurrencyCode] = React.useState<string>(dojoConfig.currencyCode);

    React.useEffect(() => {
        getBuildInfo().then(info => {
            setIsContainer(info["CONTAINERIZED"] === "ON");
            setCurrencyCode(info["currencyCode"] ?? "");
            setBuildInfo(info);
        });
    }, []);

    return [buildInfo, { isContainer, currencyCode }];
}
