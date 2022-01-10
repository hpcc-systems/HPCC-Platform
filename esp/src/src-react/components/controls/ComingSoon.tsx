import * as React from "react";
import { IStyle, Toggle } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useUserStore } from "../../hooks/store";

interface ComingSoon {
    defaultValue: boolean;
    style?: IStyle;
}

export const ComingSoon: React.FunctionComponent<ComingSoon> = ({
    defaultValue = false,
    style
}) => {
    const [modernMode, setModernMode] = useUserStore("ModernMode", String(defaultValue));

    const onChangeCallback = React.useCallback((ev: React.MouseEvent<HTMLElement>, checked: boolean) => {
        setModernMode(checked ? String(true) : String(false));
        window.location.replace(checked ? "/esp/files/index.html" : "/esp/files/stub.htm");
    }, [setModernMode]);

    return <Toggle label={nlsHPCC.TechPreview} checked={(modernMode ?? String(defaultValue)) !== String(false)} onText={nlsHPCC.On} offText={nlsHPCC.Off} onChange={onChangeCallback} styles={{ label: style }} />;
};
