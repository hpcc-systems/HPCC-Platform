import * as React from "react";
import { IconButton, IIconProps, Stack, mergeStyleSets } from "@fluentui/react";
import { useUserTheme } from "../hooks/theme";
import { updateFullscreen } from "../util/history";

const FullscreenIcon: IIconProps = { iconName: "FullScreen" };
const RestoreIcon: IIconProps = { iconName: "ChromeRestore" };

export interface FullscreenProps {
    fullscreen: boolean;
}

export const FullscreenFrame: React.FunctionComponent<FullscreenProps> = ({
    fullscreen,
    children
}) => {
    const { themeV9 } = useUserTheme();
    const layoutStyles = React.useMemo(() => mergeStyleSets({
        fullscreen: {
            position: "fixed",
            top: "0",
            left: "0",
            width: "100%",
            height: "100%",
            background: themeV9.colorNeutralBackground1,
        },
        normal: {
            height: "100%"
        }
    }), [themeV9.colorNeutralBackground1]);

    return <div className={fullscreen ? layoutStyles.fullscreen : layoutStyles.normal}>
        {children}
    </div>;
};

export const FullscreenStack: React.FunctionComponent<FullscreenProps> = ({
    fullscreen,
    children
}) => {

    return <Stack horizontal>
        <Stack.Item grow>
            {children}
        </Stack.Item>
        <Stack.Item align="center">
            <IconButton iconProps={fullscreen ? RestoreIcon : FullscreenIcon} onClick={() => updateFullscreen(!fullscreen)} />
        </Stack.Item>
    </Stack>;
};
