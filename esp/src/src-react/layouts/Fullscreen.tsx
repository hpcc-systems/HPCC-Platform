import * as React from "react";
import { IconButton, IIconProps, mergeStyleSets } from "@fluentui/react";
import { StackShim, StackItemShim } from "@fluentui/react-migration-v8-v9";
import { useUserTheme } from "../hooks/theme";
import { updateFullscreen } from "../util/history";

const FullscreenIcon: IIconProps = { iconName: "FullScreen" };
const RestoreIcon: IIconProps = { iconName: "ChromeRestore" };

export interface FullscreenProps {
    fullscreen: boolean;
    children?: React.ReactNode;
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

    return <StackShim horizontal>
        <StackItemShim grow>
            {children}
        </StackItemShim>
        <StackItemShim align="center">
            <IconButton iconProps={fullscreen ? RestoreIcon : FullscreenIcon} onClick={() => updateFullscreen(!fullscreen)} />
        </StackItemShim>
    </StackShim>;
};
