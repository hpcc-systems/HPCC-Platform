import { useUserTheme } from "../hooks/theme";

export const pivotItemStyle = (size, padding: number = 4) => {
    if (isNaN(size.width)) {
        return { position: "absolute", padding: `${padding}px`, overflow: "auto", zIndex: 0 } as React.CSSProperties;
    }
    return { position: "absolute", padding: `${padding}px`, overflow: "auto", zIndex: 0, width: size.width - padding * 2, height: size.height - 45 - padding * 2 } as React.CSSProperties;
};

interface usePivotItemDisableResponse {
    disabled?: boolean;
    style?: {
        background?: string;
        color?: string;
    };
}

export function usePivotItemDisable(disable: boolean): usePivotItemDisableResponse {
    const { themeV9 } = useUserTheme();

    return disable ? {
        disabled: true,
        style: {
            background: themeV9.colorNeutralBackgroundDisabled,
            color: themeV9.colorNeutralForegroundDisabled
        }
    } : {
    };
}
