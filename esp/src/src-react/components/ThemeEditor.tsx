import * as React from "react";
import { Callout, ColorPicker, getColorFromString, IColor, Label, mergeStyles, Stack, TextField } from "@fluentui/react";
import { useUserTheme } from "../hooks/theme";
import nlsHPCC from "src/nlsHPCC";

const colorBoxClassName = mergeStyles({
    width: 20,
    height: 20,
    display: "inline-block",
    position: "absolute",
    left: 5,
    top: 5,
    border: "1px solid black",
    flexShrink: 0,
});

const textBoxClassName = mergeStyles({
    width: 100,
});

const colorPanelClassName = mergeStyles({
    position: "relative",
});

interface ThemeEditorColorPickerProps {
    color: IColor;
    onColorChange: (color: IColor | undefined) => void;
    label: string;
}

export const ThemeEditorColorPicker: React.FunctionComponent<ThemeEditorColorPickerProps> = ({
    color,
    onColorChange,
    label
}) => {
    const [pickerColor, setPickerColor] = React.useState(color);
    const _colorPickerRef = React.useRef(null);
    const [editingColorStr, setEditingColorStr] = React.useState(pickerColor?.str ?? "");
    const [isColorPickerVisible, setIsColorPickerVisible] = React.useState<boolean>(false);

    const _onTextFieldValueChange = React.useCallback((ev: any, newValue: string | undefined) => {
        const newColor = getColorFromString(newValue);
        if (newColor) {
            onColorChange(newColor);
            setEditingColorStr(newColor.str);
        } else {
            setEditingColorStr(newValue);
        }
    }, [onColorChange, setEditingColorStr]);

    React.useEffect(() => {
        setPickerColor(color);
    }, [color]);

    React.useEffect(() => {
        setEditingColorStr(pickerColor?.str);
    }, [pickerColor]);

    return <div>
        <Stack horizontal horizontalAlign={"space-between"} tokens={{ childrenGap: 20 }}>
            <Label>{label}</Label>
            <Stack horizontal className={colorPanelClassName} tokens={{ childrenGap: 35 }}>
                <div
                    ref={_colorPickerRef}
                    id="colorbox"
                    className={colorBoxClassName}
                    style={{ backgroundColor: color?.str }}
                    onClick={() => setIsColorPickerVisible(true)}
                />
                <TextField
                    id="textfield"
                    className={textBoxClassName}
                    value={editingColorStr}
                    onChange={_onTextFieldValueChange}
                />
            </Stack>
        </Stack>
        {isColorPickerVisible && (
            <Callout
                gapSpace={10}
                target={_colorPickerRef.current}
                setInitialFocus={true}
                onDismiss={() => setIsColorPickerVisible(false)}
            >
                <ColorPicker color={pickerColor} onChange={(evt, color) => onColorChange(color)} alphaType={"none"} />
            </Callout>
        )}
    </div>;
};

interface ThemeEditorProps {
}

export const ThemeEditor: React.FunctionComponent<ThemeEditorProps> = () => {

    const { primaryColor, setPrimaryColor, textColor, setTextColor, backgroundColor, setBackgroundColor } = useUserTheme();

    return <div style={{ minHeight: "208px", paddingTop: "32px" }}>
        <ThemeEditorColorPicker
            color={getColorFromString(primaryColor)}
            onColorChange={(newColor: IColor | undefined) => {
                setPrimaryColor(newColor.str);
            }}
            label={nlsHPCC.PrimaryColor}
        />
        <ThemeEditorColorPicker
            color={getColorFromString(textColor)}
            onColorChange={(newColor: IColor | undefined) => {
                setTextColor(newColor.str);
            }}
            label={nlsHPCC.TextColor}
        />
        <ThemeEditorColorPicker
            color={getColorFromString(backgroundColor)}
            onColorChange={(newColor: IColor | undefined) => {
                setBackgroundColor(newColor.str);
            }}
            label={nlsHPCC.BackgroundColor}
        />
    </div>;

};