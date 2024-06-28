import * as React from "react";
import { Callout, ColorPicker, getColorFromString, IColor, Label, mergeStyles, mergeStyleSets, Stack, TextField } from "@fluentui/react";
import { Slider, tokens } from "@fluentui/react-components";
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

    const { primaryColor, setPrimaryColor, hueTorsion, setHueTorsion, vibrancy, setVibrancy } = useUserTheme();

    const handleHueTorsionChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const hueTorsion = parseInt(e.target.value || "0", 10);
        setHueTorsion(hueTorsion);
    };

    const handleVibrancyChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const vibrancy = parseInt(e.target.value || "0", 10);
        setVibrancy(vibrancy);
    };

    const sliderStyles = mergeStyleSets({
        root: {
            backgroundColor: tokens.colorNeutralBackground3,
        },
        wrapper: {
            display: "flex",
            justifyContent: "space-between",
            marginTop: 6
        },
    });

    return <div style={{ minHeight: "208px", paddingTop: "32px" }}>
        <ThemeEditorColorPicker
            color={getColorFromString(primaryColor)}
            onColorChange={(newColor: IColor | undefined) => {
                setPrimaryColor(newColor.str);
            }}
            label={nlsHPCC.Theme_PrimaryColor}
        />

        <div className={sliderStyles.wrapper}>
            <Label style={{ width: 80 }}>{nlsHPCC.Theme_HueTorsion}</Label>
            <Slider size="small" min={-50} max={50} value={hueTorsion} onChange={handleHueTorsionChange} />
            <TextField type="number" min={-50} max={50} value={hueTorsion?.toString() ?? ""} onChange={handleHueTorsionChange} />
        </div>
        <div className={sliderStyles.wrapper}>
            <Label style={{ width: 80 }}>{nlsHPCC.Theme_Vibrancy}</Label>
            <Slider size="small" min={-50} max={50} value={vibrancy} onChange={handleVibrancyChange} />
            <TextField type="number" min={-50} max={50} value={vibrancy?.toString() ?? ""} onChange={handleVibrancyChange} />
        </div>
    </div>;

};