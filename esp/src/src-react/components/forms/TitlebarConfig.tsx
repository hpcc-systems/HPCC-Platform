import * as React from "react";
import { Checkbox, DefaultButton, getColorFromString, IColor, PrimaryButton, TextField, TooltipHost } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import { ThemeEditorColorPicker } from "../ThemeEditor";
import { MessageBox } from "../../layouts/MessageBox";
import { useGlobalStore } from "../../hooks/store";
import { useToolbarTheme } from "../../hooks/theme";

import nlsHPCC from "src/nlsHPCC";

interface TitlebarConfigValues {
    showEnvironmentTitle: boolean;
    environmentTitle: string;
    titlebarColor: string;
}

const defaultValues: TitlebarConfigValues = {
    showEnvironmentTitle: false,
    environmentTitle: "",
    titlebarColor: ""
};

interface TitlebarConfigProps {
    toolbarThemeDefaults: Readonly<{ active: boolean, text: string, color: string }>;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const TitlebarConfig: React.FunctionComponent<TitlebarConfigProps> = ({
    toolbarThemeDefaults,
    showForm,
    setShowForm
}) => {
    const { handleSubmit, control, reset } = useForm<TitlebarConfigValues>({ defaultValues });
    const { primaryColor, setPrimaryColor } = useToolbarTheme();
    const [previousColor, setPreviousColor] = React.useState(primaryColor);
    const [showEnvironmentTitle, setShowEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", toolbarThemeDefaults.active, true);
    const [environmentTitle, setEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", toolbarThemeDefaults.text, true);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    React.useEffect(() => {
        // cache the previous color at dialog open, used to reset upon close
        if (showForm) setPreviousColor(primaryColor);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [showForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;
                request.titlebarColor = primaryColor;

                setShowEnvironmentTitle(request?.showEnvironmentTitle);
                setEnvironmentTitle(request?.environmentTitle);
                setPrimaryColor(request.titlebarColor);

                closeForm();
            },
        )();
    }, [closeForm, primaryColor, handleSubmit, setEnvironmentTitle, setShowEnvironmentTitle, setPrimaryColor]);

    const [, , resetShowEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", toolbarThemeDefaults.active, true);
    const [, , resetEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", toolbarThemeDefaults.text, true);
    const [, , resetTitlebarColor] = useGlobalStore("HPCCPlatformWidget_Toolbar_Color", toolbarThemeDefaults.color, true);

    const onReset = React.useCallback(() => {
        resetShowEnvironmentTitle();
        resetEnvironmentTitle();
        resetTitlebarColor();
    }, [resetEnvironmentTitle, resetShowEnvironmentTitle, resetTitlebarColor]);

    React.useEffect(() => {
        const values = {
            showEnvironmentTitle: showEnvironmentTitle,
            environmentTitle: environmentTitle || ""
        };
        reset(values);
    }, [environmentTitle, reset, showEnvironmentTitle]);

    return <MessageBox show={showForm} setShow={closeForm} blocking={true} modeless={false} title={nlsHPCC.SetToolbarColor} minWidth={400}
        footer={<>
            <PrimaryButton text={nlsHPCC.OK} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => { setPrimaryColor(previousColor); reset(defaultValues); closeForm(); }} />
            <DefaultButton text={nlsHPCC.Reset} onClick={() => { onReset(); }} />
        </>}>
        <Controller
            control={control} name="showEnvironmentTitle"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Controller
                    control={control} name={fieldName}
                    render={({
                        field: { onChange, name: fieldName, value }
                    }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.EnableBannerText} />}
                />
            }
        />
        <TooltipHost content={nlsHPCC.BannerMessageTooltip} id="bannerMessageTooltip">
            <Controller
                control={control} name="environmentTitle"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        aria-describedby="bannerMessageTooltip"
                        label={nlsHPCC.NameOfEnvironment}
                        value={value}
                    />}
            />
        </TooltipHost>
        <TooltipHost content={nlsHPCC.BannerColorTooltip} id="bannerColorTooltip">
            <div style={{ marginTop: 6 }}>
                <ThemeEditorColorPicker
                    color={getColorFromString(primaryColor)}
                    onColorChange={(newColor: IColor | undefined) => {
                        setPrimaryColor(newColor.str);
                    }}
                    label={nlsHPCC.ToolbarColor}
                />
            </div>
        </TooltipHost>
    </MessageBox>;

};