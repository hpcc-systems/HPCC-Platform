import * as React from "react";
import { Checkbox, ColorPicker, DefaultButton, getColorFromString, IColor, Label, PrimaryButton, TextField, TooltipHost } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import { MessageBox } from "../../layouts/MessageBox";
import { useGlobalStore } from "../../hooks/store";

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

const white = getColorFromString("#ffffff");

export const TitlebarConfig: React.FunctionComponent<TitlebarConfigProps> = ({
    toolbarThemeDefaults,
    showForm,
    setShowForm
}) => {
    const { handleSubmit, control, reset } = useForm<TitlebarConfigValues>({ defaultValues });
    const [color, setColor] = React.useState(white);
    const updateColor = React.useCallback((evt: any, colorObj: IColor) => setColor(colorObj), []);
    const [showEnvironmentTitle, setShowEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", toolbarThemeDefaults.active, true);
    const [environmentTitle, setEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", toolbarThemeDefaults.text, true);
    const [titlebarColor, setTitlebarColor] = useGlobalStore("HPCCPlatformWidget_Toolbar_Color", toolbarThemeDefaults.color, true);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;
                request.titlebarColor = color.str;

                setShowEnvironmentTitle(request?.showEnvironmentTitle);
                setEnvironmentTitle(request?.environmentTitle);
                setTitlebarColor(request.titlebarColor);

                closeForm();
            },
        )();
    }, [closeForm, color, handleSubmit, setEnvironmentTitle, setShowEnvironmentTitle, setTitlebarColor]);

    const [, , resetShowEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", toolbarThemeDefaults.active, true);
    const [, , resetEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", toolbarThemeDefaults.text, true);
    const [, , resetTitlebarColor] = useGlobalStore("HPCCPlatformWidget_Toolbar_Color", toolbarThemeDefaults.color, true);

    const onReset = React.useCallback(() => {
        resetShowEnvironmentTitle();
        resetEnvironmentTitle();
        resetTitlebarColor();
    }, [resetEnvironmentTitle, resetShowEnvironmentTitle, resetTitlebarColor]);

    React.useEffect(() => {
        setColor(getColorFromString(titlebarColor));
        const values = {
            showEnvironmentTitle: showEnvironmentTitle,
            environmentTitle: environmentTitle || ""
        };
        reset(values);
    }, [environmentTitle, reset, showEnvironmentTitle, titlebarColor]);

    return <MessageBox show={showForm} setShow={closeForm} blocking={true} modeless={false} title={nlsHPCC.SetToolbarColor} minWidth={400}
        footer={<>
            <PrimaryButton text={nlsHPCC.OK} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => { reset(defaultValues); closeForm(); }} />
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
            <Label aria-describedby="bannerColorTooltip">{nlsHPCC.BannerColor}</Label>
            <ColorPicker
                onChange={updateColor}
                color={color}
            />
        </TooltipHost>
    </MessageBox>;

};