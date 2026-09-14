import * as React from "react";
import { Button, Checkbox, ColorArea, ColorPicker, ColorSlider, Field, Input, Label, Spinner, Tooltip } from "@fluentui/react-components";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";
import { useGlobalStore } from "../../hooks/store";
import { cssColorToHSV, hsvToHex, HSVColor } from "../../util/color";


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
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const TitlebarConfig: React.FunctionComponent<TitlebarConfigProps> = ({
    showForm,
    setShowForm
}) => {
    const { handleSubmit, control, reset } = useForm<TitlebarConfigValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);
    const [color, setColor] = React.useState<HSVColor>(cssColorToHSV("#ffffff"));
    const updateColor = React.useCallback((_: unknown, data: { color: HSVColor }) => setColor(data.color), []);
    const [showEnvironmentTitle, setShowEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", false, true);
    const [environmentTitle, setEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", undefined, true);
    const [titlebarColor, setTitlebarColor] = useGlobalStore("HPCCPlatformWidget_Toolbar_Color", undefined, true);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);
                const request: any = data;
                request.titlebarColor = hsvToHex(color);

                setShowEnvironmentTitle(request?.showEnvironmentTitle);
                setEnvironmentTitle(request?.environmentTitle);
                setTitlebarColor(request.titlebarColor);

                setSubmitDisabled(false);
                setSpinnerHidden(true);
                closeForm();
            },
        )();
    }, [closeForm, color, handleSubmit, setEnvironmentTitle, setShowEnvironmentTitle, setTitlebarColor]);

    const [, , resetShowEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", false, true);
    const [, , resetEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", undefined, true);
    const [, , resetTitlebarColor] = useGlobalStore("HPCCPlatformWidget_Toolbar_Color", undefined, true);

    const onReset = React.useCallback(() => {
        resetShowEnvironmentTitle();
        resetEnvironmentTitle();
        resetTitlebarColor();
    }, [resetEnvironmentTitle, resetShowEnvironmentTitle, resetTitlebarColor]);

    React.useEffect(() => {
        setColor(cssColorToHSV(titlebarColor ?? "#b6dff3"));
        const values = {
            showEnvironmentTitle: showEnvironmentTitle,
            environmentTitle: environmentTitle ?? "ECL Watch"
        };
        reset(values);
    }, [environmentTitle, reset, showEnvironmentTitle, titlebarColor]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.SetToolbarColor} minWidth={400}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.OK}</Button>
            <Button onClick={() => { reset(defaultValues); closeForm(); }}>{nlsHPCC.Cancel}</Button>
            <Button onClick={() => { onReset(); }}>{nlsHPCC.Reset}</Button>
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
                    }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.EnableBannerText} />}
                />
            }
        />
        <Tooltip content={nlsHPCC.BannerMessageTooltip} relationship="label">
            <Controller
                control={control} name="environmentTitle"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field label={nlsHPCC.NameOfEnvironment}>
                        <Input
                            name={fieldName}
                            value={value}
                            aria-describedby="bannerMessageTooltip"
                            onChange={(_, data) => onChange(data.value)}
                        />
                    </Field>}
            />
        </Tooltip>
        <Tooltip content={nlsHPCC.BannerColorTooltip} relationship="label">
            <div>
                <Label aria-describedby="bannerColorTooltip">{nlsHPCC.BannerColor}</Label>
                <ColorPicker color={color} onColorChange={updateColor}>
                    <ColorArea />
                    <ColorSlider />
                </ColorPicker>
            </div>
        </Tooltip>
    </MessageBox>;

};