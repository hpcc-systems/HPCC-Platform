import * as React from "react";
import { tinycolor } from "@ctrl/tinycolor";
import { Button, Checkbox, ColorArea, ColorPicker, ColorSlider, Field, Input, Label, MessageBar, MessageBarActions, MessageBarBody, Textarea } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { useActivity } from "./activity";
import { MessageBox } from "../layouts/MessageBox";

interface BannerConfigValues {
    BannerAction: number;
    BannerContent: string;
    BannerColor: string;
    BannerSize: string;
    BannerScroll: string;
}

const defaultValues: BannerConfigValues = {
    BannerAction: 0,
    BannerContent: "",
    BannerColor: "",
    BannerSize: "",
    BannerScroll: ""
};

interface useBannerProps {
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export function useBanner({ showForm, setShowForm }: useBannerProps): [React.FunctionComponent, React.FunctionComponent] {

    const { activity } = useActivity();

    const [bannerColor, setBannerColor] = React.useState(activity?.BannerColor || "black");
    const [bannerMessage, setBannerMessage] = React.useState(activity?.BannerContent || "");
    const [bannerSize, setBannerSize] = React.useState(activity?.BannerSize || "16");
    const [showBanner, setShowBanner] = React.useState(activity?.ShowBanner == 1 || false);

    const { handleSubmit, control, reset } = useForm<BannerConfigValues>({ defaultValues });
    const [color, setColor] = React.useState<{ h: number; s: number; v: number; a?: number }>(tinycolor("#ffffff").toHsv());
    const updateColor = React.useCallback((_: unknown, data: { color: { h: number; s: number; v: number; a?: number } }) => setColor(data.color), []);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;
                request.BannerColor = tinycolor(color).toHexString();
                request.BannerAction = request.BannerAction === true ? "1" : "0";
                setBannerColor(request.BannerColor);
                setBannerMessage(request.BannerContent);
                setBannerSize(request.BannerSize);
                setShowBanner(request.BannerAction == 1);
                activity.setBanner(request);
                closeForm();
            },
        )();
    }, [activity, closeForm, color, handleSubmit]);

    React.useEffect(() => {
        if (!activity?.BannerColor) return;
        setColor(tinycolor(activity?.BannerColor).toHsv());
        const values = {
            BannerAction: activity?.ShowBanner || 0,
            BannerContent: activity?.BannerContent || "",
            BannerScroll: activity?.BannerScroll || "",
            BannerSize: activity?.BannerSize || ""
        };
        reset(values);
    }, [activity?.BannerColor, activity?.BannerContent, activity?.BannerScroll, activity?.BannerSize, activity?.ShowBanner, reset]);

    const BannerConfig = React.useMemo(() => () => {
        return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.SetBanner} minWidth={680}
            footer={<>
                <Button appearance="primary" onClick={handleSubmit(onSubmit)}>{nlsHPCC.OK}</Button>
                <Button onClick={closeForm}>{nlsHPCC.Cancel}</Button>
            </>}>
            <div style={{ display: "flex", flexDirection: "row", justifyContent: "space-between" }}>
                <div style={{ flexGrow: 2 }}>
                    <Controller
                        control={control} name="BannerAction"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Controller
                                control={control} name={fieldName}
                                render={({
                                    field: { onChange, name: fieldName, value }
                                }) => <Checkbox name={fieldName} checked={value == 1} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.Enable} />}
                            />
                        }
                    />
                    <Controller
                        control={control} name="BannerContent"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.BannerMessage} validationMessage={error?.message}>
                                <Textarea
                                    name={fieldName}
                                    value={value}
                                    onChange={(_, data) => onChange(data.value)}
                                />
                            </Field>}
                    />
                    <Controller
                        control={control} name="BannerSize"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.BannerSize} validationMessage={error?.message}>
                                <Input
                                    name={fieldName}
                                    value={value}
                                    onChange={(_, data) => onChange(data.value)}
                                />
                            </Field>}
                        rules={{
                            pattern: {
                                value: /^[0-9]+$/i,
                                message: nlsHPCC.ValidationErrorEnterNumber
                            }
                        }}
                    />
                    <Controller
                        control={control} name="BannerScroll"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.BannerScroll} validationMessage={error?.message}>
                                <Input
                                    name={fieldName}
                                    value={value}
                                    onChange={(_, data) => onChange(data.value)}
                                />
                            </Field>}
                        rules={{
                            pattern: {
                                value: /^[0-9]+$/i,
                                message: nlsHPCC.ValidationErrorEnterNumber
                            }
                        }}
                    />
                </div>
                <div>
                    <Label>{nlsHPCC.BannerColor}</Label>
                    <ColorPicker color={color} onColorChange={updateColor}>
                        <ColorArea />
                        <ColorSlider />
                    </ColorPicker>
                </div>
            </div>
        </MessageBox>;
    }, [closeForm, color, control, handleSubmit, onSubmit, showForm, updateColor]);

    React.useEffect(() => {
        setShowBanner(activity?.ShowBanner == 1);
        setBannerMessage(activity?.BannerContent || "");
        setBannerColor(activity?.BannerColor || "black");
        setBannerSize(activity?.BannerSize || "16");
    }, [activity?.BannerContent, activity?.ShowBanner, activity?.BannerColor, activity?.BannerSize]);

    const BannerMessageBar = React.useMemo(() => () => {
        return showBanner &&
            <MessageBar
                intent="warning"
                style={{
                    color: bannerColor,
                    fontSize: `${bannerSize}px`,
                    lineHeight: `${bannerSize}px`
                }}
            >
                <MessageBarBody>{bannerMessage}</MessageBarBody>
                <MessageBarActions containerAction={<Button onClick={() => setShowBanner(false)} aria-label="Close" appearance="transparent" icon={<DismissRegular />} />} />
            </MessageBar>
            ;
    }, [bannerColor, bannerMessage, bannerSize, showBanner]);

    return [BannerMessageBar, BannerConfig];

}