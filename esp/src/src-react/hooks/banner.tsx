import * as React from "react";
import { Checkbox, ColorPicker, DefaultButton, getColorFromString, IColor, Label, MessageBar, MessageBarType, PrimaryButton, Stack, TextField } from "@fluentui/react";
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

const white = getColorFromString("#ffffff");

export function useBanner({ showForm, setShowForm }: useBannerProps): [React.FunctionComponent, React.FunctionComponent] {

    const [activity] = useActivity();

    const [bannerColor, setBannerColor] = React.useState(activity?.BannerColor || "black");
    const [bannerMessage, setBannerMessage] = React.useState(activity?.BannerContent || "");
    const [bannerSize, setBannerSize] = React.useState(activity?.BannerSize || "16");
    const [showBanner, setShowBanner] = React.useState(activity?.ShowBanner == 1 || false);

    const { handleSubmit, control, reset } = useForm<BannerConfigValues>({ defaultValues });
    const [color, setColor] = React.useState(white);
    const updateColor = React.useCallback((evt: any, colorObj: IColor) => setColor(colorObj), []);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;
                request.BannerColor = color.str;
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
        setColor(getColorFromString(activity?.BannerColor));
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
                <PrimaryButton text={nlsHPCC.OK} onClick={handleSubmit(onSubmit)} />
                <DefaultButton text={nlsHPCC.Cancel} onClick={closeForm} />
            </>}>
            <Stack horizontal horizontalAlign="space-between">
                <Stack.Item grow={2}>
                    <Controller
                        control={control} name="BannerAction"
                        render={({
                            field: { onChange, name: fieldName, value }
                        }) => <Controller
                                control={control} name={fieldName}
                                render={({
                                    field: { onChange, name: fieldName, value }
                                }) => <Checkbox name={fieldName} checked={value == 1} onChange={onChange} label={nlsHPCC.Enable} />}
                            />
                        }
                    />
                    <Controller
                        control={control} name="BannerContent"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <TextField
                                name={fieldName}
                                onChange={onChange}
                                required={true}
                                multiline
                                autoAdjustHeight
                                label={nlsHPCC.BannerMessage}
                                value={value}
                                errorMessage={error && error?.message}
                            />}
                        rules={{
                            required: nlsHPCC.ValidationErrorRequired
                        }}
                    />
                    <Controller
                        control={control} name="BannerSize"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <TextField
                                name={fieldName}
                                onChange={onChange}
                                label={nlsHPCC.BannerSize}
                                value={value}
                                errorMessage={error && error?.message}
                            />}
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
                        }) => <TextField
                                name={fieldName}
                                onChange={onChange}
                                label={nlsHPCC.BannerScroll}
                                value={value}
                                errorMessage={error && error?.message}
                            />}
                        rules={{
                            pattern: {
                                value: /^[0-9]+$/i,
                                message: nlsHPCC.ValidationErrorEnterNumber
                            }
                        }}
                    />
                </Stack.Item>
                <Stack.Item>
                    <Label>{nlsHPCC.BannerColor}</Label>
                    <ColorPicker
                        onChange={updateColor}
                        color={color}
                    />
                </Stack.Item>
            </Stack>
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
                messageBarType={MessageBarType.warning}
                onDismiss={() => setShowBanner(false)}
                dismissButtonAriaLabel="Close"
                isMultiline={false}
                truncated={true}
                overflowButtonAriaLabel="See More"
                style={{
                    color: bannerColor,
                    fontSize: `${bannerSize}px`,
                    lineHeight: `${bannerSize}px`
                }}
            >
                {bannerMessage}
            </MessageBar>
            ;
    }, [bannerColor, bannerMessage, bannerSize, showBanner]);

    return [BannerMessageBar, BannerConfig];

}