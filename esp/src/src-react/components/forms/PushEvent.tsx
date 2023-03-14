import * as React from "react";
import { DefaultButton, MessageBar, MessageBarType, PrimaryButton, TextField, } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as WsWorkunits from "src/WsWorkunits";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/PushEvent.tsx");

interface PushEventValues {
    EventName: string;
    EventText: string;
}

const defaultValues: PushEventValues = {
    EventName: "",
    EventText: ""
};

interface PushEventProps {
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const PushEventForm: React.FunctionComponent<PushEventProps> = ({
    showForm,
    setShowForm
}) => {

    const { handleSubmit, control, reset } = useForm<PushEventValues>({ defaultValues });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;

                WsWorkunits.WUPushEvent({ request: request })
                    .then(({ WUPushEventResponse }) => {
                        if (WUPushEventResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setErrorMessage(WUPushEventResponse?.retmsg);
                            logger.error(WUPushEventResponse?.retmsg);
                        } else {
                            closeForm();
                            reset(defaultValues);
                        }
                    })
                    .catch(err => logger.error(err))
                    ;
            }
        )();
    }, [closeForm, handleSubmit, reset]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.PushEvent} minWidth={400}
        footer={<>
            <PrimaryButton text={nlsHPCC.Apply} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => { reset(defaultValues); closeForm(); }} />
        </>}>
        <Controller
            control={control} name="EventName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    required={true}
                    label={nlsHPCC.EventName}
                    value={value}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="EventText"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    required={true}
                    label={nlsHPCC.EventText}
                    value={value}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        {showError &&
            <div style={{ marginTop: 16 }}>
                <MessageBar
                    messageBarType={MessageBarType.error} isMultiline={true}
                    onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                    {errorMessage}
                </MessageBar>
            </div>
        }
    </MessageBox>;
};
