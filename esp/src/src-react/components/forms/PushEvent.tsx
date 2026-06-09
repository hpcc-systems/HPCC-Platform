import * as React from "react";
import { Button, Field, Input, MessageBar, MessageBarActions, MessageBarBody, Spinner } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
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
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);
                const request: any = data;

                WsWorkunits.WUPushEvent({ request: request })
                    .then(({ WUPushEventResponse }) => {
                        if (WUPushEventResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            setErrorMessage(WUPushEventResponse?.retmsg);
                            logger.error(WUPushEventResponse?.retmsg);
                        } else {
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
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
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Apply}</Button>
            <Button onClick={() => { reset(defaultValues); closeForm(); }}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="EventName"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.EventName} required validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="EventText"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.EventText} required validationMessage={error?.message}>
                    <Input
                        name={fieldName}
                        value={value}
                        onChange={(_, data) => onChange(data.value)}
                    />
                </Field>}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        {showError &&
            <div style={{ marginTop: 16 }}>
                <MessageBar intent="error">
                    <MessageBarBody>{errorMessage}</MessageBarBody>
                    <MessageBarActions containerAction={<Button onClick={() => setShowError(false)} aria-label="Close" appearance="transparent" icon={<DismissRegular />} />} />
                </MessageBar>
            </div>
        }
    </MessageBox>;
};
