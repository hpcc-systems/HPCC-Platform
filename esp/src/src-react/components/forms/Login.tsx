import * as React from "react";
import { Button, Field, Input, makeStyles, MessageBar, MessageBarActions, MessageBarBody } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import { useUserSession } from "../../hooks/user";
import { replaceUrl } from "../../util/history";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/forms/Login.tsx");

const useStyles = makeStyles({
    root: {
        height: "100%",
        backgroundColor: "#1a9bd7",
    },
    container: {
        width: "99%",
        position: "absolute",
        top: "50%",
        transform: "translateY(-50%)",
    },
    formContainer: {
        width: "500px",
        padding: "20px 0",
        borderRadius: "5px",
        backgroundColor: "#fff",
        margin: "auto",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        "& .fui-Input": {
            width: "300px",
        },
        "& .fui-Field": {
            margin: "10px 0 0 0"
        },
        "& .fui-Button": {
            margin: "20px 0 0 0"
        }
    },
});

interface LoginFormValues {
    username: string;
    password: string;
}

const defaultValues: LoginFormValues = {
    username: "",
    password: ""
};

interface LoginProps {

}

export const Login: React.FunctionComponent<LoginProps> = ({

}) => {

    const clearCookie = React.useCallback((name: string) => {
        document.cookie = `${name}=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0; Path=/;`;
    }, []);

    const consumeLoginMessage = React.useCallback((cookies: Record<string, string>) => {
        const loginMessage = cookies.ESPAuthenticationMSG || cookies.ESPUserAcctError || "";
        if (cookies.ESPAuthenticationMSG || cookies.ESPUserAcctError) {
            clearCookie("ESPAuthenticationMSG");
            clearCookie("ESPUserAcctError");
        }
        return loginMessage;
    }, [clearCookie]);

    const { createUserSession } = useUserSession();

    const { handleSubmit, control } = useForm<LoginFormValues>({ defaultValues });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    React.useEffect(() => {
        const cookies = Utility.parseCookies();

        // Check for session-restore redirect before consuming login-message cookies
        // so they are not cleared before the redirect can occur.
        if (cookies["ESPSessionState"] === "true") {
            const lastUrl = window.localStorage.getItem("pageOnLock") ?? "/";
            window.localStorage.removeItem("pageOnLock");
            replaceUrl(lastUrl);
            return;
        }

        const loginMessage = consumeLoginMessage(cookies);
        if (loginMessage) {
            setErrorMessage(loginMessage);
            setShowError(true);
        }
    }, [consumeLoginMessage]);

    const loginStyles = useStyles();

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            async (data, evt) => {
                const formData = new FormData();

                for (const key in data) {
                    formData.append(key, data[key]);
                }

                const loginResponse = await fetch("/esp/login", {
                    method: "POST",
                    body: formData
                });

                const cookies = Utility.parseCookies();

                const isRedirectedToLogin = loginResponse?.url.indexOf("esp/files/Login.html") > -1;

                if (isRedirectedToLogin) {
                    const loginMessage = consumeLoginMessage(cookies);
                    setErrorMessage(loginMessage || nlsHPCC.InvalidUsernamePassword);
                    setShowError(true);
                } else {
                    createUserSession(cookies).then(() => {
                        setShowError(false);
                        setErrorMessage("");
                        const lastUrl = window.localStorage.getItem("pageOnLock") ?? "/";
                        window.localStorage.removeItem("pageOnLock");
                        replaceUrl(lastUrl, true);
                    }).catch(err => logger.error("Unable to create user session."));
                }
            }
        )();
    }, [handleSubmit, createUserSession, consumeLoginMessage]);

    return <div className={loginStyles.root}>
        <div className={loginStyles.container}>
            <div className={loginStyles.formContainer}>
                <form onSubmit={handleSubmit(onSubmit)}>
                    <center>
                        <img id="logo" src="eclwatch/img/hpccsystems.svg" alt="HPCC Systems" style={{ width: "206px" }} />
                    </center>
                    <Controller
                        control={control} name="username"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.UserID} required validationMessage={error?.message}>
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
                        control={control} name="password"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <Field label={nlsHPCC.Password} required validationMessage={error?.message}>
                                <Input
                                    name={fieldName}
                                    type="password"
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
                    <Button type="submit" appearance="primary">{nlsHPCC.Login}</Button>
                </form>
            </div>
        </div>
    </div>;

};
