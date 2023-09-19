import * as React from "react";
import { Image, mergeStyleSets, MessageBar, MessageBarType, TextField } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import { useUserSession } from "../../hooks/user";
import { useUserTheme } from "../../hooks/theme";
import { replaceUrl } from "../../util/history";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/forms/Login.tsx");

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

    const { theme } = useUserTheme();

    const { createUserSession } = useUserSession();

    const { handleSubmit, control } = useForm<LoginFormValues>({ defaultValues });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const loginStyles = React.useMemo(() => mergeStyleSets({
        root: {
            height: "100%",
            backgroundColor: "#1A9BD7"
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
            backgroundColor: theme.palette.white,
            margin: "auto",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            "selectors": {
                "p": {
                    fontSize: "15px"
                },
                ".ms-TextField": {
                    width: "300px"
                }
            }
        },
        button: {
            fontFamily: "'Segoe UI', 'Segoe UI Web (West European)', 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, 'Helvetica Neue', sans-serif",
            fontSize: "16px",
            fontWeight: "600",
            border: `2px solid ${theme.palette.themePrimary}`,
            cursor: "pointer",
            padding: "12px 36px",
            margin: "20px 0 0 0",
            borderRadius: "2px",
            color: theme.palette.white,
            background: theme.palette.themePrimary,
            "selectors": {
                ":hover": {
                    backgroundColor: theme.palette.themePrimary,
                    border: `2px solid ${theme.palette.themePrimary}`,
                    color: theme.palette.white,
                }
            }
        }
    }), [theme]);

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

                if (cookies.ESPAuthenticationMSG && loginResponse?.url.indexOf("esp/files/Login.html") > -1) {
                    setErrorMessage(cookies.ESPAuthenticationMSG);
                    setShowError(true);
                } else {
                    createUserSession(cookies).then(() => {
                        setErrorMessage("");
                        replaceUrl("/", true);
                    }).catch(err => logger.error("Unable to create user session."));
                }
            }
        )();
    }, [handleSubmit, createUserSession]);

    return <div className={loginStyles.root}>
        <div className={loginStyles.container}>
            <div className={loginStyles.formContainer}>
                <form onSubmit={handleSubmit(onSubmit)}>
                    <Image src={Utility.getImageURL("Loginlogo.png")} />
                    <p>{nlsHPCC.PleaseLogIntoECLWatch}</p>
                    <Controller
                        control={control} name="username"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <TextField
                                name={fieldName}
                                onChange={onChange}
                                required={true}
                                label={nlsHPCC.UserID}
                                value={value}
                                errorMessage={error && error?.message}
                            />}
                        rules={{
                            required: nlsHPCC.ValidationErrorRequired
                        }}
                    />
                    <Controller
                        control={control} name="password"
                        render={({
                            field: { onChange, name: fieldName, value },
                            fieldState: { error }
                        }) => <TextField
                                name={fieldName}
                                type="password"
                                onChange={onChange}
                                required={true}
                                label={nlsHPCC.Password}
                                value={value}
                                canRevealPassword
                                revealPasswordAriaLabel={nlsHPCC.ShowPassword}
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

                    <button type="submit" className={loginStyles.button}>{nlsHPCC.Login}</button>
                </form>
            </div>
        </div>
    </div>;

};