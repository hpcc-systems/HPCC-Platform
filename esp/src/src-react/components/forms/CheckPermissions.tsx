import * as React from "react";
import { ComboBox, DefaultButton, IDropdownOption, MessageBar, MessageBarType, PrimaryButton, TextField } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";
import { FilePermission } from "src/ws_access";

const logger = scopedLogger("src-react/components/forms/CheckPermissions.tsx");

interface CheckPermissionsFormValues {
    FileName: string;
    UserName: string;
    GroupName: string;
}

const defaultValues: CheckPermissionsFormValues = {
    FileName: "",
    UserName: "",
    GroupName: "",
};

interface CheckPermissionsFormProps {
    refreshGrid?: () => void;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const CheckPermissionsForm: React.FunctionComponent<CheckPermissionsFormProps> = ({
    refreshGrid,
    showForm,
    setShowForm
}) => {

    const { handleSubmit, control, reset, setValue, watch } = useForm<CheckPermissionsFormValues>({ defaultValues });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");
    const [userOptions, setUserOptions] = React.useState<IDropdownOption[]>([]);
    const [groupOptions, setGroupOptions] = React.useState<IDropdownOption[]>([]);
    const [filePermissionResponse, setFilePermissionResponse] = React.useState<string>("");

    React.useEffect(() => {
        const fetchData = async () => {
            try {
                const { FilePermissionResponse } = await FilePermission({});
                const { Users, Groups } = FilePermissionResponse;
                const sortedUsers = Users?.User?.sort((a, b) => a.username.localeCompare(b.username));
                const sortedGroups = Groups?.Group?.sort((a, b) => a.name.localeCompare(b.name));

                setUserOptions(sortedUsers?.map((user: { username: any; }) => {
                    return { key: user.username, text: user.username };
                }) ?? []);

                setGroupOptions(sortedGroups?.map(group => {
                    return { key: group.name, text: group.name };
                }) ?? []);
            } catch (error) {
                logger.error(error);
            }
        };

        fetchData();
    }, []);

    const closeForm = React.useCallback(() => {
        reset(defaultValues);
        setShowForm(false);
        setFilePermissionResponse("");
    }, [reset, setShowForm]);

    const onSubmit = async (data: CheckPermissionsFormValues) => {
        try {
            const { FileName, UserName, GroupName } = data;
            logger.info(`Checking permissions for file ${FileName} for UserName: ${UserName} and GroupName: ${GroupName}`);
            const response = await FilePermission({
                request: {
                    FileName,
                    UserName,
                    GroupName
                }
            });
            setFilePermissionResponse(response.FilePermissionResponse.UserPermission);
        } catch (error) {
            logger.error(error);
            setErrorMessage(nlsHPCC.FilePermissionError);
            setShowError(true);
        }
    };

    return (
        <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.CheckFilePermissions} minWidth={500}
            footer={<>
                <PrimaryButton text={nlsHPCC.Submit} onClick={handleSubmit(onSubmit)} />
                <DefaultButton text={nlsHPCC.Cancel} onClick={closeForm} />
            </>}>
            <Controller
                control={control} name="FileName"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        onChange={onChange}
                        label={nlsHPCC.Scope}
                        value={value}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <ComboBox
                label={nlsHPCC.Users}
                autoComplete="on"
                options={userOptions}
                selectedKey={watch("UserName")}
                onChange={(ev, option) => {
                    setValue("UserName", option?.key.toString() || "");
                    setValue("GroupName", "");
                }}
            />
            <ComboBox
                label={nlsHPCC.Groups}
                autoComplete="on"
                options={groupOptions}
                selectedKey={watch("GroupName")}
                onChange={(ev, option) => {
                    setValue("GroupName", option?.key.toString() || "");
                    setValue("UserName", "");
                }}
            />
            {filePermissionResponse && (
                <TextField
                    label={nlsHPCC.FilePermission}
                    value={filePermissionResponse}
                    readOnly={true}
                />
            )}
            {showError &&
                <div style={{ marginTop: 16 }}>
                    <MessageBar
                        messageBarType={MessageBarType.error} isMultiline={true}
                        onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                        {errorMessage}
                    </MessageBar>
                </div>
            }
        </MessageBox>
    );
};