import * as React from "react";
import { Button, Combobox, Field, Input, MessageBar, MessageBarActions, MessageBarBody, Option } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, useWatch, Controller } from "react-hook-form";
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

    const { handleSubmit, control, reset, setValue } = useForm<CheckPermissionsFormValues>({ defaultValues });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");
    const [userOptions, setUserOptions] = React.useState<{ key: string; text: string }[]>([]);
    const [groupOptions, setGroupOptions] = React.useState<{ key: string; text: string }[]>([]);
    const [filePermissionResponse, setFilePermissionResponse] = React.useState<string>("");

    const userName = useWatch({ control, name: "UserName" });
    const groupName = useWatch({ control, name: "GroupName" });

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
                <Button appearance="primary" onClick={handleSubmit(onSubmit)}>{nlsHPCC.Submit}</Button>
                <Button onClick={closeForm}>{nlsHPCC.Cancel}</Button>
            </>}>
            <Controller
                control={control} name="FileName"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <Field label={nlsHPCC.Scope} validationMessage={error?.message}>
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
            <Field label={nlsHPCC.Users}>
                <Combobox
                    autoComplete="on"
                    value={userOptions.find(o => o.key === userName)?.text ?? userName ?? ""}
                    selectedOptions={userName ? [userName] : []}
                    onOptionSelect={(_, data) => {
                        setValue("UserName", data.optionValue ?? "");
                        setValue("GroupName", "");
                    }}
                >
                    {userOptions.map(o => <Option key={o.key} value={o.key}>{o.text}</Option>)}
                </Combobox>
            </Field>
            <Field label={nlsHPCC.Groups}>
                <Combobox
                    autoComplete="on"
                    value={groupOptions.find(o => o.key === groupName)?.text ?? groupName ?? ""}
                    selectedOptions={groupName ? [groupName] : []}
                    onOptionSelect={(_, data) => {
                        setValue("GroupName", data.optionValue ?? "");
                        setValue("UserName", "");
                    }}
                >
                    {groupOptions.map(o => <Option key={o.key} value={o.key}>{o.text}</Option>)}
                </Combobox>
            </Field>
            {filePermissionResponse && (
                <Field label={nlsHPCC.FilePermission}>
                    <Input value={filePermissionResponse} readOnly />
                </Field>
            )}
            {showError &&
                <div style={{ marginTop: 16 }}>
                    <MessageBar intent="error">
                        <MessageBarBody>{errorMessage}</MessageBarBody>
                        <MessageBarActions containerAction={<Button onClick={() => setShowError(false)} aria-label="Close" appearance="transparent" icon={<DismissRegular />} />} />
                    </MessageBar>
                </div>
            }
        </MessageBox>
    );
};