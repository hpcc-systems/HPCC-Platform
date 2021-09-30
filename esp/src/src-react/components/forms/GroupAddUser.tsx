import * as React from "react";
import { DefaultButton, MessageBar, MessageBarType, PrimaryButton, } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as WsAccess from "src/ws_access";
import { GroupMembersField } from "./Fields";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddUser.tsx");

interface GroupAddUserValues {
    usernames: string;
}

const defaultValues: GroupAddUserValues = {
    usernames: ""
};

interface GroupAddUserProps {
    refreshGrid?: () => void;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
    groupname: string;
}

export const GroupAddUserForm: React.FunctionComponent<GroupAddUserProps> = ({
    refreshGrid,
    showForm,
    setShowForm,
    groupname
}) => {

    const { handleSubmit, control, reset } = useForm<GroupAddUserValues>({ defaultValues });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;
                request.groupname = groupname;
                request.action = "add";

                WsAccess.GroupMemberEdit({ request: request })
                    .then(({ GroupMemberEditResponse }) => {
                        if (GroupMemberEditResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setErrorMessage(GroupMemberEditResponse?.retmsg);
                            logger.error(GroupMemberEditResponse?.retmsg);
                        } else {
                            closeForm();
                            reset(defaultValues);
                            if (refreshGrid) refreshGrid();
                        }
                    })
                    .catch(logger.error)
                    ;
            },
            logger.info
        )();
    }, [closeForm, groupname, handleSubmit, refreshGrid, reset]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.PleaseSelectAUserToAdd} width={400}
        footer={<>
            <PrimaryButton text={nlsHPCC.Add} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => { reset(defaultValues); closeForm(); }} />
        </>}>
        <Controller
            control={control} name="usernames"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <GroupMembersField
                    key={fieldName}
                    groupname={groupname}
                    required={true}
                    label={nlsHPCC.GroupName}
                    defaultSelectedKey={value}
                    onChange={(evt, option) => {
                        onChange(option.key);
                    }}
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
