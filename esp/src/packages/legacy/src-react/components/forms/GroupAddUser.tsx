import * as React from "react";
import { DefaultButton, IDropdownOption, MessageBar, MessageBarType, PrimaryButton, Spinner, } from "@fluentui/react";
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
                request.groupname = groupname;
                request.action = "add";

                WsAccess.GroupMemberEdit({ request: request })
                    .then(({ GroupMemberEditResponse }) => {
                        if (GroupMemberEditResponse?.retcode < 0) {
                            //log exception from API
                            setShowError(true);
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            setErrorMessage(GroupMemberEditResponse?.retmsg);
                            logger.error(GroupMemberEditResponse?.retmsg);
                        } else {
                            setSubmitDisabled(false);
                            setSpinnerHidden(true);
                            closeForm();
                            reset(defaultValues);
                            if (refreshGrid) refreshGrid();
                        }
                    })
                    .catch(err => logger.error(err))
                    ;
            },
            logger.info
        )();
    }, [closeForm, groupname, handleSubmit, refreshGrid, reset]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.PleaseSelectAUserToAdd} minWidth={400}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Add} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
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
                    label={nlsHPCC.Username}
                    selectedKey={value}
                    onChange={(evt, option: IDropdownOption) => {
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
