import * as React from "react";
import { DefaultButton, PrimaryButton, Spinner, TextField, } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import * as WsAccess from "src/ws_access";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddGroup.tsx");

interface AddGroupFormValues {
    groupname: string;
    groupOwner: string;
    groupDesc: string;
}

const defaultValues: AddGroupFormValues = {
    groupname: "",
    groupOwner: "",
    groupDesc: ""
};

interface AddGroupFormProps {
    refreshGrid?: () => void;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const AddGroupForm: React.FunctionComponent<AddGroupFormProps> = ({
    refreshGrid,
    showForm,
    setShowForm
}) => {

    const { handleSubmit, control, reset } = useForm<AddGroupFormValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);
                const request: any = data;

                WsAccess.GroupAdd({ request: request })
                    .then(() => {
                        setSubmitDisabled(false);
                        setSpinnerHidden(true);
                        closeForm();
                        reset(defaultValues);
                        if (refreshGrid) refreshGrid();
                    })
                    .catch(err => logger.error(err))
                    ;
            },
            logger.info
        )();
    }, [closeForm, handleSubmit, refreshGrid, reset]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.AddGroup} minWidth={400}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Add} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => { reset(defaultValues); closeForm(); }} />
        </>}>
        <Controller
            control={control} name="groupname"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    required={true}
                    label={nlsHPCC.GroupName}
                    value={value}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="groupOwner"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    required={true}
                    label={nlsHPCC.ManagedBy}
                    value={value}
                    placeholder={nlsHPCC.ManagedByPlaceholder}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="groupDesc"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    required={true}
                    label={nlsHPCC.Description}
                    value={value}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
    </MessageBox>;
};
