import * as React from "react";
import { Button, Checkbox, Field, Input, Spinner } from "@fluentui/react-components";
import { AccessService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddGroupResource.tsx");

const service = new AccessService({ baseUrl: "" });

interface AddGroupResourceFormValues {
    account_name: string;
    allow_access: boolean;
    allow_read: boolean;
    allow_write: boolean;
    allow_full: boolean;
}

const defaultValues: AddGroupResourceFormValues = {
    account_name: "",
    allow_access: false,
    allow_read: false,
    allow_write: false,
    allow_full: false
};

interface AddGroupResourceFormProps {
    rname: string;
    BasednName: string;
    refreshGrid?: () => void;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const AddGroupResourceForm: React.FunctionComponent<AddGroupResourceFormProps> = ({
    rname,
    BasednName,
    refreshGrid,
    showForm,
    setShowForm
}) => {

    const { handleSubmit, control, reset } = useForm<AddGroupResourceFormValues>({ defaultValues });
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

                request["action"] = "update";
                request["account_type"] = "1";
                request["rname"] = rname;
                request["BasednName"] = BasednName;

                service.PermissionAction(request)
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
    }, [BasednName, closeForm, handleSubmit, refreshGrid, reset, rname]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.AddGroup} minWidth={400}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Add}</Button>
            <Button onClick={() => { reset(defaultValues); closeForm(); }}>{nlsHPCC.Cancel}</Button>
        </>}>
        <Controller
            control={control} name="account_name"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <Field label={nlsHPCC.GroupName} required validationMessage={error?.message}>
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
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allow_access"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.AllowAccess} />}
            />
        </div>
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allow_read"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.AllowRead} />}
            />
        </div>
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allow_write"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.AllowWrite} />}
            />
        </div>
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allow_full"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={(_, data) => onChange(data.checked)} label={nlsHPCC.AllowFull} />}
            />
        </div>
    </MessageBox>;
};
