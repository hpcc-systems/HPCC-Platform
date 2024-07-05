import * as React from "react";
import { Checkbox, DefaultButton, PrimaryButton, TextField, } from "@fluentui/react";
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

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data, evt) => {
                const request: any = data;

                request["action"] = "update";
                request["account_type"] = "1";
                request["rname"] = rname;
                request["BasednName"] = BasednName;

                service.PermissionAction(request)
                    .then(() => {
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
            <PrimaryButton text={nlsHPCC.Add} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => { reset(defaultValues); closeForm(); }} />
        </>}>
        <Controller
            control={control} name="account_name"
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
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allow_access"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.AllowAccess} />}
            />
        </div>
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allow_read"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.AllowRead} />}
            />
        </div>
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allow_write"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.AllowWrite} />}
            />
        </div>
        <div style={{ paddingTop: "15px" }}>
            <Controller
                control={control} name="allow_full"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.AllowFull} />}
            />
        </div>
    </MessageBox>;
};
