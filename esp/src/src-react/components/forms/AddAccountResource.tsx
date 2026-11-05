import * as React from "react";
import { Button, Checkbox, Field, Label, MessageBar, MessageBarBody, Radio, RadioGroup, Spinner, tokens } from "@fluentui/react-components";
import { AccessService, WsAccess } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { UserField, GroupField } from "./Fields";
import { calcPermissionState } from "../PermissionsEditor";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddAccountResource.tsx");

const service = new AccessService({ baseUrl: "" });

interface AddAccountResourceFormValues {
    account_name: string;
    allow_access: boolean;
    allow_read: boolean;
    allow_write: boolean;
    allow_full: boolean;
    deny_access: boolean;
    deny_read: boolean;
    deny_write: boolean;
    deny_full: boolean;
}

const defaultValues: AddAccountResourceFormValues = {
    account_name: "",
    allow_access: false,
    allow_read: false,
    allow_write: false,
    allow_full: false,
    deny_access: false,
    deny_read: false,
    deny_write: false,
    deny_full: false
};

interface AddAccountResourceFormProps {
    rname: string;
    BasednName: string;
    refreshGrid?: () => void;
    showForm: boolean;
    setShowForm: (_: boolean) => void;
}

export const AddAccountResourceForm: React.FunctionComponent<AddAccountResourceFormProps> = ({
    rname,
    BasednName,
    refreshGrid,
    showForm,
    setShowForm
}) => {

    const [accountType, setAccountType] = React.useState<"0" | "1">("1");
    const { handleSubmit, control, reset, setValue, getValues } = useForm<AddAccountResourceFormValues>({ defaultValues });
    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [spinnerHidden, setSpinnerHidden] = React.useState(true);
    const [actionError, setActionError] = React.useState("");

    type PermissionFlag = "allow_access" | "allow_read" | "allow_write" | "allow_full" | "deny_access" | "deny_read" | "deny_write" | "deny_full";
    const permissionFlags: PermissionFlag[] = ["allow_access", "allow_read", "allow_write", "allow_full", "deny_access", "deny_read", "deny_write", "deny_full"];

    // Normalize via the same logic as PermissionsEditor so setting an Allow clears the
    // conflicting Deny (and vice versa), rather than leaving both flags set.
    const handlePermissionChange = React.useCallback((field: PermissionFlag, checked: boolean) => {
        const row: Record<string, boolean> = { ...getValues() } as unknown as Record<string, boolean>;
        calcPermissionState(field, checked, row);
        permissionFlags.forEach(f => setValue(f, !!row[f]));
    }, [getValues, setValue]); // eslint-disable-line react-hooks/exhaustive-deps

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const onSubmit = React.useCallback(() => {
        handleSubmit(
            (data) => {
                setSubmitDisabled(true);
                setSpinnerHidden(false);
                setActionError("");
                const request: WsAccess.PermissionActionRequest = { ...data };
                request["action"] = "update";
                request["account_type"] = Number(accountType);
                request["rname"] = rname;
                request["BasednName"] = BasednName;

                service.PermissionAction(request)
                    .then(({ retcode, retmsg }) => {
                        if (retcode === 0) {
                            closeForm();
                            reset(defaultValues);
                            setAccountType("1");
                            if (refreshGrid) refreshGrid();
                        } else {
                            setActionError(retmsg || nlsHPCC.Error);
                        }
                    })
                    .catch(err => logger.error(err))
                    .finally(() => {
                        setSubmitDisabled(false);
                        setSpinnerHidden(true);
                    });
            },
            logger.info
        )();
    }, [accountType, BasednName, closeForm, handleSubmit, refreshGrid, reset, rname]);

    return <MessageBox show={showForm} setShow={closeForm} title={nlsHPCC.AddAccountToScope} minWidth={420}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="after" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <Button appearance="primary" disabled={submitDisabled} onClick={handleSubmit(onSubmit)}>{nlsHPCC.Add}</Button>
            <Button onClick={() => { reset(defaultValues); setAccountType("1"); setActionError(""); closeForm(); }}>{nlsHPCC.Cancel}</Button>
        </>}>

        {actionError && <MessageBar intent="error" style={{ marginBottom: tokens.spacingVerticalS }}>
            <MessageBarBody>{actionError}</MessageBarBody>
        </MessageBar>}

        {/* Account Type */}
        <Field label={nlsHPCC.AccountType} required>
            <RadioGroup layout="horizontal" value={accountType} onChange={(_, data) => {
                setAccountType(data.value as "0" | "1");
                setValue("account_name", "");
            }}>
                <Radio value="1" label={nlsHPCC.Group} />
                <Radio value="0" label={nlsHPCC.User} />
            </RadioGroup>
        </Field>

        {/* Account Name — freeform combobox, pick from the list or type a custom name */}
        <div style={{ paddingTop: tokens.spacingVerticalMNudge }}>
            <Controller
                control={control} name="account_name"
                render={({ field: { onChange, value }, fieldState: { error } }) =>
                    accountType === "1"
                        ? <GroupField
                            key="group"
                            label={nlsHPCC.GroupName}
                            required={true}
                            selectedKey={value}
                            onChange={(newValue: string) => onChange(newValue)}
                            errorMessage={error?.message}
                        />
                        : <UserField
                            key="user"
                            label={nlsHPCC.UserName}
                            required={true}
                            selectedKey={value}
                            onChange={(newValue: string) => onChange(newValue)}
                            errorMessage={error?.message}
                        />
                }
                rules={{ required: nlsHPCC.ValidationErrorRequired }}
            />
        </div>

        {/* Allow flags */}
        <div style={{ paddingTop: tokens.spacingVerticalL }}>
            <Label>{nlsHPCC.Allow}</Label>
        </div>
        {(["allow_access", "allow_read", "allow_write", "allow_full"] as const).map(field => (
            <div key={field} style={{ paddingTop: tokens.spacingVerticalS }}>
                <Controller
                    control={control} name={field}
                    render={({ field: { name: fieldName, value } }) =>
                        <Checkbox name={fieldName} checked={value} onChange={(_, data) => handlePermissionChange(field, !!data.checked)}
                            label={nlsHPCC[field === "allow_access" ? "AllowAccess" : field === "allow_read" ? "AllowRead" : field === "allow_write" ? "AllowWrite" : "AllowFull"]} />
                    }
                />
            </div>
        ))}

        {/* Deny flags */}
        <div style={{ paddingTop: tokens.spacingVerticalL }}>
            <Label>{nlsHPCC.Deny}</Label>
        </div>
        {(["deny_access", "deny_read", "deny_write", "deny_full"] as const).map(field => (
            <div key={field} style={{ paddingTop: tokens.spacingVerticalS }}>
                <Controller
                    control={control} name={field}
                    render={({ field: { name: fieldName, value } }) =>
                        <Checkbox name={fieldName} checked={value} onChange={(_, data) => handlePermissionChange(field, !!data.checked)}
                            label={nlsHPCC[field === "deny_access" ? "DenyAccess" : field === "deny_read" ? "DenyRead" : field === "deny_write" ? "DenyWrite" : "DenyFull"]} />
                    }
                />
            </div>
        ))}
    </MessageBox>;
};
