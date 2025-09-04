import * as React from "react";
import { DefaultButton, PrimaryButton, Spinner, TextField } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import nlsHPCC from "src/nlsHPCC";
import { addUserFile } from "../../../comms/fileSpray";
import { MessageBox } from "../../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/AddFileForm.tsx");

interface AddFileFormValues {
    NetAddress: string;
    fullPath: string;
}

const defaultValues: AddFileFormValues = {
    NetAddress: "",
    fullPath: ""
};

interface AddFileFormProps {
    formMinWidth?: number;
    showForm: boolean;
    refreshGrid: (() => void);
    setShowForm: (_: boolean) => void;
    dropzones?: any[];
}

export const AddFileForm: React.FunctionComponent<AddFileFormProps> = ({
    formMinWidth = 300,
    showForm,
    refreshGrid,
    setShowForm,
    dropzones = []
}) => {

    const { handleSubmit, control, reset } = useForm<AddFileFormValues>({ defaultValues });
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

                // find dropzone by NetAddress
                const dropZone = dropzones.find(dz =>
                    dz.TpMachines?.TpMachine?.some(machine =>
                        machine.Netaddress === data.NetAddress ||
                        machine.ConfigNetaddress === data.NetAddress
                    )
                );

                if (!dropZone) {
                    logger.error(`Dropzone not found for NetAddress: ${data.NetAddress}`);
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                    return;
                }

                addUserFile({
                    NetAddress: data.NetAddress,
                    fullPath: data.fullPath,
                    dropZone: dropZone
                }).then((fileItem) => {
                    logger.debug(`Successfully added user file: ${fileItem.name}`);
                    refreshGrid();
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                    closeForm();
                    reset(defaultValues);
                }).catch((error) => {
                    logger.error(error);
                    setSubmitDisabled(false);
                    setSpinnerHidden(true);
                });
            },
            err => {
                logger.error(err);
                setSubmitDisabled(false);
                setSpinnerHidden(true);
            }
        )();
    }, [closeForm, handleSubmit, refreshGrid, reset, dropzones]);

    return <MessageBox title={nlsHPCC.AddFile} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Add} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Controller
            control={control} name="NetAddress"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.IP}
                    required={true}
                    value={value}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
        <Controller
            control={control} name="fullPath"
            render={({
                field: { onChange, name: fieldName, value },
                fieldState: { error }
            }) => <TextField
                    name={fieldName}
                    onChange={onChange}
                    label={nlsHPCC.Path}
                    required={true}
                    value={value}
                    placeholder={nlsHPCC.NamePrefixPlaceholder}
                    errorMessage={error && error?.message}
                />}
            rules={{
                required: nlsHPCC.ValidationErrorRequired
            }}
        />
    </MessageBox>;
};