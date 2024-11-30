import * as React from "react";
import { ChoiceGroup, DefaultButton, mergeStyleSets, PrimaryButton, Spinner, Stack, TextField, } from "@fluentui/react";
import { useForm, Controller } from "react-hook-form";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as WsDfu from "src/WsDfu";
import { MessageBox } from "../../layouts/MessageBox";
import * as FormStyles from "./landing-zone/styles";

const logger = scopedLogger("src-react/components/forms/AddToSuperfile.tsx");

interface AddToSuperfileFormValues {
    superFile: string;
    names?: {
        name: string
    }[];
    existingFile: number;
}

const defaultValues: AddToSuperfileFormValues = {
    superFile: ".::superfile",
    existingFile: 0
};

const componentStyles = mergeStyleSets(
    FormStyles.componentStyles,
    {
        container: {
            minWidth: 440,
        }
    }
);

interface AddToSuperfileProps {
    logicalFiles: string[];

    showForm: boolean;
    setShowForm: (_: boolean) => void;

    refreshGrid?: (_?: boolean) => void;
}

export const AddToSuperfile: React.FunctionComponent<AddToSuperfileProps> = ({
    logicalFiles,
    showForm,
    setShowForm,
    refreshGrid
}) => {

    const { handleSubmit, control, reset } = useForm<AddToSuperfileFormValues>({ defaultValues });
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
                WsDfu.AddtoSuperfile(data.names, data.superFile, data.existingFile)
                    .then(response => {
                        setSubmitDisabled(false);
                        setSpinnerHidden(true);
                        closeForm();
                        if (refreshGrid) refreshGrid(true);
                    })
                    .catch(err => logger.error(err))
                    ;
            },
            err => {
                logger.debug(err);
            }
        )();
    }, [closeForm, handleSubmit, refreshGrid]);

    React.useEffect(() => {
        if (logicalFiles.length > 0) {
            const files = [];
            logicalFiles.forEach((file, idx) => {
                files.push({ "Name": file });
            });
            const newValues = { ...defaultValues, "names": files };
            reset(newValues);
        }
    }, [logicalFiles, reset]);

    return <MessageBox title={nlsHPCC.AddToSuperfile} show={showForm} setShow={closeForm}
        footer={<>
            <Spinner label={nlsHPCC.Loading} labelPosition="right" style={{ display: spinnerHidden ? "none" : "inherit" }} />
            <PrimaryButton text={nlsHPCC.Add} disabled={submitDisabled} onClick={handleSubmit(onSubmit)} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={() => closeForm()} />
        </>}>
        <Stack>
            <Controller
                control={control} name="superFile"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TextField
                        name={fieldName}
                        label={nlsHPCC.SuperFile}
                        onChange={onChange}
                        value={value}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <Controller
                control={control} name="existingFile"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <ChoiceGroup
                        name={fieldName}
                        onChange={(evt, option) => onChange(option.key)}
                        defaultSelectedKey="0"
                        options={[
                            { key: "0", text: nlsHPCC.CreateANewFile },
                            { key: "1", text: nlsHPCC.AddToExistingSuperfile }
                        ]}
                    />}
            />
            {logicalFiles?.length > 0 &&
                <Stack>
                    <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`}>
                        <thead>
                            <tr>
                                <th>{nlsHPCC.LogicalName}</th>
                            </tr>
                        </thead>
                        <tbody>
                            {logicalFiles.map((file, idx) => {
                                return <tr key={`File-${idx}`}>
                                    <td>
                                        <Controller
                                            control={control} name={`names.${idx}.name` as const}
                                            render={({
                                                field: { onChange, name: fieldName },
                                                fieldState: { error }
                                            }) => <TextField
                                                    name={fieldName}
                                                    onChange={onChange}
                                                    value={file}
                                                    readOnly={true}
                                                />}
                                        />
                                    </td>
                                </tr>;
                            })}
                        </tbody>
                    </table>
                </Stack>
            }
        </Stack>
    </MessageBox>;
};
