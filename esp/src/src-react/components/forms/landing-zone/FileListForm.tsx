import * as React from "react";
import { Checkbox, DefaultButton, IDropdownOption, mergeStyleSets, PrimaryButton, Stack } from "@fluentui/react";
import { FileSprayService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { useForm, Controller } from "react-hook-form";
import { TargetDropzoneTextField, TargetFolderTextField, TargetServerTextField } from "../Fields";
import { joinPath } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useUserTheme } from "../../../hooks/theme";
import { MessageBox } from "../../../layouts/MessageBox";
import { debounce } from "../../../util/throttle";
import * as FormStyles from "./styles";

const logger = scopedLogger("src-react/components/forms/landing-zone/FileListForm.tsx");

const myFileSprayService = new FileSprayService({ baseUrl: "" });

const targetFolderTextChange = debounce((text, directory, onChange) => {
    onChange(directory + text);
}, 200);

interface FileListFormValues {
    dropzone: string;
    machines: string;
    path: string;
    selectedFiles?: {
        TargetName: string,
        RecordSize: string,
        SourceFile: string,
        SourceIP: string,
    }[],
    overwrite: boolean,
}

const defaultValues: FileListFormValues = {
    dropzone: "",
    machines: "",
    path: "",
    overwrite: false,
};
interface FileListFormProps {
    formMinWidth?: number;
    showForm: boolean;
    selection: object[];
    setShowForm: (_: boolean) => void;
    onSubmit?: (dropzoneName?: string, netAddress?: string) => void;
}

export const FileListForm: React.FunctionComponent<FileListFormProps> = ({
    formMinWidth = 300,
    showForm,
    selection,
    setShowForm,
    onSubmit
}) => {

    const [machine, setMachine] = React.useState<string>("");
    const [directory, setDirectory] = React.useState<string>("/");
    const [dropzone, setDropzone] = React.useState("");
    const [pathSep, setPathSep] = React.useState<string>("/");
    const [os, setOs] = React.useState<number>();

    const [submitDisabled, setSubmitDisabled] = React.useState(false);
    const [uploadPct, setUploadPct] = React.useState<number>(0);

    const uploadXHRRef = React.useRef<XMLHttpRequest | null>(null);

    const { handleSubmit, control, reset } = useForm<FileListFormValues>({ defaultValues });
    const { theme } = useUserTheme();

    const beforeUnloadHandler = React.useCallback((evt: BeforeUnloadEvent) => {
        evt.preventDefault();
        return nlsHPCC.Uploading;
    }, []);

    const addBeforeUnload = React.useCallback(() => {
        window.addEventListener("beforeunload", beforeUnloadHandler);
    }, [beforeUnloadHandler]);

    const removeBeforeUnload = React.useCallback(() => {
        window.removeEventListener("beforeunload", beforeUnloadHandler);
    }, [beforeUnloadHandler]);

    const closeForm = React.useCallback(() => {
        setShowForm(false);
        removeBeforeUnload();
        setUploadPct(0);
        const uploaderBtn = document.querySelector("#uploaderBtn");
        if (uploaderBtn) {
            uploaderBtn["value"] = null;
        }
    }, [removeBeforeUnload, setShowForm]);

    const cancelUpload = React.useCallback(() => {
        if (uploadXHRRef.current) {
            try {
                uploadXHRRef.current.abort();
                logger.warning(nlsHPCC.UploadCancelled);
            } catch (err) {
                logger.error(nlsHPCC.ErrorAbortingUpload + ": " + err);
            } finally {
                uploadXHRRef.current = null;
            }
        }
        removeBeforeUnload();
        setSubmitDisabled(false);
        setUploadPct(0);
        closeForm();
    }, [closeForm, removeBeforeUnload]);

    const doSubmit = React.useCallback((data) => {
        const uploadFiles = (folderPath, selection) => {
            const formData = new FormData();
            selection.forEach(file => {
                formData.append("uploadedfiles[]", file);
            });
            const uploadUrl = `/FileSpray/UploadFile.json?upload_&rawxml_=1&NetAddress=${machine}&OS=${os}&Path=${folderPath}&DropZoneName=${data.dropzone}`;

            setSubmitDisabled(true);
            setUploadPct(0);

            // have to use XMLHttpRequest over native fetch to capture upload progress
            const xhr = new XMLHttpRequest();
            xhr.open("POST", uploadUrl, true);
            uploadXHRRef.current = xhr;

            xhr.upload.onprogress = (evt) => {
                if (evt.lengthComputable) {
                    const pct = Math.round((evt.loaded / evt.total) * 100);
                    setUploadPct(pct);
                    if (pct < 100) {
                        addBeforeUnload();
                    }
                }
            };

            xhr.onload = () => {
                removeBeforeUnload();
                setSubmitDisabled(false);
                uploadXHRRef.current = null;
                try {
                    const response = JSON.parse(xhr.responseText || "{}");
                    const exceptions = response?.Exceptions?.Exception ?? [];
                    if (exceptions.length > 0) {
                        logger.error(exceptions[0]?.Message ?? nlsHPCC.ErrorUploadingFile);
                        return;
                    }
                    const DFUActionResult = response?.UploadFilesResponse?.UploadFileResults?.DFUActionResult ?? [];
                    if (DFUActionResult.filter(result => result.Result !== "Success").length > 0) {
                        logger.error(nlsHPCC.ErrorUploadingFile);
                    } else {
                        closeForm();
                        if (typeof onSubmit === "function") {
                            onSubmit(data.dropzone, machine);
                        }
                        reset(defaultValues);
                    }
                } catch (err) {
                    logger.error(nlsHPCC.ErrorUploadingFile + ": " + err);
                } finally {
                    setUploadPct(0);
                }
            };

            xhr.onerror = () => {
                removeBeforeUnload();
                setSubmitDisabled(false);
                uploadXHRRef.current = null;
                logger.error(nlsHPCC.ErrorUploadingFile);
                setUploadPct(0);
            };

            xhr.onabort = () => {
                removeBeforeUnload();
                setSubmitDisabled(false);
                uploadXHRRef.current = null;
                setUploadPct(0);
            };

            xhr.send(formData);
        };

        handleSubmit(
            (data, evt) => {
                const folderPath = joinPath(data.path, pathSep);
                if (data.overwrite) {
                    uploadFiles(folderPath, selection);
                } else {
                    const fileNames = selection.map(file => file["name"]);
                    myFileSprayService.FileList({
                        Netaddr: machine,
                        Path: folderPath
                    }).then((response) => {
                        let fileName = "";
                        response?.files?.PhysicalFileStruct.forEach(file => {
                            if (fileNames.indexOf(file.name) > -1) {
                                fileName = file.name;
                                return;
                            }
                        });
                        if (fileName === "") {
                            uploadFiles(folderPath, selection);
                        } else {
                            alert(nlsHPCC.OverwriteMessage + "\n" + fileNames.join("\n"));
                        }
                    }).catch(err => logger.error(err));
                }

            },
            err => {
                console.log(err);
            }
        )();
    }, [addBeforeUnload, closeForm, handleSubmit, machine, onSubmit, os, pathSep, removeBeforeUnload, reset, selection]);

    const componentStyles = mergeStyleSets(
        FormStyles.componentStyles,
        {
            container: { minWidth: formMinWidth ? formMinWidth : 300 },
            progressMessage: {
                margin: "10px 10px 8px 0",
                display: "flex",
                flexDirection: "column",
                gap: 4,
                width: "100%"
            },
            progressBarWrapper: {
                width: "100%",
                height: 6,
                background: theme.palette.neutralLight,
                borderRadius: 3,
                overflow: "hidden",
                position: "relative"
            },
            progressBarFill: {
                height: "100%",
                background: theme.palette.themePrimary,
                transition: "width .2s linear",
                width: 0
            }
        }
    );

    return <MessageBox title={nlsHPCC.FileUploader} show={showForm} setShow={closeForm} disableClose={submitDisabled}
        footer={<>
            {submitDisabled &&
                // TODO: need to figure out why there's some theme clashing issue here
                // preventing just using a ProgressBar wrapped with a FluentProvider...
                <div className={componentStyles.progressMessage} role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={uploadPct}>
                    <span>{nlsHPCC.Uploading}... {uploadPct > 0 && `${uploadPct}%`}</span>
                    <div className={componentStyles.progressBarWrapper}>
                        <div className={componentStyles.progressBarFill} style={{ width: `${uploadPct}%` }} />
                    </div>
                </div>
            }
            <PrimaryButton text={nlsHPCC.Upload} onClick={handleSubmit(doSubmit)} disabled={submitDisabled} />
            {submitDisabled &&
                <DefaultButton text={nlsHPCC.Cancel} onClick={() => {
                    if (window.confirm(nlsHPCC.CancelUploadConfirm)) {
                        cancelUpload();
                    }
                }} />
            }
        </>}>
        <Stack>
            <Controller
                control={control} name="dropzone"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetDropzoneTextField
                        key="dropzone"
                        label={nlsHPCC.LandingZone}
                        required={true}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option: IDropdownOption) => {
                            setDirectory(option["path"] as string);
                            if (option["path"].indexOf("\\") > -1) {
                                setPathSep("\\");
                            }
                            setDropzone(option.key as string);
                            onChange(option.key);
                        }}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <Controller
                control={control} name="machines"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetServerTextField
                        key="machines"
                        label={nlsHPCC.Machines}
                        dropzone={dropzone}
                        required={true}
                        placeholder={nlsHPCC.SelectValue}
                        onChange={(evt, option: IDropdownOption) => {
                            if (option) {
                                setMachine(option.key as string);
                                setOs(option["OS"] as number);
                                onChange(option.key);
                            }
                        }}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired
                }}
            />
            <Controller
                control={control} name="path"
                render={({
                    field: { onChange, name: fieldName, value },
                    fieldState: { error }
                }) => <TargetFolderTextField
                        key="path"
                        label={nlsHPCC.Folder}
                        pathSepChar={pathSep}
                        dropzone={dropzone}
                        machineAddress={machine}
                        machineDirectory={directory}
                        machineOS={os}
                        required={true}
                        placeholder={nlsHPCC.SelectValue}
                        onInputValueChange={(text) => targetFolderTextChange(text, directory, onChange)}
                        onChange={(evt, option) => {
                            if (option?.key) {
                                onChange(option.key);
                            }
                        }}
                        errorMessage={error && error?.message}
                    />}
                rules={{
                    required: nlsHPCC.ValidationErrorRequired,
                    pattern: {
                        value: /^(\/[a-z0-9]*)+$/i,
                        message: nlsHPCC.ValidationErrorTargetNameRequired
                    }
                }}
            />
        </Stack>
        <Stack>
            <table className={`${componentStyles.twoColumnTable} ${componentStyles.selectionTable}`}>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>{nlsHPCC.Type}</th>
                        <th>{nlsHPCC.FileName}</th>
                        <th>{nlsHPCC.Size}</th>
                    </tr>
                </thead>
                <tbody>
                    {selection && selection.map((file, idx) => {
                        return <tr key={`File-${idx}`}>
                            <td>{idx + 1}</td>
                            <td>{file["name"].substr(file["name"].lastIndexOf(".") + 1).toUpperCase()}</td>
                            <td>{file["name"]}</td>
                            <td>{`${(parseInt(file["size"], 10) / 1024).toFixed(2)} kb`}</td>
                        </tr>;
                    })}
                </tbody>
            </table>
            <Controller
                control={control} name="overwrite"
                render={({
                    field: { onChange, name: fieldName, value }
                }) => <Checkbox name={fieldName} checked={value} onChange={onChange} label={nlsHPCC.Overwrite} />}
            />
        </Stack>
    </MessageBox>;
};