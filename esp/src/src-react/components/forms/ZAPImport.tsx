import * as React from "react";
import { Button, makeStyles, MessageBar, MessageBarActions, MessageBarBody, tokens } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { useFileUpload } from "../../hooks/useFileUpload";
import { MessageBox } from "../../layouts/MessageBox";

const logger = scopedLogger("src-react/components/forms/ZAPImport.tsx");

const useStyles = makeStyles({
    message: { padding: 0, marginTop: 0 },
    cancelBtn: { marginLeft: "10px" },
    actionsContainer: { display: "flex", flexDirection: "column" },
    progressMessage: {
        margin: "10px 10px 8px 0",
        display: "flex",
        flexDirection: "column",
        gap: "4px",
        width: "100%"
    },
    progressBarWrapper: {
        width: "100%",
        height: "6px",
        backgroundColor: tokens.colorNeutralBackground3,
        borderRadius: "3px",
        overflow: "hidden",
        position: "relative"
    },
    progressBarFill: {
        height: "100%",
        backgroundColor: tokens.colorBrandBackground,
        transition: "width .2s linear"
    }
});

interface ZAPImportProps {
    showForm: boolean;
    setShowForm: (_: boolean) => void;
    refreshGrid?: () => void;
}

export const ZAPImport: React.FunctionComponent<ZAPImportProps> = ({
    showForm,
    setShowForm,
    refreshGrid
}) => {

    const styles = useStyles();
    const uploaderBtnRef = React.useRef<HTMLInputElement>(null);
    const [uploadFile, setUploadFile] = React.useState<File | undefined>(undefined);
    const handleFileSelect = React.useCallback((evt: React.ChangeEvent<HTMLInputElement>) => {
        evt.preventDefault();
        evt.stopPropagation();
        const files = Array.from(evt.target.files ?? []);
        if (files.length > 0) {
            setUploadFile(files[0]);
        }
    }, [setUploadFile]);

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");
    const { uploadPct, isUploading, upload, cancelUpload } = useFileUpload();

    const closeForm = React.useCallback(() => {
        setShowForm(false);
    }, [setShowForm]);

    const doSubmit = React.useCallback((evt) => {
        if (!uploadFile) return;
        try {
            const formData = new FormData();
            formData.append("file", uploadFile);
            upload("/WsWorkunits/ImportWUZAPFile.json?upload_", formData, [uploadFile.name], (responseText) => {
                const parser = new DOMParser();
                const doc = parser.parseFromString(responseText, "text/html");
                const message = doc.querySelector("#DropzoneFileData b")?.textContent?.trim() ?? "";
                if (message === "File has been uploaded.") {
                    closeForm();
                } else {
                    setShowError(true);
                    setErrorMessage(message || responseText);
                    if (uploaderBtnRef.current) uploaderBtnRef.current.value = "";
                }
                // slight delay to refreshGrid allowing for the imported WU creation
                window.setTimeout(() => {
                    if (refreshGrid) refreshGrid();
                }, 200);
            });
        } catch (err) {
            logger.error(err);
        }
    }, [closeForm, refreshGrid, setErrorMessage, setShowError, upload, uploadFile]);

    return <MessageBox title={nlsHPCC.Import} show={showForm} setShow={closeForm} disableClose={isUploading}
        footer={<div className={styles.actionsContainer}>
            {isUploading &&
                <div className={styles.progressMessage} role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={uploadPct}>
                    <span>{nlsHPCC.Uploading}... {uploadPct > 0 && `${uploadPct}%`}</span>
                    <div className={styles.progressBarWrapper}>
                        <div className={styles.progressBarFill} style={{ width: `${uploadPct}%` }} />
                    </div>
                </div>
            }
            <div>
                <Button appearance="primary" onClick={doSubmit} disabled={isUploading}>{nlsHPCC.Upload}</Button>
                {isUploading
                    ? <Button className={styles.cancelBtn} onClick={() => {
                        if (window.confirm(nlsHPCC.CancelUploadConfirm)) {
                            cancelUpload();
                            closeForm();
                        }
                    }}>{nlsHPCC.Cancel}</Button>
                    : <Button className={styles.cancelBtn} onClick={() => closeForm()}>{nlsHPCC.Cancel}</Button>
                }
            </div>
        </div>}>
        <div>
            {showError &&
                <MessageBar intent="error">
                    <MessageBarBody>{errorMessage}</MessageBarBody>
                    <MessageBarActions containerAction={
                        <Button appearance="transparent" aria-label="Close" icon={<DismissRegular />} onClick={() => setShowError(false)} />
                    } />
                </MessageBar>
            }
            <p className={styles.message}>{nlsHPCC.SelectAZAPFile}</p>
            <input id="uploaderBtn" ref={uploaderBtnRef} disabled={isUploading} type="file" accept=".zip" onChange={handleFileSelect} multiple={false} />
        </div>
    </MessageBox>;
};