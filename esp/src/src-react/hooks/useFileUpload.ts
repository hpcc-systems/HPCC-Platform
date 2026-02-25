import * as React from "react";
import { useOnEvent } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/hooks/useFileUpload.ts");

export interface UseFileUploadReturn {
    uploadPct: number;
    isUploading: boolean;
    upload: (url: string, formData: FormData, fileName: string[], onLoad: (responseText: string) => void) => void;
    cancelUpload: () => void;
}

export const useFileUpload = (): UseFileUploadReturn => {

    const [uploadPct, setUploadPct] = React.useState<number>(0);
    const [isUploading, setIsUploading] = React.useState<boolean>(false);
    const uploadXHRRef = React.useRef<XMLHttpRequest | null>(null);

    const beforeUnloadHandler = React.useCallback((evt: BeforeUnloadEvent) => {
        evt.preventDefault();
        return nlsHPCC.Uploading;
    }, []);

    useOnEvent(isUploading ? window : null, "beforeunload", beforeUnloadHandler);

    const resetState = React.useCallback(() => {
        setIsUploading(false);
        setUploadPct(0);
        uploadXHRRef.current = null;
    }, []);

    const cancelUpload = React.useCallback(() => {
        if (uploadXHRRef.current) {
            try {
                uploadXHRRef.current.abort();
                logger.warning(nlsHPCC.UploadCancelled);
            } catch (err) {
                logger.error(nlsHPCC.ErrorAbortingUpload + ": " + err);
            }
        }
        resetState();
    }, [resetState]);

    const upload = React.useCallback((url: string, formData: FormData, fileNames: string[], onLoad: (responseText: string) => void) => {
        setIsUploading(true);
        setUploadPct(0);

        const xhr = new XMLHttpRequest();
        xhr.open("POST", url, true);
        uploadXHRRef.current = xhr;

        xhr.upload.onprogress = (evt) => {
            if (evt.lengthComputable) {
                const pct = Math.round((evt.loaded / evt.total) * 100);
                setUploadPct(pct);
            }
        };

        xhr.onload = () => {
            setIsUploading(false);
            uploadXHRRef.current = null;
            try {
                onLoad(xhr.responseText);
                logger.notice(`${nlsHPCC.UploadComplete}: ${fileNames.join(", ")}`);
            } catch (err) {
                logger.error(nlsHPCC.ErrorUploadingFile + ": " + err);
            } finally {
                setUploadPct(0);
            }
        };

        xhr.onerror = () => {
            resetState();
            logger.error(nlsHPCC.ErrorUploadingFile);
        };

        xhr.onabort = () => {
            resetState();
        };

        xhr.send(formData);
    }, [resetState]);

    return { uploadPct, isUploading, upload, cancelUpload };
};
