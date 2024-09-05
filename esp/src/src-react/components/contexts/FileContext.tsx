import * as React from "react";
import { LogicalFile } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { singletonDebounce } from "../../util/throttle";
import { useCounter } from "../../hooks/util";

const logger = scopedLogger("../components/controls/FileContext.tsx");

const FileContext = React.createContext(null);

export const FileContextProvider = ({ cluster, logicalFile, children }) => {
    const [file, setFile] = React.useState<LogicalFile>();
    const [isProtected, setIsProtected] = React.useState(false);
    const [lastUpdate, setLastUpdate] = React.useState(Date.now());
    const [count, increment] = useCounter();

    React.useEffect(() => {
        const file = LogicalFile.attach({ baseUrl: "" }, cluster === "undefined" ? undefined : cluster, logicalFile);
        let active = true;
        let handle;
        const fetchInfo = singletonDebounce(file, "fetchInfo");
        fetchInfo()
            .then((response) => {
                if (active) {
                    setFile(file);
                    setIsProtected(response.ProtectList?.DFUFileProtect?.length > 0 || false);
                    setLastUpdate(Date.now());
                    handle = file.watch(() => {
                        setIsProtected(response.ProtectList?.DFUFileProtect?.length > 0 || false);
                        setLastUpdate(Date.now());
                    });
                }
            })
            .catch(err => logger.error(err));

        return () => {
            active = false;
            handle?.release();
        };
    }, [cluster, count, logicalFile]);

    return (
        <FileContext.Provider value={{ file, isProtected, lastUpdate, increment }}>
            {children}
        </FileContext.Provider>
    );
};

export const useFileContext = () => {
    const context = React.useContext(FileContext);
    if (!context) {
        throw new Error("useFileContext must be used within a FileContextProvider");
    }
    return context;
};

export default FileContextProvider;