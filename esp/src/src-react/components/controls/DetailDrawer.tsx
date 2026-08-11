import * as React from "react";
import { Button, DrawerBody, DrawerHeader, DrawerHeaderTitle, OverlayDrawer, makeStyles } from "@fluentui/react-components";
import { Dismiss20Regular } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";

const useStyles = makeStyles({
    drawerBody: {
        padding: "0",
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
        flex: "1 1 auto"
    },
    drawerContent: {
        flex: "1 1 auto",
        height: "100%",
        overflow: "hidden"
    }
});

interface DetailDrawerProps {
    open: boolean;
    title?: string;
    position?: "start" | "end" | "bottom";
    onClose: () => void;
    children?: React.ReactNode;
}

export const DetailDrawer: React.FunctionComponent<DetailDrawerProps> = ({
    open,
    title,
    position = "end",
    onClose,
    children
}) => {
    const styles = useStyles();

    return <OverlayDrawer
        position={position}
        open={open}
        style={{ width: "calc(100vw - 120px)" }}
        onOpenChange={(_, data) => { if (!data.open) onClose(); }}
    >
        <DrawerHeader>
            <DrawerHeaderTitle
                action={
                    <Button
                        appearance="subtle"
                        icon={<Dismiss20Regular />}
                        onClick={onClose}
                        aria-label={nlsHPCC.Close}
                    />
                }
            >
                {title}
            </DrawerHeaderTitle>
        </DrawerHeader>
        <DrawerBody className={styles.drawerBody}>
            <div className={styles.drawerContent}>
                {children}
            </div>
        </DrawerBody>
    </OverlayDrawer>;
};
