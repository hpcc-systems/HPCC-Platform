import * as React from "react";
import { Button, DrawerBody, DrawerHeader, DrawerHeaderTitle, OverlayDrawer } from "@fluentui/react-components";
import { WaffleOffice365Regular } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";
import { useWebLinks } from "../hooks/resources";

const paddingStyle = { margin: "5px", height: "auto", width: "100%" };

interface AppPanelProps {
    show: boolean;
    onDismiss: () => void;
}

export const AppPanel: React.FunctionComponent<AppPanelProps> = ({
    show,
    onDismiss
}) => {

    const [webLinks, _refresh] = useWebLinks();

    const buttons = React.useMemo(() => {
        const retVal = [];
        webLinks?.forEach(webLink => {
            webLink.Annotations.NamedValue.forEach((nv: any) => {
                retVal.push(<Button key={`${webLink.ServiceName}-${nv.Name}`} as="a" href={`/${nv.Value}`} target="_blank" style={paddingStyle}>{`${webLink.ServiceName} - ${nv.Name}`}</Button>);
            });
        });
        // Include HPCC Systems link when there are no other web links available
        if (retVal.length === 0) {
            retVal.push(<a key="hpcc-link" href="https://www.hpccsystems.com" target="_blank" rel="noopener noreferrer">{nlsHPCC.HPCCSystems}</a>);
        }
        return retVal;
    }, [webLinks]);

    return <OverlayDrawer
        position="start"
        size="small"
        open={show}
        onOpenChange={(_, data) => { if (!data.open) onDismiss(); }}
    >
        <DrawerHeader>
            <div style={{ display: "flex", alignItems: "center" }}>
                <Button appearance="subtle" icon={<WaffleOffice365Regular />} onClick={onDismiss} style={{ width: 48, height: 48 }} />
                <span style={paddingStyle} />
                <DrawerHeaderTitle>{nlsHPCC.Links}</DrawerHeaderTitle>
            </div>
        </DrawerHeader>
        <DrawerBody>
            {buttons}
        </DrawerBody>
    </OverlayDrawer>;
};
