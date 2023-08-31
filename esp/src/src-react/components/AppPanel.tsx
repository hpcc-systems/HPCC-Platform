import * as React from "react";
import { DefaultButton, IconButton, IIconProps, IPanelProps, IRenderFunction, Panel, PanelType } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useWebLinks } from "../hooks/resources";

const waffleIcon: IIconProps = { iconName: "WaffleOffice365" };
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

    const onRenderNavigationContent: IRenderFunction<IPanelProps> = React.useCallback(
        (props, defaultRender) => (
            <>
                <IconButton iconProps={waffleIcon} onClick={onDismiss} style={{ width: 48, height: 48 }} />
                <span style={paddingStyle} />
                {defaultRender!(props)}
            </>
        ),
        [onDismiss],
    );

    const buttons = React.useMemo(() => {
        const retVal = [];
        webLinks?.forEach(webLink => {
            webLink.Annotations.NamedValue.forEach(nv => {
                retVal.push(<DefaultButton text={`${webLink.ServiceName} - ${nv.Name}`} href={`/${nv.Value}`} target="_blank" />);
            });
        });
        // Include HPCC Systems link when there are no other web links available
        if (retVal.length === 0) {
            retVal.push(<a key="hpcc-link" href="https://www.hpccsystems.com" target="_blank" rel="noopener noreferrer">{nlsHPCC.HPCCSystems}</a>);
        }
        return retVal;
    }, [webLinks]);

    return <Panel type={PanelType.smallFixedNear}
        onRenderNavigationContent={onRenderNavigationContent}
        headerText={nlsHPCC.Links}
        isLightDismiss
        isOpen={show}
        onDismiss={onDismiss}
        hasCloseButton={false}
    >
        {buttons}
    </Panel>;
};
