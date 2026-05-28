import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { Input, Label, makeStyles } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "../layouts/react-reflex";
import { hashHistory, pushUrl } from "../util/history";
import { WorkunitDetails } from "./WorkunitDetails";
import { ShortVerticalDivider } from "./Common";

const useStyles = makeStyles({
    wuidInput: {
        width: "240px",
    },
    wuidField: {
        display: "flex",
        alignItems: "center",
        gap: "4px",
        padding: "0 4px",
    }
});

interface WorkunitCompareProps {
    wuids: string[];
    parentUrl?: string;
    tab?: string;
    fullscreen?: boolean;
    state?: React.ComponentProps<typeof WorkunitDetails>["state"];
    queryParams?: React.ComponentProps<typeof WorkunitDetails>["queryParams"];
}

export const WorkunitCompare: React.FunctionComponent<WorkunitCompareProps> = ({
    wuids,
    parentUrl = "/workunits",
    tab,
    fullscreen,
    state,
    queryParams
}) => {
    const styles = useStyles();

    const [localWuids, setLocalWuids] = React.useState<string[]>([...wuids, ""]);

    const lastPushedRef = React.useRef(wuids.join(","));

    React.useEffect(() => {
        const incoming = wuids.join(",");
        if (incoming !== lastPushedRef.current) {
            setLocalWuids([...wuids, ""]);
            lastPushedRef.current = incoming;
        }
    }, [wuids]);

    const handleLoad = React.useCallback(() => {
        const nonEmpty = localWuids.map(w => w.trim()).filter(w => w.length > 0);
        if (nonEmpty.length === 0) return;
        const joined = nonEmpty.join(",");
        lastPushedRef.current = joined;
        const currentPrefix = `${parentUrl}/${wuids.join(",")}${tab ? `/${tab}` : ""}`;
        const nextPrefix = `${parentUrl}/${joined}${tab ? `/${tab}` : ""}`;
        const suffix = hashHistory.location.pathname.startsWith(currentPrefix)
            ? hashHistory.location.pathname.slice(currentPrefix.length)
            : "";
        pushUrl(`${nextPrefix}${suffix}`);
    }, [localWuids, parentUrl, tab, wuids]);

    const handleChange = React.useCallback((index: number, value: string) => {
        setLocalWuids(prev => {
            const next = [...prev];
            next[index] = value;
            const nonEmpty = next.filter(w => w.trim().length > 0);
            return [...nonEmpty, ""];
        });
    }, []);

    const urlWuid = wuids.join(",");

    const toolbar = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => handleLoad()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        ...localWuids.map((wuid, index) => ({
            key: `wuid${index}`,
            onRender: () => <span className={styles.wuidField}>
                <Label htmlFor={`wuid-input-${index}`}>{`${nlsHPCC.WUID} ${index + 1}`}</Label>
                <Input
                    id={`wuid-input-${index}`}
                    className={styles.wuidInput}
                    value={wuid}
                    placeholder={nlsHPCC.WUID}
                    onChange={(_, d) => handleChange(index, d.value)}
                />
            </span>
        }))
    ], [handleChange, handleLoad, localWuids, styles.wuidField, styles.wuidInput]);

    return <HolyGrail
        header={<CommandBar items={toolbar} />}
        main={
            <ReflexContainer orientation="vertical">
                {wuids.flatMap((wuid, index) => {
                    const key = `${wuid}-${index}`;
                    const pane = <ReflexElement key={key}>
                        <WorkunitDetails
                            wuid={wuid}
                            urlWuid={urlWuid}
                            parentUrl={parentUrl}
                            tab={tab}
                            fullscreen={fullscreen}
                            state={state}
                            queryParams={queryParams}
                        />
                    </ReflexElement>;
                    return index > 0 ? [<ReflexSplitter key={`splitter-${key}`} />, pane] : [pane];
                })}
            </ReflexContainer>
        }
    />;
};

