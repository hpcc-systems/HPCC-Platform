import * as React from "react";
import { Dropdown, IDropdownOption, Stack } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useBuildInfo } from "../../hooks/platform";
import { useUserStartPage } from "../../hooks/user";

export interface GeneralSettingsProps {
    handleSubmit: () => void
}

export const GeneralSettings = React.forwardRef((props, ref) => {

    const [, { isContainer }] = useBuildInfo();
    const { startPage, setStartPage } = useUserStartPage();
    const [selectedStartPage, setSelectedStartPage] = React.useState("");

    React.useEffect(() => {
        setSelectedStartPage(startPage);
    }, [startPage]);

    const startPages: IDropdownOption[] = React.useMemo(() => {
        return [
            { key: "/activities", text: "Activities" },
            { key: "/workunits", text: "Workunits" },
            { key: "/files", text: "Files" },
            { key: "/queries", text: "Queries" },
            isContainer ? { key: "/topology", text: "Topology" } : { key: "/operations", text: "Operations" }
        ];
    }, [isContainer]);

    React.useImperativeHandle(ref, () => ({
        handleSubmit: async () => {
            setStartPage(selectedStartPage);
        }
    }));

    return <Stack tokens={{ childrenGap: 10 }}>
        <Dropdown
            key={"startPage"}
            label={nlsHPCC.DefaultStartPage}
            options={startPages}
            selectedKey={selectedStartPage}
            onChange={(evt, option) => {
                setSelectedStartPage(option.key.toString());
            }}
        />
    </Stack>;
});