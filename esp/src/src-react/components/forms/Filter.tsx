import * as React from "react";
import { getTheme, mergeStyleSets, FontWeights, IDragOptions, IIconProps, ContextualMenu, DefaultButton, PrimaryButton, IconButton, IStackStyles, Modal, Stack } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import nlsHPCC from "src/nlsHPCC";
import { Fields, Values } from "./Fields";
import { TableForm } from "./Forms";

interface FilterProps {
    filterFields: Fields;
    onApply: (values: Values) => void;

    showFilter: boolean;
    setShowFilter: (_: boolean) => void;
}

export const Filter: React.FunctionComponent<FilterProps> = ({
    filterFields,
    onApply,
    showFilter,
    setShowFilter
}) => {

    const [doSubmit, setDoSubmit] = React.useState(false);
    const [doReset, setDoReset] = React.useState(false);

    const closeFilter = () => setShowFilter(false);

    const titleId = useId("title");

    const dragOptions: IDragOptions = {
        moveMenuItemText: "Move",
        closeMenuItemText: "Close",
        menu: ContextualMenu,
    };

    const theme = getTheme();

    const contentStyles = mergeStyleSets({
        container: {
            display: "flex",
            flexFlow: "column nowrap",
            alignItems: "stretch",
        },
        header: [
            {
                flex: "1 1 auto",
                borderTop: `4px solid ${theme.palette.themePrimary}`,
                display: "flex",
                alignItems: "center",
                fontWeight: FontWeights.semibold,
                padding: "12px 12px 14px 24px",
            },
        ],
        body: {
            flex: "4 4 auto",
            padding: "0 24px 24px 24px",
            overflowY: "hidden",
            selectors: {
                p: { margin: "14px 0" },
                "p:first-child": { marginTop: 0 },
                "p:last-child": { marginBottom: 0 },
            },
        },
    });

    const cancelIcon: IIconProps = { iconName: "Cancel" };
    const iconButtonStyles = {
        root: {
            marginLeft: "auto",
            marginTop: "4px",
            marginRight: "2px",
        },
    };
    const buttonStackStyles: IStackStyles = {
        root: {
            height: "56px",
        },
    };
    return <Modal
        titleAriaId={titleId}
        isOpen={showFilter}
        onDismiss={closeFilter}
        isBlocking={false}
        containerClassName={contentStyles.container}
        dragOptions={dragOptions}
    >
        <div className={contentStyles.header}>
            <span id={titleId}>Filter</span>
            <IconButton
                styles={iconButtonStyles}
                iconProps={cancelIcon}
                ariaLabel="Close popup modal"
                onClick={closeFilter}
            />
        </div>
        <div className={contentStyles.body}>
            <Stack>
                <TableForm
                    fields={filterFields}
                    doSubmit={doSubmit}
                    doReset={doReset}
                    onSubmit={fields => {
                        setDoSubmit(false);
                        onApply(fields);
                    }}
                    onReset={() => {
                        setDoReset(false);
                    }}
                />
            </Stack>
            <Stack
                horizontal
                horizontalAlign="space-between"
                verticalAlign="end"
                styles={buttonStackStyles}
            >
                <DefaultButton
                    text={nlsHPCC.Clear}
                    onClick={() => {
                        setDoReset(true);
                    }}
                />
                <PrimaryButton
                    text={nlsHPCC.Apply}
                    onClick={() => {
                        setDoSubmit(true);
                        closeFilter();
                    }}
                />
            </Stack>
        </div>
    </Modal>;
};