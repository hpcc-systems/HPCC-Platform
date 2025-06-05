import * as React from "react";
import { List, ListItem, makeStyles, mergeClasses, SelectionItemId, tokens } from "@fluentui/react-components";

const useStyles = makeStyles({
    item: {
        cursor: "pointer",
        padding: "2px 6px",
        justifyContent: "space-between",
    },
    itemSelected: {
        backgroundColor: tokens.colorSubtleBackgroundSelected,
        "@media (forced-colors:active)": {
            background: "Highlight",
        },
    },
});

interface ResourcesListProps {
    resources: string[];
    selectedResource?: string;
    setSelection: React.Dispatch<React.SetStateAction<string>>;
}

export const ResourcesList: React.FunctionComponent<ResourcesListProps> = ({
    resources,
    selectedResource = "",
    setSelection
}) => {

    const classes = useStyles();

    const [selectedItems, setSelectedItems] = React.useState<SelectionItemId[]>([]);

    const onSelectionChange = React.useCallback((_evt: React.SyntheticEvent | Event, data: { selectedItems: SelectionItemId[] }) => {
        setSelectedItems(data.selectedItems);
        setSelection(data.selectedItems[0].toString());
    }, [setSelection]);

    const onFocus = React.useCallback((evt: React.FocusEvent<HTMLLIElement>) => {
        // Ignore bubbled up events from the children
        if (evt.target !== evt.currentTarget) {
            return;
        }
        setSelectedItems([evt.currentTarget.dataset.value as SelectionItemId]);
    }, []);

    React.useEffect(() => {
        setSelectedItems([selectedResource]);
    }, [selectedResource]);

    return <List
        selectionMode="single"
        selectedItems={selectedItems}
        onSelectionChange={onSelectionChange}
    >
        {resources.map(url =>
            <ListItem
                key={url}
                value={url}
                className={mergeClasses(
                    classes.item,
                    selectedItems.includes(url) && classes.itemSelected
                )}
                onFocus={onFocus}
                checkmark={null}
            >{url}</ListItem>
        )}
    </List>;
};