import * as React from "react";
import { Overflow, Breadcrumb, OverflowItem, BreadcrumbItem, BreadcrumbButton, BreadcrumbButtonProps, BreadcrumbDivider, OverflowDivider } from "@fluentui/react-components";
import { bundleIcon, Folder20Filled, Folder20Regular, FolderOpen20Filled, FolderOpen20Regular, } from "@fluentui/react-icons";
import { OverflowMenu } from "./OverflowMenu";

const LineageIcon = bundleIcon(Folder20Filled, Folder20Regular);
const SelectedLineageIcon = bundleIcon(FolderOpen20Filled, FolderOpen20Regular);

export interface BreadcrumbInfo {
    id: string;
    label: string;
    props?: BreadcrumbButtonProps
}

interface OverflowGroupDividerProps {
    groupId: string;
}

const OverflowGroupDivider: React.FunctionComponent<OverflowGroupDividerProps> = ({
    groupId,
}) => {
    return <OverflowDivider groupId={groupId}>
        <BreadcrumbDivider data-group={groupId} />
    </OverflowDivider>;
};

function icon(breadcrumb: BreadcrumbInfo, selected: string) {
    return breadcrumb.id === selected ? <SelectedLineageIcon /> : <LineageIcon />;
}

export interface OverflowBreadcrumbProps {
    breadcrumbs: BreadcrumbInfo[];
    selected: string;
    onSelect: (tab: BreadcrumbInfo) => void;
}

export const OverflowBreadcrumb: React.FunctionComponent<OverflowBreadcrumbProps> = ({
    breadcrumbs,
    selected,
    onSelect
}) => {

    const overflowItems = React.useMemo(() => {
        return breadcrumbs.map((breadcrumb, idx) => <>
            <OverflowItem id={breadcrumb.id} groupId={breadcrumb.id} key={`button-items-${breadcrumb.id}`}>
                <BreadcrumbItem>
                    <BreadcrumbButton {...breadcrumb.props} current={breadcrumb.id === selected} icon={icon(breadcrumb, selected)} onClick={() => onSelect(breadcrumb)}>
                        {breadcrumb.label}
                    </BreadcrumbButton>
                </BreadcrumbItem>
            </OverflowItem>
            {idx < breadcrumbs.length - 1 && <OverflowGroupDivider groupId={breadcrumb.id} />}
        </>);
    }, [breadcrumbs, onSelect, selected]);

    return <Overflow>
        <Breadcrumb>
            {...overflowItems}
            <OverflowMenu menuItems={breadcrumbs.map(breadcrumb => ({ ...breadcrumb, icon: icon(breadcrumb, selected) }))} onMenuSelect={onSelect} />
        </Breadcrumb>
    </Overflow>;
};
