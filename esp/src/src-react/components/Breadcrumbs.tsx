import { Breadcrumb, FontSizes, IBreadcrumbItem, IBreadcrumbStyleProps, IBreadcrumbStyles, IStyleFunctionOrObject } from "@fluentui/react";
import * as React from "react";

const breadCrumbStyles: IStyleFunctionOrObject<IBreadcrumbStyleProps, IBreadcrumbStyles> = {
    root: { margin: 0 },
    itemLink: { fontSize: FontSizes.size10, lineHeight: 20, paddingLeft: 2, paddingRight: 2 },
};

interface BreadcrumbsProps {
    hashPath: string;
    ignoreN?: number;
}

export const Breadcrumbs: React.FunctionComponent<BreadcrumbsProps> = ({
    hashPath,
    ignoreN = 0
}) => {

    const crumbs = React.useMemo(() => {
        const paths = decodeURI(hashPath).split("/");

        let fullPath = "#";
        return [{ text: "", key: "home", href: "#/" },
        ...paths.filter(path => !!path).map((path, idx) => {
            const retVal: IBreadcrumbItem = { text: path.toUpperCase(), key: "" + idx, href: `${fullPath}/${path}` };
            fullPath = `${fullPath}/${path}`;
            return retVal;
        }).filter((row, idx) => idx >= ignoreN)];
    }, [hashPath, ignoreN]);

    return <Breadcrumb items={crumbs} styles={breadCrumbStyles} />;
};
