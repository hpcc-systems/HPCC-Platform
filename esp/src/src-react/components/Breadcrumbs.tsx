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
        const paths = decodeURI(hashPath).split("/").filter(path => !!path);

        return [{ text: "", key: "home", href: "#/" },
        ...paths.map((path, idx) => {
            const href = idx < (paths.length - 1) ? `#/${paths.slice(0, idx + 1).join("/")}` : undefined;
            const style = { fontSize: 10, lineHeight: "10px" };
            return { text: path.toUpperCase(), key: "" + idx, href, style } as IBreadcrumbItem;
        }).filter((row, idx) => idx >= ignoreN)];
    }, [hashPath, ignoreN]);

    return <Breadcrumb items={crumbs} styles={breadCrumbStyles} />;
};
