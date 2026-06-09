import * as React from "react";
import { Breadcrumb, BreadcrumbButton, BreadcrumbDivider, BreadcrumbItem, makeStyles, tokens } from "@fluentui/react-components";

const useStyles = makeStyles({
    breadcrumb: {
        margin: 0,
    },
    button: {
        fontSize: tokens.fontSizeBase100,
        lineHeight: "20px",
        paddingLeft: "2px",
        paddingRight: "2px",
        minHeight: "unset",
        height: "auto",
    },
});

interface Crumb {
    key: string;
    text: string;
    href?: string;
}

interface BreadcrumbsProps {
    hashPath: string;
    ignoreN?: number;
}

export const Breadcrumbs: React.FunctionComponent<BreadcrumbsProps> = ({
    hashPath,
    ignoreN = 0
}) => {
    const styles = useStyles();

    const crumbs = React.useMemo<Crumb[]>(() => {
        const paths = decodeURI(hashPath).split("/").filter(path => !!path);

        return [{ text: "", key: "home", href: "#/" },
        ...paths.map((path, idx) => {
            const href = idx < (paths.length - 1) ? `#/${paths.slice(0, idx + 1).join("/")}` : undefined;
            return { text: path.toUpperCase(), key: "" + idx, href };
        }).filter((row, idx) => idx >= ignoreN)];
    }, [hashPath, ignoreN]);

    return <Breadcrumb className={styles.breadcrumb} size="small">
        {crumbs.map((crumb, idx) => {
            const isLast = idx === crumbs.length - 1;
            return <React.Fragment key={crumb.key}>
                <BreadcrumbItem>
                    <BreadcrumbButton
                        className={styles.button}
                        href={crumb.href}
                        current={isLast}
                    >
                        {crumb.text}
                    </BreadcrumbButton>
                </BreadcrumbItem>
                {!isLast && <BreadcrumbDivider />}
            </React.Fragment>;
        })}
    </Breadcrumb>;
};
