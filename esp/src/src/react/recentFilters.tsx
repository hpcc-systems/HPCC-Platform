import * as React from "react";
import Skeleton from '@material-ui/lab/Skeleton';
import { Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Button, Typography, Paper } from "@material-ui/core";
import { ThemeProvider } from "@material-ui/core/styles";
import Tooltip from '@material-ui/core/Tooltip';
import nlsHPCC from "../nlsHPCC";
import { theme } from './theme';
import { useGet } from "./hooks/useWsStore";

interface RecentFilterProps {
    ws_key: string;
    widget: {NewPage};
    filter: object;
}

export const RecentFilters: React.FunctionComponent<RecentFilterProps> = ({
    ws_key, widget, filter
}) => {
    const {data, loading} = useGet(ws_key, filter);

    const handleClick = (e) => {
        const tempObj = JSON.parse(e.currentTarget.value);
        widget.NewPage.onClick(tempObj);
    }

    const cleanUpFilter = (value:string) => {
        const result = value.replace(/[{}'"]+/g, '');
        return result;
    }

    return (
        <>
            <Typography variant="h4" noWrap>
                {nlsHPCC.RecentFilters}
            </Typography>
            { loading ? (<div><Skeleton /><Skeleton animation={false} /><Skeleton animation="wave" /></div>) : ( data ?
                <ThemeProvider theme={theme}>
                <TableContainer component={Paper}>
                    <Table aria-label={nlsHPCC.RecentFiltersTable} size="small">
                        <TableHead>
                            <TableRow>
                                <TableCell align="left">{nlsHPCC.FilterDetails}</TableCell>
                                <TableCell align="center">{nlsHPCC.OpenInNewPage}</TableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {data.map((row, idx) => (
                                <TableRow key={idx}>
                                    <Tooltip arrow title={cleanUpFilter(JSON.stringify(row))} placement="bottom"><TableCell align="left">{cleanUpFilter(JSON.stringify(row))}</TableCell></Tooltip>
                                    <TableCell align="center"><Button size="small" variant="contained" value={JSON.stringify(row)} color="primary" onClick={handleClick}>Open</Button></TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </TableContainer>
            </ThemeProvider> : <Typography variant="subtitle1">{nlsHPCC.NoRecentFiltersFound}</Typography> ) }
        </>
    )
};