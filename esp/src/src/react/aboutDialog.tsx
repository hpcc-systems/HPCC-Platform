import * as React from "react";
import { MuiThemeProvider } from "@material-ui/core/styles";
import Button from "@material-ui/core/Button";
import Dialog from "@material-ui/core/Dialog";
import DialogActions from "@material-ui/core/DialogActions";
import DialogContent from "@material-ui/core/DialogContent";
import DialogTitle from "@material-ui/core/DialogTitle";
import ToggleButton from "@material-ui/lab/ToggleButton";
import Typography from "@material-ui/core/Typography";
import Box from "@material-ui/core/Box";
import { Bar } from "@hpcc-js/chart";
import { fetchStats } from "../KeyValStore";
import nlsHPCC from "../nlsHPCC";
import { theme } from "./theme";
import { VisualizationComponent } from "./vizComponent";

interface BarChart {
    columns: string[];
    data: (string | number)[][];
}

export const BarChart: React.FunctionComponent<BarChart> = ({
    columns,
    data
}) => {
    return <VisualizationComponent widget={Bar} widgetProps={{ columns, data }} >
    </VisualizationComponent>;
};

function TabPanel(props) {
    const { children, value, index, ...other } = props;

    return (
        <div
            role="tabpanel"
            hidden={value !== index}
            id={`simple-tabpanel-${index}`}
            aria-labelledby={`simple-tab-${index}`}
            {...other}
        >
            {value === index && (
                <Box p={3}>
                    {children}
                </Box>
            )}
        </div>
    );
}

interface AboutDialog {
    version: string;
    handleClose: () => void;
}

export const AboutDialog: React.FunctionComponent<AboutDialog> = ({
    version,
    handleClose
}) => {
    const [page, setPage] = React.useState(0);
    const [columns] = React.useState([nlsHPCC.Client, nlsHPCC.Count]);
    const [browser, setBrowser] = React.useState([]);
    const [os, setOS] = React.useState([]);

    React.useEffect(() => {
        fetchStats().then(response => {
            setBrowser(response.browser);
            setOS(response.os);
        });
    }, []);

    return (
        <MuiThemeProvider theme={theme}>
            <Dialog
                open={true}
                onClose={handleClose}
                maxWidth="sm"
                fullWidth={true}
                aria-labelledby="alert-dialog-title"
                aria-describedby="alert-dialog-description"
            >
                <DialogTitle id="alert-dialog-title">{nlsHPCC.AboutHPCCSystems}</DialogTitle>
                <DialogContent>
                    <TabPanel value={page} index={0}>
                        <Typography align="center" >
                            <b>{nlsHPCC.Version}:  </b>{version}
                        </Typography>
                    </TabPanel>
                    <TabPanel value={page} index={1}>
                        <BarChart columns={columns} data={browser}>
                        </BarChart>
                    </TabPanel>
                    <TabPanel value={page} index={2}>
                        <BarChart columns={columns} data={os}>
                        </BarChart>
                    </TabPanel>
                </DialogContent>
                <DialogActions>
                    <ToggleButton selected={page === 1} value="check" onChange={() => {
                        setPage(page === 1 ? 0 : 1);
                    }}>{nlsHPCC.BrowserStats}</ToggleButton>
                    <ToggleButton selected={page === 2} value="check" onChange={() => {
                        setPage(page === 2 ? 0 : 2);
                    }}>{nlsHPCC.OSStats}</ToggleButton>
                    <Button onClick={handleClose} color="primary" autoFocus>Close</Button>
                </DialogActions>
            </Dialog>
        </MuiThemeProvider >
    );
};
