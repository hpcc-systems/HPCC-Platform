import * as React from "react";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import IconButton from "@material-ui/core/IconButton";
import MenuIcon from "@material-ui/icons/Menu";
import Container from "@material-ui/core/Container";
import Typography from "@material-ui/core/Typography";
import Box from "@material-ui/core/Box";
import { DojoComponent } from "./dojoComponent";

export function Frame() {
    const divStyle = {
        width: "100%",
        height: "800px"
    };
    return (
        <Container maxWidth="xl">
            <AppBar title="ECL Watch" position="static">
                <Toolbar>
                    <IconButton edge="start" color="inherit" aria-label="menu">
                        <MenuIcon />
                    </IconButton>
                    <Typography variant="h6">
                        ECL Watch
                    </Typography>
                </Toolbar>
            </AppBar>
            <Box my={4}>
                <div>
                    <h1>WU Queries</h1>
                    <div style={divStyle}>
                        <DojoComponent widget="WUQueryWidget" params={{}} />
                    </div>
                </div>
            </Box>
        </Container>
    );
}
