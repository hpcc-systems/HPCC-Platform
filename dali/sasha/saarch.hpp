#ifndef SAARCH_HPP
#define SAARCH_HPP

interface ISashaServer;
interface ISashaCommand;
extern ISashaServer *createSashaArchiverServer(); 

bool processArchiverCommand(ISashaCommand *cmd);

#endif
