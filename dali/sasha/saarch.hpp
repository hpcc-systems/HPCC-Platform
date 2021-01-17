#ifndef SAARCH_HPP
#define SAARCH_HPP

interface ISashaServer;
interface ISashaCommand;

#ifndef _CONTAINERIZED
extern ISashaServer *createSashaArchiverServer();
#else
extern ISashaServer *createSashaWUArchiverServer(); 
extern ISashaServer *createSashaDFUWUArchiverServer(); 
extern ISashaServer *createSashaCachedWURemoverServer(); 
extern ISashaServer *createSashaDFURecoveryArchiverServer(); 
#endif

bool processArchiverCommand(ISashaCommand *cmd);

#endif
