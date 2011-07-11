#ifndef SAXREFIF_HPP
#define SAXREFIF_HPP

interface ISashaServer;
interface ISashaCommand;
extern ISashaServer *createSashaXrefServer();
extern void processXRefRequest(ISashaCommand *cmd);
extern ISashaServer *createSashaFileExpiryServer();

#endif
