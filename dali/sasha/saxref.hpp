#ifndef SAXREFIF_HPP
#define SAXREFIF_HPP

#include "sautil.hpp"

interface ISashaCommand;
extern sashalib_decl ISashaServer *createSashaXrefServer();
extern sashalib_decl void processXRefRequest(ISashaCommand *cmd);
extern sashalib_decl ISashaServer *createSashaFileExpiryServer();
extern sashalib_decl void runExpiryCLI();


#endif
