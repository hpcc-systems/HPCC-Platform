#ifndef SAARCH_HPP
#define SAARCH_HPP

#include "sautil.hpp"

#ifndef _CONTAINERIZED
extern sashalib_decl ISashaServer *createSashaArchiverServer();
#else
extern sashalib_decl ISashaServer *createSashaWUArchiverServer();
extern sashalib_decl ISashaServer *createSashaDFUWUArchiverServer();
extern sashalib_decl ISashaServer *createSashaCachedWURemoverServer();
extern sashalib_decl ISashaServer *createSashaDFURecoveryArchiverServer();
#endif

interface ISashaCommand;
extern sashalib_decl bool processArchiverCommand(ISashaCommand *cmd);

#endif
