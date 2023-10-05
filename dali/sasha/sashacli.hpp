#ifndef SASHACLI_HPP
#define SASHACLI_HPP

#ifdef SASHACLI_API_EXPORTS
    #define SASHACLI_API DECL_EXPORT
#else
    #define SASHACLI_API DECL_IMPORT
#endif

#include "dautils.hpp"

extern SASHACLI_API void runSashaCommand(SocketEndpoint ep, ISashaCommand *cmd, IFileIOStream *outfile, StringBuffer &outBuffer, bool viaESP);

#endif
