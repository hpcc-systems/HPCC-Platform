#ifndef SARUNCMD_HPP
#define SARUNCMD_HPP

#ifdef SARUNCMD_API_EXPORTS
    #define SARUNCMD_API DECL_EXPORT
#else
    #define SARUNCMD_API DECL_IMPORT
#endif

extern SARUNCMD_API void runSashaCommand(SashaCommandAction action, INode *node, StringBuffer &outBuffer);

#endif