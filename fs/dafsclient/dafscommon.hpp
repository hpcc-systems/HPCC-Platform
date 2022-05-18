/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef DAFSCOMMON_HPP
#define DAFSCOMMON_HPP

#define DAFILESRV_VERSION_MAJOR 2
#define DAFILESRV_VERSION_MINOR 6
#define MAJORMINOR(MAJOR, MINOR) MAJOR ## MINOR
#define DAFILESRV_VERSION_JOIN(X, Y) MAJORMINOR(X, Y)
#define DAFILESRV_VERSION DAFILESRV_VERSION_JOIN(DAFILESRV_VERSION_MAJOR, DAFILESRV_VERSION_MINOR)
#define TOVERSIONSTRING_JOIN(MAJOR, MINOR, OS) "DS V" #MAJOR "." #MINOR OS " "
#define TOVERSIONSTRING(MAJOR, MINOR, OS) TOVERSIONSTRING_JOIN(MAJOR, MINOR , OS)
#ifdef _WIN32
#define DAFILESRV_VERSIONSTRING TOVERSIONSTRING(DAFILESRV_VERSION_MAJOR, DAFILESRV_VERSION_MINOR, "Windows")
#else
#define DAFILESRV_VERSIONSTRING TOVERSIONSTRING(DAFILESRV_VERSION_MAJOR, DAFILESRV_VERSION_MINOR, "Linux")
#endif

const unsigned RFEnoerror = 0;

enum
{
    RFCopenIO,                                      // 0
    RFCcloseIO,
    RFCread,
    RFCwrite,
    RFCsize,
    RFCexists,
    RFCremove,
    RFCrename,
    RFCgetver,
    RFCisfile,
    RFCisdirectory,                                 // 10
    RFCisreadonly,
    RFCsetreadonly,
    RFCgettime,
    RFCsettime,
    RFCcreatedir,
    RFCgetdir,
    RFCstop,
    RFCexec,                                        // legacy cmd removed
    RFCdummy1,                                      // legacy placeholder
    RFCredeploy,                                    // 20
    RFCgetcrc,
    RFCmove,
// 1.5 features below
    RFCsetsize,
    RFCextractblobelements,
    RFCcopy,
    RFCappend,
    RFCmonitordir,
    RFCsettrace,
    RFCgetinfo,
    RFCfirewall,    // not used currently          // 30
    RFCunlock,
    RFCunlockreply,
    RFCinvalid,
    RFCcopysection,
// 1.7e
    RFCtreecopy,
// 1.7e - 1
    RFCtreecopytmp,
// 1.8
    RFCsetthrottle, // legacy version
// 1.9
    RFCsetthrottle2,
    RFCsetfileperms,
// 2.0
    RFCreadfilteredindex,    // No longer used     // 40
    RFCreadfilteredindexcount,
    RFCreadfilteredindexblob,
// 2.2
    RFCStreamRead,                                 // 43
// 2.4
    RFCStreamReadTestSocket,                       // 44
// 2.5
    RFCStreamGeneral,                              // 45
    RFCStreamReadJSON = '{',
// 2.6
    RFCFtSlaveCmd,
    RFCmaxnormal,
    RFCmax,
    RFCunknown = 255 // 0 would have been more sensible, but can't break backward compatibility
};

enum DAFS_ERROR_CODES {
    DAFSERR_connection_failed               = -1,
    DAFSERR_authenticate_failed             = -2,
    DAFSERR_protocol_failure                = -3,
    DAFSERR_serveraccept_failed             = -4,
    DAFSERR_serverinit_failed               = -5,
    DAFSERR_cmdstream_invalidexpiry         = -6,
    DAFSERR_cmdstream_authexpired           = -7,
    DAFSERR_cmdstream_unsupported_recfmt    = -8,
    DAFSERR_cmdstream_openfailure           = -9,
    DAFSERR_cmdstream_protocol_failure      = -10,
    DAFSERR_cmdstream_unauthorized          = -11,
    DAFSERR_cmdstream_unknownwritehandle    = -12,
    DAFSERR_cmdstream_generalwritefailure   = -13
};


typedef unsigned char RemoteFileCommandType;

inline MemoryBuffer & initSendBuffer(MemoryBuffer & buff)
{
    buff.setEndian(__BIG_ENDIAN);       // transfer as big endian...
    buff.append((unsigned)0);           // reserve space for length prefix
    return buff;
}

enum ThrottleClass
{
    ThrottleStd,
    ThrottleSlow,
    ThrottleClassMax
};

// RemoteFileServer throttling defaults
#define DEFAULT_THREADLIMIT 100
#define DEFAULT_THREADLIMITDELAYMS (60*1000)
#define DEFAULT_ASYNCCOPYMAX 10

#define DEFAULT_STDCMD_PARALLELREQUESTLIMIT 80
#define DEFAULT_STDCMD_THROTTLEDELAYMS 1000
#define DEFAULT_STDCMD_THROTTLECPULIMIT 85
#define DEFAULT_STDCMD_THROTTLEQUEUELIMIT 1000

#define DEFAULT_SLOWCMD_PARALLELREQUESTLIMIT 20
#define DEFAULT_SLOWCMD_THROTTLEDELAYMS 5000
#define DEFAULT_SLOWCMD_THROTTLECPULIMIT 75
#define DEFAULT_SLOWCMD_THROTTLEQUEUELIMIT 1000


#endif // DAFSCOMMON_HPP
