/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef ECLCMD_COMMON_HPP
#define ECLCMD_COMMON_HPP

#include "ws_workunits.hpp"

//=========================================================================================

interface IEclCommand : extends IInterface
{
    virtual bool parseCommandLineOptions(ArgvIterator &iter)=0;
    virtual bool finalizeOptions(IProperties *globals)=0;
    virtual int processCMD()=0;
    virtual void usage()=0;
};

typedef IEclCommand *(*EclCommandFactory)(const char *cmdname);

#define ECLOPT_HELP "--help"
#define ECLARG_HELP "help"

#define ECLOPT_SERVER "--server"
#define ECLOPT_SERVER_S "-s"
#define ECLOPT_SERVER_INI "eclWatchIP"
#define ECLOPT_SERVER_ENV "ECL_WATCH_IP"
#define ECLOPT_SERVER_DEFAULT "."
#define ECLOPT_SSL "--ssl"
#define ECLOPT_SSL_S "-ssl"

#define ECLOPT_PORT "--port"
#define ECLOPT_PORT_INI "eclWatchPort"
#define ECLOPT_PORT_ENV "ECL_WATCH_PORT"
#define ECLOPT_PORT_DEFAULT "8010"

#define ECLOPT_USERNAME "--username"
#define ECLOPT_USERNAME_S "-u"
#define ECLOPT_USERNAME_INI "eclUserName"
#define ECLOPT_USERNAME_ENV "ECL_USER_NAME"

#define ECLOPT_PASSWORD "--password"
#define ECLOPT_PASSWORD_S "-pw"
#define ECLOPT_PASSWORD_INI "eclPassword"
#define ECLOPT_PASSWORD_ENV "ECL_PASSWORD"

#define ECLOPT_NORELOAD "--no-reload"
#define ECLOPT_OVERWRITE "--overwrite"
#define ECLOPT_OVERWRITE_S "-O"
#define ECLOPT_OVERWRITE_INI "overwriteDefault"
#define ECLOPT_OVERWRITE_ENV NULL

#define ECLOPT_DONT_COPY_FILES "--no-files"

#define ECLOPT_ACTIVE "--active"
#define ECLOPT_ALL "--all"
#define ECLOPT_INACTIVE "--inactive"
#define ECLOPT_NO_ACTIVATE "--no-activate"
#define ECLOPT_ACTIVATE "--activate"
#define ECLOPT_ACTIVATE_S "-A"
#define ECLOPT_ACTIVATE_INI "activateDefault"
#define ECLOPT_ACTIVATE_ENV NULL
#define ECLOPT_SUSPEND_PREVIOUS "--suspend-prev"
#define ECLOPT_SUSPEND_PREVIOUS_S "-sp"
#define ECLOPT_SUSPEND_PREVIOUS_INI "suspendPrevDefault"
#define ECLOPT_SUSPEND_PREVIOUS_ENV "ACTIVATE_SUSPEND_PREVIOUS"
#define ECLOPT_DELETE_PREVIOUS "--delete-prev"
#define ECLOPT_DELETE_PREVIOUS_S "-dp"
#define ECLOPT_DELETE_PREVIOUS_INI "deletePrevDefault"
#define ECLOPT_DELETE_PREVIOUS_ENV "ACTIVATE_DELETE_PREVIOUS"

#define ECLOPT_MAIN "--main"
#define ECLOPT_MAIN_S "-main"  //eclcc compatible format
#define ECLOPT_SNAPSHOT "--snapshot"
#define ECLOPT_SNAPSHOT_S "-sn"
#define ECLOPT_ECL_ONLY "--ecl-only"

#define ECLOPT_WAIT "--wait"
#define ECLOPT_WAIT_INI "waitTimeout"
#define ECLOPT_WAIT_ENV "ECL_WAIT_TIMEOUT"

#define ECLOPT_TIME_LIMIT "--timeLimit"
#define ECLOPT_MEMORY_LIMIT "--memoryLimit"
#define ECLOPT_WARN_TIME_LIMIT "--warnTimeLimit"
#define ECLOPT_PRIORITY "--priority"
#define ECLOPT_COMMENT "--comment"

#define ECLOPT_CHECK_PACKAGEMAPS "--check-packagemaps"

#define ECLOPT_RESULT_LIMIT "--limit"
#define ECLOPT_RESULT_LIMIT_INI "resultLimit"
#define ECLOPT_RESULT_LIMIT_ENV "ECL_RESULT_LIMIT"

#define ECLOPT_INPUT "--input"
#define ECLOPT_INPUT_S "-in"

#define ECLOPT_NOROOT "--noroot"

#define ECLOPT_WUID "--wuid"
#define ECLOPT_WUID_S "-wu"
#define ECLOPT_CLUSTER_DEPRECATED "--cluster"
#define ECLOPT_CLUSTER_DEPRECATED_S "-cl"
#define ECLOPT_TARGET "--target"
#define ECLOPT_TARGET_S "-t"
#define ECLOPT_NAME "--name"
#define ECLOPT_NAME_S "-n"
#define ECLOPT_QUERYSET "--queryset"
#define ECLOPT_QUERYSET_S "-qs"
#define ECLOPT_VERSION "--version"
#define ECLOPT_SHOW "--show"
#define ECLOPT_PMID "--pmid"
#define ECLOPT_PMID_S "-pm"
#define ECLOPT_QUERYID "--queryid"


#define ECLOPT_DALIIP "--daliip"
#define ECLOPT_PROCESS "--process"
#define ECLOPT_PROCESS_S "-p"

#define ECLOPT_LIB_PATH_S "-L"
#define ECLOPT_IMP_PATH_S "-I"
#define ECLOPT_MANIFEST "--manifest"
#define ECLOPT_MANIFEST_DASH "-manifest"

#define ECLOPT_VERBOSE "--verbose"
#define ECLOPT_VERBOSE_S "-v"

bool isValidMemoryValue(const char *value);
bool isValidPriorityValue(const char *value);

bool extractEclCmdOption(StringBuffer & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix);
bool extractEclCmdOption(StringAttr & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix);
bool extractEclCmdOption(bool & option, IProperties * globals, const char * envName, const char * propertyName, bool defval);
bool extractEclCmdOption(unsigned & option, IProperties * globals, const char * envName, const char * propertyName, unsigned defval);

bool matchVariableOption(ArgvIterator &iter, const char prefix, IArrayOf<IEspNamedValue> &values);

enum eclObjParameterType
{
    eclObjTypeUnknown = 0x00,
    eclObjSource = 0x01,
    eclObjArchive = 0x02,
    eclObjSharedObject = 0x04,
    eclObjWuid = 0x08,
    eclObjQuery = 0x10,
    eclObjManifest = 0x20
};

#define eclObjSourceOrArchive (eclObjSource|eclObjArchive)

class EclObjectParameter
{
public:
    EclObjectParameter(unsigned _accept=0x00);
    eclObjParameterType set(const char *_value);
    const char *queryTypeName();
    StringBuffer &getDescription(StringBuffer &s);
    void loadStdIn();
    void loadFile();

    eclObjParameterType finalizeContentType();

    bool isElfContent();
    bool isPEContent();
    void ensureUtf8Content();

public:
    eclObjParameterType type;
    StringAttr value;
    StringAttr query;
    MemoryBuffer mb;
    unsigned accept;
};

enum eclCmdOptionMatchIndicator
{
    EclCmdOptionNoMatch=0,
    EclCmdOptionMatch=1,
    EclCmdOptionCompletion=2
};

class EclCmdCommon : public CInterface, implements IEclCommand
{
public:
    IMPLEMENT_IINTERFACE;
    EclCmdCommon() : optVerbose(false), optSSL(false)
    {
    }
    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt=false);
    virtual bool finalizeOptions(IProperties *globals);

    virtual void usage()
    {
        fprintf(stdout,
            "   --help                 display usage information for the given command\n"
            "   -v, --verbose          output additional tracing information\n"
            "   -s, --server=<ip>      ip of server running ecl services (eclwatch)\n"
            "   -ssl, --ssl            use SSL to secure the connection to the server\n"
            "   --port=<port>          ecl services port\n"
            "   -u, --username=<name>  username for accessing ecl services\n"
            "   -pw, --password=<pw>   password for accessing ecl services\n"
        );
    }
public:
    StringAttr optServer;
    StringAttr optPort;
    StringAttr optUsername;
    StringAttr optPassword;
    bool optVerbose;
    bool optSSL;
};

class EclCmdWithEclTarget : public EclCmdCommon
{
public:
    EclCmdWithEclTarget() : optNoArchive(false), optResultLimit((unsigned)-1)
    {
    }
    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt=false);
    virtual bool finalizeOptions(IProperties *globals);

    virtual void usage()
    {
        EclCmdCommon::usage();
        fprintf(stdout,
            "   --main=<definition>    definition to use from legacy ECL repository\n"
            "   --snapshot,-sn=<label> snapshot label to use from legacy ECL repository\n"
            "   --ecl-only             send ecl text to hpcc without generating archive\n"
            "   --limit=<limit>        sets the result limit for the query, defaults to 100\n"
            "   -f<option>[=value]     set an ECL option (equivalent to #option)\n"
            " eclcc options:\n"
            "   -Ipath                 Add path to locations to search for ecl imports\n"
            "   -Lpath                 Add path to locations to search for system libraries\n"
            "   --manifest             Specify path to manifest file\n"
        );
    }
public:
    StringAttr optTargetCluster;
    EclObjectParameter optObj;
    StringBuffer optLibPath;
    StringBuffer optImpPath;
    StringAttr optManifest;
    StringAttr optAttributePath;
    StringAttr optSnapshot;
    IArrayOf<IEspNamedValue> debugValues;
    unsigned optResultLimit;
    bool optNoArchive;
};

class EclCmdWithQueryTarget : public EclCmdCommon
{
public:
    EclCmdWithQueryTarget()
    {
    }
    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt=false);
    virtual bool finalizeOptions(IProperties *globals);
    virtual bool parseCommandLineOptions(ArgvIterator &iter);

    virtual void usage()
    {
        EclCmdCommon::usage();
    }
public:
    StringAttr optQuerySet;
    StringAttr optQuery;
};

void outputMultiExceptions(const IMultiException &me);

class EclCmdURL : public StringBuffer
{
public:
    EclCmdURL(const char *service, const char *ip, const char *port, bool ssl)
    {
        set("http");
        if (ssl)
            append('s');
        append("://").append(ip).append(':').append(port).append('/').append(service);
    }
};

template <class Iface> Iface *intClient(Iface *client, EclCmdCommon &cmd, const char *service)
{
    if(cmd.optServer.isEmpty())
        throw MakeStringException(-1, "Server IP not specified");

    EclCmdURL url(service, cmd.optServer, cmd.optPort, cmd.optSSL);
    client->addServiceUrl(url.str());
    if (cmd.optUsername.length())
        client->setUsernameToken(cmd.optUsername, cmd.optPassword, NULL);

    return client;
}

#define createCmdClient(SN, cmd) intClient<IClient##SN>(create##SN##Client(), cmd, #SN);

#endif
