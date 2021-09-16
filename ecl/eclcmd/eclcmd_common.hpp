/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "eclcc.hpp"

//=========================================================================================

enum eclCmdOptionMatchIndicator
{
    EclCmdOptionNoMatch=0,
    EclCmdOptionMatch=1,
    EclCmdOptionCompletion=2
};

interface IEclCommand : extends IInterface
{
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter)=0;
    virtual bool finalizeOptions(IProperties *globals)=0;
    virtual int processCMD()=0;
    virtual void usage()=0;
};

typedef IEclCommand *(*EclCommandFactory)(const char *cmdname);

#define ECLOPT_HELP "--help"
#define ECLARG_HELP "help"

#define ECLOPT_OUTPUT "--output"
#define ECLOPT_OUTPUT_S "-O"

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

#define ECLOPT_WAIT_CONNECT "--wait-connect"
#define ECLOPT_WAIT_READ "--wait-read"

#define ECLOPT_USERNAME "--username"
#define ECLOPT_USERNAME_S "-u"
#define ECLOPT_USERNAME_INI "eclUserName"
#define ECLOPT_USERNAME_ENV "ECL_USER_NAME"

#define ECLOPT_PASSWORD "--password"
#define ECLOPT_PASSWORD_S "-pw"
#define ECLOPT_PASSWORD_INI "eclPassword"
#define ECLOPT_PASSWORD_ENV "ECL_PASSWORD"

#define ECLOPT_NORELOAD "--no-reload"
#define ECLOPT_NOPUBLISH "--no-publish"
#define ECLOPT_OVERWRITE "--overwrite"
#define ECLOPT_REPLACE "--replace"
#define ECLOPT_OVERWRITE_S "-O"
#define ECLOPT_OVERWRITE_INI "overwriteDefault"
#define ECLOPT_OVERWRITE_ENV NULL

#define ECLOPT_DONT_COPY_FILES "--no-files"
#define ECLOPT_ALLOW_FOREIGN "--allow-foreign"

#define ECLOPT_ACTIVE "--active"
#define ECLOPT_ACTIVE_ONLY "--active-only"
#define ECLOPT_ALL "--all"
#define ECLOPT_INACTIVE "--inactive"
#define ECLOPT_NO_ACTIVATE "--no-activate"
#define ECLOPT_ACTIVATE "--activate"
#define ECLOPT_ACTIVATE_S "-A"
#define ECLOPT_ACTIVATE_INI "activateDefault"
#define ECLOPT_ACTIVATE_ENV NULL
#define ECLOPT_CLONE_ACTIVE_STATE "--clone-active-state"
#define ECLOPT_SUSPEND_PREVIOUS "--suspend-prev"
#define ECLOPT_SUSPEND_PREVIOUS_S "-sp"
#define ECLOPT_SUSPEND_PREVIOUS_INI "suspendPrevDefault"
#define ECLOPT_SUSPEND_PREVIOUS_ENV "ACTIVATE_SUSPEND_PREVIOUS"
#define ECLOPT_DELETE_PREVIOUS "--delete-prev"
#define ECLOPT_DELETE_PREVIOUS_S "-dp"
#define ECLOPT_DELETE_PREVIOUS_INI "deletePrevDefault"
#define ECLOPT_DELETE_PREVIOUS_ENV "ACTIVATE_DELETE_PREVIOUS"
#define ECLOPT_CHECK_DFS "--check-dfs"
#define ECLOPT_UPDATE_DFS "--update-dfs"
#define ECLOPT_GLOBAL_SCOPE "--global-scope"
#define ECLOPT_DELETE_FILES "--delete"
#define ECLOPT_DELETE_SUBFILES "--delete-subfiles"
#define ECLOPT_DELETE_RECURSIVE "--delete-recursive"
#define ECLOPT_RETRY "--retry"
#define ECLOPT_CHECK_ALL_NODES "--check-all-nodes"
#define ECLOPT_CHECK_ALL_NODES_INI "checkAllNodes"
#define ECLOPT_CHECK_ALL_NODES_ENV "CHECK_ALL_NODES"
#define ECLOPT_UPDATE_SUPER_FILES "--update-super-files"
#define ECLOPT_UPDATE_CLONE_FROM "--update-clone-from"
#define ECLOPT_DONT_APPEND_CLUSTER "--dont-append-cluster"
#define ECLOPT_PART_NAME "--part-name"
#define ECLOPT_PROTECT "--protect"
#define ECLOPT_USE_EXISTING "--use-existing"
#define ECLOPT_IGNORE_WARNINGS "--ignore-warnings"
#define ECLOPT_IGNORE_OPTIONAL "--ignore-optional"
#define ECLOPT_IGNORE_QUERIES "--ignore-queries"

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
#define ECLOPT_PRELOAD_ALL_PACKAGES "--preload-all"

#define ECLOPT_EXCEPTION_LEVEL "--exception-level"
#define ECLOPT_RESULT_LIMIT "--limit"
#define ECLOPT_RESULT_LIMIT_INI "resultLimit"
#define ECLOPT_RESULT_LIMIT_ENV "ECL_RESULT_LIMIT"

#define ECLOPT_INPUT "--input"
#define ECLOPT_INPUT_S "-in"

#define ECLOPT_NOROOT "--noroot"
#define ECLOPT_POLL "--poll"

#define ECLOPT_WUID "--wuid"
#define ECLOPT_WUID_S "-wu"
#define ECLOPT_CLUSTER_DEPRECATED "--cluster"
#define ECLOPT_CLUSTER_DEPRECATED_S "-cl"
#define ECLOPT_TARGET "--target"
#define ECLOPT_TARGET_S "-t"
#define ECLOPT_NAME "--name"
#define ECLOPT_NAME_S "-n"
#define ECLOPT_JOB_NAME "--job-name"
#define ECLOPT_QUERY_NAME "--query-name"
#define ECLOPT_QUERYSET "--queryset"
#define ECLOPT_QUERYSET_S "-qs"
#define ECLOPT_VERSION "--version"
#define ECLOPT_SHOW "--show"
#define ECLOPT_PMID "--pmid"
#define ECLOPT_PMID_S "-pm"
#define ECLOPT_QUERYID "--queryid"
#define ECLOPT_QUERIES "--queries"

#define ECLOPT_DALIIP "--daliip"
#define ECLOPT_PROCESS "--process"
#define ECLOPT_PROCESS_S "-p"
#define ECLOPT_SOURCE_PROCESS "--source-process"

#define ECLOPT_PATH "--path"
#define ECLOPT_INC_THOR_SLAVE_LOGS "--inc-thor-slave-logs"
#define ECLOPT_PROBLEM_DESC "--description"
#define ECLOPT_CREATE_DIRS "--create-dirs"


#define ECLOPT_LIB_PATH_S "-L"
#define ECLOPT_IMP_PATH_S "-I"
#define ECLOPT_MANIFEST "--manifest"
#define ECLOPT_MANIFEST_DASH "-manifest"
#define ECLOPT_LEGACY "--legacy"
#define ECLOPT_LEGACY_DASH "-legacy"
#define ECLOPT_CHECKDIRTY "-checkDirty"
#define ECLOPT_DEBUG "--debug"
#define ECLOPT_DEBUG_DASH "-g"
#define ECLOPT_FAST_SYNTAX "--fastsyntax"
#define ECLOPT_NO_STD_INC "--nostdinc"

#define ECLOPT_VERBOSE "--verbose"
#define ECLOPT_VERBOSE_S "-v"

#define ECLOPT_KEYUID "--keyuid"
#define ECLOPT_KEYPASS "--keypass"

const char *queryEclccPath(bool optVerbose);

bool extractEclCmdOption(StringBuffer & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix);
bool extractEclCmdOption(StringAttr & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix);
bool extractEclCmdOption(bool & option, IProperties * globals, const char * envName, const char * propertyName, bool defval);
bool extractEclCmdOption(unsigned & option, IProperties * globals, const char * envName, const char * propertyName, unsigned defval);

bool matchVariableOption(ArgvIterator &iter, const char prefix, IArrayOf<IEspNamedValue> &values, bool expandEclccOptions);
void addNamedValue(const char * name, const char * value, IArrayOf<IEspNamedValue> &values);

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

class EclCmdCommon : implements IEclCommand, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    EclCmdCommon(bool _usesESP=true) : optVerbose(false), optSSL(false), usesESP(_usesESP)
    {
    }
    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt=false);
    virtual bool finalizeOptions(IProperties *globals);

    virtual void usage()
    {
        fprintf(stdout,
            "   --help                 Display usage information for the given command\n"
            "   -v, --verbose          Output additional tracing information\n"
          );
        if (usesESP)
            fprintf(stdout,
                "   -s, --server=<ip>      IP of server running ecl services (eclwatch)\n"
                "   -ssl, --ssl            Use SSL to secure the connection to the server\n"
                "   --port=<port>          ECL services port\n"
                "   -u, --username=<name>  Username for accessing ecl services\n"
                "   -pw, --password=<pw>   Password for accessing ecl services\n"
                "   --wait-connect=<Ms>    Timeout while connecting to server (in milliseconds)\n"
                "   --wait-read=<Sec>      Timeout while reading from socket (in seconds)\n"
              );
    }
public:
    StringAttr optServer;
    StringAttr optPort;
    StringAttr optUsername;
    StringAttr optPassword;
    bool optPasswordProvided = false;
    unsigned optWaitConnectMs = 0;
    unsigned optWaitReadSec = 0;
    bool optVerbose;
    bool optSSL;
    bool usesESP;
};

class EclCmdWithEclTarget : public EclCmdCommon
{
public:
    EclCmdWithEclTarget() : optLegacy(false), optCheckDirty(false), optNoArchive(false), optResultLimit((unsigned)-1), optDebug(false), paramCount(0)
    {
    }
    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt=false);
    virtual bool finalizeOptions(IProperties *globals);
    bool setTarget(const char *target);
    bool setParam(const char *in, bool final);

    virtual void usage()
    {
        EclCmdCommon::usage();
        fprintf(stdout,
            "   --main=<definition>    Definition to use from legacy ECL repository\n"
            "   --snapshot,-sn=<label> Snapshot label to use from legacy ECL repository\n"
            "   --ecl-only             Send ECL query to HPCC as text rather than as a generated archive\n"
            "   --limit=<limit>        Sets the result limit for the query\n"
            "   -f<option>[=value]     Set an ECL language option (equivalent to #option)\n"
            "   -f-<option>[=value]    Set an eclcc command line option (single '-')\n"
            "   -f--<option>[=value]   Set an eclcc command line option (double '-')\n"
            "   -Dname=value           Override the definition of a global attribute 'name'\n"
            " eclcc options (everything following):\n"
        );
        for (unsigned line=0; line < _elements_in(helpText); line++)
        {
            const char * text = helpText[line];
            StringBuffer wsPrefix;
            if (*text == '?')
            {
                text = text+1;
                if (*text != ' ')
                    wsPrefix.append(' ');
            }
            else
                continue;
            if (*text == '!')
            {
                if (optVerbose)
                {
                    text = text+1;
                    fprintf(stdout, "%s%s\n", wsPrefix.str(), text);
                }
            }
            else
                fprintf(stdout, "%s%s\n", wsPrefix.str(), text);
        }
    }
public:
    StringAttr param;
    StringAttr optTargetCluster;
    EclObjectParameter optObj;
    StringBuffer optLibPath;
    StringBuffer optImpPath;
    StringAttr optManifest;
    StringAttr optAttributePath;
    StringAttr optSnapshot;
    IArrayOf<IEspNamedValue> debugValues;
    IArrayOf<IEspNamedValue> definitions;
    unsigned optResultLimit;
    unsigned paramCount;
    bool optNoArchive;
    bool optLegacy;
    bool optDebug;
    bool optCheckDirty;
    bool optFastSyntax = false;
    bool optNoStdInc = false;
};

class EclCmdWithQueryTarget : public EclCmdCommon
{
public:
    EclCmdWithQueryTarget()
    {
    }
    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt=false);
    virtual bool finalizeOptions(IProperties *globals);
    virtual eclCmdOptionMatchIndicator parseCommandLineOptions(ArgvIterator &iter);

    virtual void usage()
    {
        EclCmdCommon::usage();
    }
public:
    StringAttr optQuerySet;
    StringAttr optQuery;
};

void outputExceptionEx(IException &e);
int outputMultiExceptionsEx(const IMultiException &me);
bool checkMultiExceptionsQueryNotFound(const IMultiException &me);
bool outputQueryFileCopyErrors(IArrayOf<IConstLogicalFileError> &errors);


class EclCmdURL : public StringBuffer
{
public:
    EclCmdURL(const char *service, const char *ip, const char *port, bool ssl, const char *tail=NULL)
    {
        set("http");
        if (ssl)
            append('s');
        append("://").append(ip).append(':').append(port).append('/').append(service);
        if (tail)
            append(tail);
    }
};

template <class Iface> Iface *intClient(Iface *client, EclCmdCommon &cmd, const char *service, const char *urlTail)
{
    if(cmd.optServer.isEmpty())
        throw MakeStringException(-1, "Server IP not specified");

    EclCmdURL url(service, cmd.optServer, cmd.optPort, cmd.optSSL, urlTail);
    client->addServiceUrl(url.str());
    if (cmd.optUsername.length())
        client->setUsernameToken(cmd.optUsername, cmd.optPassword, NULL);

    return client;
}

#define createCmdClient(SN, cmd) intClient<IClient##SN>(create##SN##Client(), cmd, #SN, NULL);
#define createCmdClientExt(SN, cmd, urlTail) intClient<IClient##SN>(create##SN##Client(), cmd, #SN, urlTail);

inline void setCmdRequestTimeouts(IEspClientRpcSettings &rpc, unsigned waitMs, unsigned waitConnectMs, unsigned waitReadSec)
{
    if (waitMs==(unsigned)-1)
        waitMs=0;
    if (waitConnectMs || waitMs)
        rpc.setConnectTimeOutMs(waitConnectMs ? waitConnectMs : waitMs);
    if (waitReadSec || waitMs)
        rpc.setReadTimeOutSecs(waitReadSec ? waitReadSec : (waitMs / 1000));
}

#endif
