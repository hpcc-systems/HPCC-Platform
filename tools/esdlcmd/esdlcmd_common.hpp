/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#ifndef ESDLCMD_COMMON_HPP
#define ESDLCMD_COMMON_HPP

#include "jprop.hpp"
#include "jargv.hpp"
#include "common.hpp"
#include "esdl_def.hpp"
#include "esdl_def_helper.hpp"

#include "ws_esdlconfig_esp.ipp"
#include "esdl2xml.hpp"

#include <algorithm>
#include <list>

#define COMPONENTS_DIR_NAME "componentfiles"
#define HIGHER_DIR_RELATIVE ".."

//=========================================================================================

interface IEsdlCommand : extends IInterface
{
    virtual bool parseCommandLineOptions(ArgvIterator &iter)=0;
    virtual bool finalizeOptions(IProperties *globals)=0;
    virtual int processCMD()=0;
    virtual void usage()=0;
};

typedef IEsdlCommand *(*EsdlCommandFactory)(const char *cmdname);

#define ESDLOPT_HELP                    "--help"
#define ESDLOPT_HELP_s                  "--?"
#define ESDLARG_HELP                    "help"

#define ESDL_OPTION_VERBOSE              "--verbose"
#define ESDL_OPT_VERBOSE                 "-v"

#define ESDL_OPTION_TRACE_CATEGORY      "--trace-category"
#define ESDL_OPT_TRACE_CATEGORY         "-tcat"
#define ESDL_TRACE_CATEGORY_IERROR      "ie"
#define ESDL_TRACE_CATEGORY_OERROR      "oe"
#define ESDL_TRACE_CATEGORY_UERROR      "ue"
#define ESDL_TRACE_CATEGORY_IWARNING    "iw"
#define ESDL_TRACE_CATEGORY_OWARNING    "ow"
#define ESDL_TRACE_CATEGORY_UWARNING    "uw"
#define ESDL_TRACE_CATEGORY_DPROGRESS   "dp"
#define ESDL_TRACE_CATEGORY_OPROGRESS   "op"
#define ESDL_TRACE_CATEGORY_UPROGRESS   "up"
#define ESDL_TRACE_CATEGORY_DINFO       "di"
#define ESDL_TRACE_CATEGORY_OINFO       "oi"
#define ESDL_TRACE_CATEGORY_UINFO       "ui"
#define ESDL_TRACE_CATEGORY_DEVELOPER   "dev"
#define ESDL_TRACE_CATEGORY_OPERATOR    "admin"
#define ESDL_TRACE_CATEGORY_USER        "user"
#define ESDL_TRACE_CATEGORY_ERROR       "err"
#define ESDL_TRACE_CATEGORY_WARNING     "warn"
#define ESDL_TRACE_CATEGORY_PROGRESS    "prog"
#define ESDL_TRACE_CATEGORY_INFO        "info"

#define ESDL_CONVERT_SOURCE             "--source"
#define ESDL_CONVERT_OUTDIR             "--outdir"

#define ESDL_PROCESS_INCLUDES           "--includes"
#define ESDL_CONVERT_ALL                "--all"
#define ESDL_CONVERT_EXPANDEDXML        "--expandedxml"
#define ESDL_CONVERT_EXPANDEDXML_x      "-x"

#define HPCC_COMPONENT_FILES_DIR_CDE    "-cde"

#define ESDLOPT_XSLT_PATH               "--xslt"

#define ESDLOPT_VERSION                 "--version"
#define ESDLOPT_INTERFACE_VERSION       "--interface-version"
#define ESDLOPT_INTERFACE_VERSION_S     "-iv"
#define ESDLOPT_SERVICE                 "--service"
#define ESDLOPT_METHOD                  "--method"
#define ESDLOPT_PREPROCESS_OUT          "--preprocess-output"
#define ESDLOPT_ANNOTATE                "--annotate"
#define ESDLOPT_NO_OPTIONAL_ATTRIBUTES  "--noopt"
#define ESDLOPT_OPT_PARAM_VAL           "-opt"
#define ESDLOPT_OPTIONAL_PARAM_VAL      "--optional"
#define ESDLOPT_TARGET_NS               "-tns"
#define ESDLOPT_TARGET_NAMESPACE        "--target-namespace"
#define ESDLOPT_NUMBER                  "-n"
#define ESDLOPT_NO_COLLAPSE             "--show-inheritance"
#define ESDLOPT_NO_ARRAYOF              "--no-arrayof"
#define ESDLOPT_OUTPUT_CATEGORIES       "--output-categories"
#define ESDLOPT_USE_UTF8_STRINGS        "--utf8"
#define ESDLOPT_NO_EXPORT               "--no-export"
#define ESDLOPT_HIDE_GETDATAFROM        "--hide-get-data-from"
#define ESDLOPT_WSDL_ADDRESS            "--wsdl-address"
#define ESDLOPT_UNVERSIONED_NAMESPACE   "--unversioned-ns"
#define ESDLOPT_UNVERSIONED_NAMESPACE_S "-uvns"

#define DEFAULT_NAMESPACE_BASE          "urn:hpccsystems:ws"
#define ESDLOPTLIST_DELIMITER           ";"

#define ESDL_OPT_SERVICE_SERVER         "-s"
#define ESDL_OPTION_SERVICE_SERVER      "--server"
#define ESDL_OPTION_SERVICE_PORT        "--port"
#define ESDL_OPT_SERVICE_USER           "-u"
#define ESDL_OPTION_SERVICE_USER        "--username"
#define ESDL_OPTION_SERVICE_PORT        "--port"
#define ESDL_OPT_SERVICE_PORT           "-p"
#define ESDL_OPT_SERVICE_PASS           "-pw"
#define ESDL_OPTION_SERVICE_PASS        "--password"

#define ESDL_OPTION_ESP_PROC_NAME        "--esp-proc-name"
#define ESDL_OPT_ESP_PROC_NAME           "-epn"
#define ESDL_OPTION_TARGET_ESP_ADDRESS   "--esp-ip"
#define ESDL_OPTION_TARGET_ESP_PORT      "--esp-port"
#define ESDL_OPTION_CONFIG              "--config"
#define ESDL_OPTION_OVERWRITE           "--overwrite"
#define ESDL_OPTION_ROLLUP              "--rollup"
#define ESDL_OPTION_ECL_INCLUDE_LIST    "--ecl-imports"
#define ESDL_OPTION_ECL_HEADER_BLOCK    "--ecl-header"
#define ESDL_OPTION_ENCODED             "--encoded"

#define ESDL_OPTION_CASSANDRA_CONSISTENCY  "--cassandra-consistency"

#define ESDLOPT_INCLUDE_PATH            "--include-path"
#define ESDLOPT_INCLUDE_PATH_S          "-I"
#define ESDLOPT_INCLUDE_PATH_ENV        "ESDL_INCLUDE_PATH"
#define ESDLOPT_INCLUDE_PATH_INI        "esdlIncludePath"
#define ESDLOPT_INCLUDE_PATH_USAGE      "   -I, --include-path <include path>    Locations to look for included esdl files\n"

bool matchVariableOption(ArgvIterator &iter, const char prefix, IArrayOf<IEspNamedValue> &values);

enum esdlCmdOptionMatchIndicator
{
    EsdlCmdOptionNoMatch=0,
    EsdlCmdOptionMatch=1,
    EsdlCmdOptionCompletion=2
};

class EsdlCmdReporter : public EsdlDefReporter
{
protected:
    void reportSelf(Flags flag, const char* component, const char* level, const char* msg) const override
    {
        fprintf(stdout, "%s [%s]: %s\n", level, component, msg);
    }
};

class EsdlCmdCommon : public CInterface, implements IEsdlCommand
{
public:
    using TraceFlags = IEsdlDefReporter::Flags;
    enum : TraceFlags
    {
        defaultSuccinctTraceFlags = IEsdlDefReporter::ReportErrorClass | IEsdlDefReporter::ReportWarningClass,
        defaultVerboseTraceFlags = defaultSuccinctTraceFlags | IEsdlDefReporter::ReportProgressClass | IEsdlDefReporter::ReportInfoClass,
    };
    IMPLEMENT_IINTERFACE;
    EsdlCmdCommon() : optVerbose(false)
    {}
    virtual esdlCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt=false);
    virtual bool finalizeOptions(IProperties *globals);

    virtual void usage()
    {
        fprintf(stdout,
            "   --help                               Display usage information for the given command\n"
            "   -v,--verbose                         Output additional tracing information\n"
            "   -tcat,--trace-category <flags>       Control which debug messages are output; a case-insensitive comma-delimited combination of:\n"
            "                                            " ESDL_TRACE_CATEGORY_DEVELOPER ": all output for the developer audience\n"
            "                                            " ESDL_TRACE_CATEGORY_OPERATOR ": all output for the operator audience\n"
            "                                            " ESDL_TRACE_CATEGORY_USER ": all output for the user audience\n"
            "                                            " ESDL_TRACE_CATEGORY_ERROR ": all error output\n"
            "                                            " ESDL_TRACE_CATEGORY_WARNING ": all warning output\n"
            "                                            " ESDL_TRACE_CATEGORY_PROGRESS ": all progress output\n"
            "                                            " ESDL_TRACE_CATEGORY_INFO ": all info output\n"
            "                                        Errors and warnings are enabled by default if not verbose, and all are enabled when verbose."
            "                                        Use an empty <flags> value to disable all."
        );
    }
    virtual void outputWsStatus(int code, const char * message)
    {
        fprintf(code == 0 ? stdout : stderr, "\n %s.\n", message);
    }
public:
    inline TraceFlags optTraceFlags() { return (m_optTraceFlagsGiven ? m_actualTraceFlags : (optVerbose ? m_verboseTraceFlags : m_succinctTraceFlags)) | IEsdlDefReporter::ReportMethod; }
    bool optVerbose;
protected:
    void parseTraceFlags(const char* traceCategories);
    TraceFlags m_succinctTraceFlags = defaultSuccinctTraceFlags;
    TraceFlags m_verboseTraceFlags = defaultVerboseTraceFlags;
private:
    TraceFlags m_actualTraceFlags = 0;
    bool m_optTraceFlagsGiven = false;
};

class EsdlCmdHelper : public CInterface
{
    static IEsdlDefReporter* makeCmdReporter() { return new EsdlCmdReporter(); }
public:
    Owned<IEsdlDefinition> esdlDef;
    Owned<IEsdlDefinitionHelper> defHelper;
    Owned<IFile> serviceDefFile;
    bool verbose = false;

public:
    EsdlCmdHelper()
    {
        esdlDef.set(createEsdlDefinition(nullptr, makeCmdReporter));
        defHelper.set(createEsdlDefinitionHelper());
    }

    IMPLEMENT_IINTERFACE;

    static EsdlCmdHelper * createEsdlHelper()
    {
        return new EsdlCmdHelper();
    }

    void loadDefinition(const char * sourceFileName, const char * serviceName, double version, const char* includePath, IEsdlDefReporter::Flags traceFlags)
    {
        if (!esdlDef.get())
            esdlDef.set(createEsdlDefinition(nullptr, makeCmdReporter));
        IEsdlDefReporter* reporter = esdlDef->queryReporter();
        reporter->setFlags(traceFlags, true);

        if(!esdlDef->hasFileLoaded(sourceFileName))
        {
            StringBuffer extension;
            StringBuffer filename;
            splitFilename(sourceFileName, NULL, NULL, &filename, &extension);
            if (stricmp(extension.str(),LEGACY_FILE_EXTENSION)==0 || stricmp(extension.str(),ESDL_FILE_EXTENSION)==0)
            {
                StringBuffer esxml;
                EsdlCmdHelper::convertECMtoESXDL(sourceFileName, filename.str(), esxml, true, verbose, false, true, includePath);
                esdlDef->addDefinitionFromXML(esxml, sourceFileName);
            }
            else
            {
                loadEXSDLFromFile(sourceFileName);
            }
        }
    }

    void getServiceESXDL(const char * sourceFileName, const char * serviceName, StringBuffer & xmlOut, double version, IProperties *opts=nullptr, unsigned flags=0, const char* includePath=nullptr, IEsdlDefReporter::Flags traceFlags = EsdlCmdCommon::defaultSuccinctTraceFlags)
    {
        loadDefinition(sourceFileName, serviceName, version, includePath, traceFlags);

        if (esdlDef)
        {
            IEsdlDefObjectIterator *deps = esdlDef->getDependencies(serviceName, "", version, opts, flags);

            if( deps )
            {
                if (serviceName)
                    xmlOut.appendf("<esxdl name=\"%s\">", serviceName);
                else
                    xmlOut.appendf("<esxdl>");
                defHelper->toXML( *deps, xmlOut, version, opts, flags );
                xmlOut.append("</esxdl>");
            }
            else
                throw( MakeStringException(0, "Could not get ESDL structure") );
        }
    }

    static void convertECMtoESXDL(const char * filepath, const char * esxdlname, StringBuffer & esxml, bool recursive, bool verbose, bool outputincludes, bool isIncludedESDL, const char* includePath)
    {
        if (verbose)
            fprintf(stdout,"Converting ESDL file %s to XML\n", filepath);

        Owned<Esdl2Esxdl> cmd = new Esdl2Esxdl(recursive, verbose);
        cmd->setIncluePath(includePath);
        esxml.setf( "<esxdl name=\"%s\">", esxdlname);
        cmd->transform(filepath, "", &esxml, outputincludes, isIncludedESDL); //output to buffer
        esxml.append("</esxdl>");
    }

    static void outputMultiExceptions(const IMultiException &me)
    {
        fprintf(stderr, "\nException(s):\n");
        aindex_t count = me.ordinality();
        for (aindex_t i=0; i<count; i++)
        {
            IException& e = me.item(i);
            StringBuffer msg;
            fprintf(stderr, "%d: %s\n", e.errorCode(), e.errorMessage(msg).str());
        }
        fprintf(stderr, "\n");
    }

    static IClientWsESDLConfig * getWsESDLConfigSoapService(const char *server, const char *port, const char *username, const char *password)
    {
        if(server == NULL)
            throw MakeStringException(-1, "Server url not specified");

        VStringBuffer url("http://%s:%s/WsESDLConfig/?ver_=%s", server, port, VERSION_FOR_ESDLCMD);

        IClientWsESDLConfig * esdlConfigClient = createWsESDLConfigClient();
        esdlConfigClient->addServiceUrl(url.str());
        esdlConfigClient->setUsernameToken(username, password, NULL);

        return esdlConfigClient;
    }

protected:

    void loadEXSDLFromFile(const char * sourceFileName)
    {
        serviceDefFile.setown( createIFile(sourceFileName) );
        if( serviceDefFile->exists() )
        {
            if( serviceDefFile->isFile()==fileBool::foundYes )
            {
                if( serviceDefFile->size() > 0 )
                {
                    // Realized a subtle source of potential problems. Because there
                    // can be multiple EsdlStruct definitions with the same name
                    // in multiple files, you need to be careful that only those files
                    // explicitly included by your service are loaded to the
                    // EsdlDefinition object that you'll getDependencies() on. If not,
                    // you could inadvertently getDependencies() from a different structure
                    // with the same name. This means we can only reliably process one
                    // Web Service at a time, and must load files by explicitly loading
                    // only the top-level ws_<service> definition file, and allowing the
                    // load code to handle loading only the minimal set of required includes
                    esdlDef->addDefinitionsFromFile( serviceDefFile->queryFilename() );
                }
                else
                {
                    throw( MakeStringException(0, "ESDL definition file source %s is empty", sourceFileName) );
                }

            }
            else
            {
                throw( MakeStringException(0, "ESDL definition file source %s is not a file", sourceFileName) );
            }
        }
        else
        {
            throw( MakeStringException(0, "ESDL definition file source %s does not exist", sourceFileName) );
        }
    }
};

class EsdlConvertCmd : public EsdlCmdCommon
{
public:
    EsdlConvertCmd() {}
    virtual esdlCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt=false);
    virtual bool finalizeOptions(IProperties *globals);
    virtual bool parseCommandLineOptions(ArgvIterator&);
    virtual bool parseCommandLineOption(ArgvIterator&);
    virtual void usage()
    {
        EsdlCmdCommon::usage();
        puts("   --outdir=<out dir path>              Location to generate output\n");
    }

public:
    StringAttr optSource;
    StringAttr optOutDirPath;
    StringBuffer optIncludePath;
};

class EsdlHelperConvertCmd : public EsdlConvertCmd
{
protected:
    EsdlCmdHelper cmdHelper;
public:
    EsdlHelperConvertCmd()
    {
    }

    virtual void usage()
    {
        EsdlConvertCmd::usage();
    }
    virtual void doTransform(IEsdlDefObjectIterator& objs, StringBuffer &target, double version=0, IProperties *opts=NULL, const char *ns=NULL, unsigned flags=0 )=0;
    virtual void loadTransform( StringBuffer &xsltpath, IProperties *params )=0;
    virtual void setTransformParams(IProperties *params )=0;
};

static bool getCurrentFolder(StringBuffer & path)
{
    StringBuffer folder;
    splitDirTail(queryCurrentProcessPath(), folder);
    removeTrailingPathSepChar(folder);
    if (folder.length())
    {
        path = folder;
        return true;
    }
    return false;
}

static bool getComponentFilesRelPathFromBin(StringBuffer & path)
{
    if (getCurrentFolder(path))
        return checkDirExists(path.appendf("%c%s%c%s", PATHSEPCHAR, HIGHER_DIR_RELATIVE,PATHSEPCHAR,COMPONENTS_DIR_NAME));
    return false;
}

void saveAsFile(const char * path, const char *filename, const char *text, const char *ext="");

#endif
