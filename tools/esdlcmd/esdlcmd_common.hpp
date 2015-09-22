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

#define ESDL_CONVERT_SOURCE             "--source"
#define ESDL_CONVERT_OUTDIR             "--outdir"

#define ESDL_PROCESS_INCLUDES           "--includes"
#define ESDL_CONVERT_ALL                "--all"
#define ESDL_CONVERT_EXPANDEDXML        "--expandedxml"
#define ESDL_CONVERT_EXPANDEDXML_x      "-x"

#define HPCC_COMPONENT_FILES_DIR        "--compfilesdir"
#define HPCC_COMPONENT_FILES_DIR_CDE    "--CDE"

#define ESDLOPT_XSLT_PATH               "--xslt"

#define ESDLOPT_VERSION                 "--version"
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

#define ESDLOPT_WSDL_ADDRESS            "--wsdl-address"

#define ESDLBINDING_URN_BASE            "urn:hpccsystems:ws"
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


bool matchVariableOption(ArgvIterator &iter, const char prefix, IArrayOf<IEspNamedValue> &values);

enum esdlCmdOptionMatchIndicator
{
    EsdlCmdOptionNoMatch=0,
    EsdlCmdOptionMatch=1,
    EsdlCmdOptionCompletion=2
};

class EsdlCmdCommon : public CInterface, implements IEsdlCommand
{
public:
    IMPLEMENT_IINTERFACE;
    EsdlCmdCommon() : optVerbose(false)
    {}
    virtual esdlCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt=false);
    virtual bool finalizeOptions(IProperties *globals);

    virtual void usage()
    {
        fprintf(stdout,
            "   --help                 display usage information for the given command\n"
            "   -v,--verbose          output additional tracing information\n"
        );
    }
public:
    bool optVerbose;
};

class EsdlCmdHelper : public CInterface
{
public:
    Owned<IEsdlDefinition> esdlDef;
    Owned<IEsdlDefinitionHelper> defHelper;
    Owned<IFile> serviceDefFile;

public:
    EsdlCmdHelper()
    {
        esdlDef.set(createEsdlDefinition());
        defHelper.set(createEsdlDefinitionHelper());
    }

    IMPLEMENT_IINTERFACE;

    static EsdlCmdHelper * createEsdlHelper()
    {
        return new EsdlCmdHelper();
    }

    void loadDefinition(const char * sourceFileName, const char * serviceName, double version)
    {
        if (!esdlDef.get())
            esdlDef.set(createEsdlDefinition());

        if(!esdlDef->hasFileLoaded(sourceFileName))
        {
            StringBuffer extension;
            StringBuffer filename;
            splitFilename(sourceFileName, NULL, NULL, &filename, &extension);
            if (stricmp(extension.str(),LEGACY_FILE_EXTENSION)==0 || stricmp(extension.str(),ESDL_FILE_EXTENSION)==0)
            {
                StringBuffer esxml;
                EsdlCmdHelper::convertECMtoESXDL(sourceFileName, filename.str(), esxml, true, true, false, true);
                esdlDef->addDefinitionFromXML(esxml, serviceName, (int)version);
            }
            else
            {
                loadEsdlDef(sourceFileName);
            }
        }
    }

    void getServiceESXDL(const char * sourceFileName, const char * serviceName, StringBuffer & xmlOut, double version, IProperties *opts=NULL, unsigned flags=0)
    {
        loadDefinition(sourceFileName, serviceName, version);

        if (esdlDef)
        {
            IEsdlDefObjectIterator *deps = esdlDef->getDependencies(serviceName, "", version, opts, flags);

            if( deps )
            {
                xmlOut.appendf( "<esxdl name=\"%s\">", serviceName);
                defHelper->toXML( *deps, xmlOut, version, opts, flags );
                xmlOut.append("</esxdl>");
            }
            else
                throw( MakeStringException(0, "Could not get ESDL structure") );
        }
    }

    static void convertECMtoESXDL(const char * filepath, const char * esxdlname, StringBuffer & esxml, bool recursive, bool verbose, bool outputincludes, bool isIncludedESDL)
    {
        if (verbose)
            fprintf(stdout,"Converting ESDL file %s to XML\n", filepath);

        Owned<Esdl2Esxdl> cmd = new Esdl2Esxdl(recursive, verbose);
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

        VStringBuffer url("http://%s:%s/WsESDLConfig", server, port);

        IClientWsESDLConfig * esdlConfigClient = createWsESDLConfigClient();
        esdlConfigClient->addServiceUrl(url.str());
        esdlConfigClient->setUsernameToken(username, password, NULL);

        return esdlConfigClient;
    }

protected:

    void loadEsdlDef(const char * sourceFileName)
    {
        serviceDefFile.setown( createIFile(sourceFileName) );
        if( serviceDefFile->exists() )
        {
            if( serviceDefFile->isFile() )
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
        puts("   --outdir=<out dir path> Location to generate output\n");
    }

public:
    StringAttr optSource;
    StringAttr optOutDirPath;
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
    {
        path.appendf("%c%s%c%s", PATHSEPCHAR, HIGHER_DIR_RELATIVE,PATHSEPCHAR,COMPONENTS_DIR_NAME);
        return true;
    }

    return false;
}
#endif
