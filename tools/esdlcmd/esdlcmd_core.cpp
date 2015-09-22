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

#include <stdio.h>
#include "jlog.hpp"
#include "jfile.hpp"
#include "jargv.hpp"
#include "build-config.h"

#include "esdlcmd_common.hpp"
#include "esdlcmd_core.hpp"

#include "esdl2ecl.cpp"
#include "esdl-publish.cpp"

class Esdl2XSDCmd : public EsdlHelperConvertCmd
{
public:
    Esdl2XSDCmd() : optVersion(0), optAllAnnot(false), optNoAnnot(false),
                    optEnforceOptional(true), optRawOutput(false), optXformTimes(1), optFlags(DEPFLAG_COLLAPSE|DEPFLAG_ARRAYOF),
                    outfileext(".xsd")
    {}

    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {

        if (iter.done())
        {
            usage();
            return false;
        }

        //First two parameters' order is fixed.
        for (int par = 0; par < 2 && !iter.done(); par++)
        {
            const char *arg = iter.query();
            if (*arg != '-')
            {
                if (optSource.isEmpty())
                    optSource.set(arg);
                else if (optService.isEmpty())
                    optService.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument detected before required parameters: %s\n", arg);
                    usage();
                    return false;
                }
            }
            else
            {
                fprintf(stderr, "\noption detected before required parameters: %s\n", arg);
                usage();
                return false;
            }

            iter.next();
        }

        for (; !iter.done(); iter.next())
        {
            if (parseCommandLineOption(iter))
                continue;

            if (matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
                return false;
        }

        return true;
    }

    virtual bool parseCommandLineOption(ArgvIterator &iter)
    {
        if (iter.matchOption(optVersionStr, ESDLOPT_VERSION))
            return true;
        if (iter.matchOption(optService, ESDLOPT_SERVICE))
            return true;
        if (iter.matchOption(optMethod, ESDLOPT_METHOD))
            return true;
        if (iter.matchOption(optXsltPath, ESDLOPT_XSLT_PATH))
            return true;
        if (iter.matchOption(optPreprocessOutputDir, ESDLOPT_PREPROCESS_OUT))
            return true;
        if (iter.matchOption(optAnnotate, ESDLOPT_ANNOTATE))
            return true;
        if (iter.matchOption(optTargetNamespace, ESDLOPT_TARGET_NAMESPACE) || iter.matchOption(optTargetNamespace, ESDLOPT_TARGET_NS))
            return true;
        if (iter.matchOption(optOptional, ESDLOPT_OPT_PARAM_VAL) || iter.matchOption(optOptional, ESDLOPT_OPTIONAL_PARAM_VAL))
            return true;
        if (iter.matchFlag(optEnforceOptional, ESDLOPT_NO_OPTIONAL_ATTRIBUTES))
            return true;
        if (iter.matchOption(optXformTimes, ESDLOPT_NUMBER))
            return true;
        if (iter.matchFlag(optNoCollapse, ESDLOPT_NO_COLLAPSE))
            return true;
        if (iter.matchFlag(optNoArrayOf, ESDLOPT_NO_ARRAYOF))
            return true;

        if (EsdlConvertCmd::parseCommandLineOption(iter))
            return true;

        return false;
    }

    esdlCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
    {
        return EsdlConvertCmd::matchCommandLineOption(iter, true);
    }

    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optSource.isEmpty())
        {
            usage();
            throw( MakeStringException(0, "\nError: Source esdl parameter required\n"));
        }

        if( optService.isEmpty() )
        {
            usage();
            throw( MakeStringException(0, "A service name must be provided") );
        }

        if (!optVersionStr.isEmpty())
        {
            optVersion = atof( optVersionStr.get() );
            if( optVersion <= 0 )
            {
                throw MakeStringException( 0, "Version option must be followed by a real number > 0" );
            }
        }

        if (optXsltPath.isEmpty())
        {
            StringBuffer tmp;
            if (getComponentFilesRelPathFromBin(tmp))
                optXsltPath.set(tmp.str());
            else
                optXsltPath.set(COMPONENTFILES_DIR);
        }

        fullxsltpath.set(optXsltPath);
        fullxsltpath.append("/xslt/esxdl2xsd.xslt");

        if (!optPreprocessOutputDir.isEmpty())
            optRawOutput = true;

        if (!optAnnotate.isEmpty())
        {
            if (strcmp (optAnnotate.get(), "all") ==0)
            {
                optAllAnnot = true;
            }
            else if (strcmp (optAnnotate.get(), "none") ==0)
            {
                optNoAnnot = true;
            }
            else
            {
                  throw MakeStringException( 0, "--annotate option must be followed by 'all' or 'none' " );
            }
        }

        if (optNoCollapse)
        {
            unsetFlag(DEPFLAG_COLLAPSE);
        }

        if(optNoArrayOf)
        {
            unsetFlag(DEPFLAG_ARRAYOF);
        }

        if(optTargetNamespace.isEmpty())
        {
            optTargetNamespace.set(ESDLBINDING_URN_BASE);
        }

        return true;
    }

    virtual void doTransform(IEsdlDefObjectIterator& objs, StringBuffer &target, double version=0, IProperties *opts=NULL, const char *ns=NULL, unsigned flags=0 )
    {
        TimeSection ts("transforming via XSLT");
        cmdHelper.defHelper->toXSD( objs, target, EsdlXslToXsd, optVersion, opts, NULL, optFlags );
    }

    virtual void loadTransform( StringBuffer &xsltpath, IProperties *params)
    {
        TimeSection ts("loading XSLT");
        cmdHelper.defHelper->loadTransform( xsltpath, params, EsdlXslToXsd );
    }

    virtual void setTransformParams(IProperties *params )
    {
        cmdHelper.defHelper->setTransformParams(EsdlXslToXsd, params);
    }

    virtual int processCMD()
    {
        loadServiceDef();
        createOptionals();

        Owned<IEsdlDefObjectIterator> structs = cmdHelper.esdlDef->getDependencies( optService.get(), optMethod.get(), ESDLOPTLIST_DELIMITER, optVersion, opts.get(), optFlags );

        if( optRawOutput )
        {
            outputRaw(*structs);
        }

        if( !optXsltPath.isEmpty() )
        {
            createParams();

            loadTransform( fullxsltpath, params);

            for( unsigned i=0; i < optXformTimes; i++ )
            {
                doTransform( *structs, outputBuffer, optVersion, opts.get(), NULL, optFlags );
            }

            outputToFile();

            printf( "%s\n", outputBuffer.str() );
        }
        else
        {
            throw( MakeStringException(0, "Path to /xslt/esxdl2xsd.xslt is empty, cannot perform transform.") );
        }

        return 0;
    }

    void printOptions()
    {
        puts("Options:");
        puts("  --version <version number> : Constrain to interface version\n");
        puts("  --method <method name>[;<method name>]* : Constrain to list of specific method(s)\n" );
        puts("  --xslt <xslt file path> : Path to '/xslt/esxdl2xsd.xslt' file to transform EsdlDef to XSD\n" );
        puts("  --preprocess-output <raw output directory> : Output pre-processed xml file to specified directory before applying XSLT transform\n" );
        puts("  --annotate <all | none> : Flag turning on either all annotations or none. By default annotations are generated " );
        puts("                    for Enumerations. Setting the flag to 'none' will disable even those. Setting it" );
        puts("                    to 'all' will enable additional annotations such as collapsed, cols, form_ui, html_head and rows.\n");
        puts("  --noopt : Turns off the enforcement of 'optional' attributes on elements. If no -noopt is specified then all elements with an 'optional'" );
        puts("       will be included in the output. By default 'optional' filtering is enforced.\n");
        puts("  -opt,--optional <param value> : Value to use for optional tag filter when gathering dependencies" );
        puts("                       An example: passing 'internal' when some Esdl definition objects have the attribute");
        puts("                       optional(\"internal\") will ensure they appear in the XSD, otherwise they'd be filtered out\n");
        puts("  -tns,--target-namespace <target namespace> : The target namespace, passed to the transform via the parameter 'tnsParam'\n" );
        puts("                            used for the final output of the XSD. If not supplied will default to " );
        puts("                            http://webservices.seisint.com/<service name>\n" );
        puts("  -n <int>: Number of times to run transform after loading XSLT. Defaults to 1.\n" );
        puts("  --show-inheritance : Turns off the collapse feature. Collapsing optimizes the XML output to strip out structures\n" );
        puts("                        only used for inheritance, and collapses their elements into their child. That simplifies the\n" );
        puts("                        stylesheet. By default this option is on.");
        puts("  --no-arrayof : Supresses the use of the arrrayof element. arrayof optimizes the XML output to include 'ArrayOf...'\n" );
        puts("                        structure definitions for those EsdlArray elements with no item_tag attribute. Works in conjunction\n" );
        puts("                        with an optimized stylesheet that doesn't generate these itself. This defaults to on.");
    }

    virtual void usage()
    {
        puts("Usage:");
        puts("esdl xsd sourcePath serviceName [options]\n" );
        puts("\nsourcePath must be absolute path to the ESDL Definition file containing the" );
        puts("EsdlService definition for the service you want to work with.\n" );
        puts("serviceName EsdlService definition for the service you want to work with.\n" );

        printOptions();
        EsdlConvertCmd::usage();
    }

    virtual void outputRaw( IEsdlDefObjectIterator& obj)
    {
        if( optRawOutput )
        {
            StringBuffer xmlOut;
            StringBuffer empty;

            xmlOut.appendf( "<esxdl name=\"%s\">", optService.get());
            cmdHelper.defHelper->toXML( obj, xmlOut, optVersion, opts.get(), optFlags );
            xmlOut.append("</esxdl>");

            saveAsFile( optPreprocessOutputDir.get(), empty, xmlOut.str(), NULL );
        }
    }
    virtual void createOptionals()
    {
        // 09jun2011 tja: We must ensure that the opts IProperties object is
        // valid/non-null. This is because by passing null/invalid in to the
        // getDependencies call we're indicating that we want to turn off
        // optional filtering.

        if( optEnforceOptional )
        {
            opts.setown(createProperties(false));
            if( optOptional.length() )
            {
                opts->setProp(optOptional.get(), 1);
            }
        }
    }

    void createParams()
    {
        params.set(createProperties());

        generateNamespace(tns);

        params->setProp( "tnsParam", tns );
        params->setProp( "optional", optOptional );

        if( optAllAnnot )
        {
            params->setProp( "all_annot_Param", 1 );
        }

        if( optNoAnnot )
        {
            params->setProp( "no_annot_Param", 1 );
        }
    }

    virtual void loadServiceDef()
    {
        serviceDef.setown( createIFile(optSource) );
        if( serviceDef->exists() )
        {
            if( serviceDef->isFile() )
            {
                if( serviceDef->size() > 0 )
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
                    cmdHelper.esdlDef->addDefinitionsFromFile( serviceDef->queryFilename() );
                }
                else
                {
                    throw( MakeStringException(0, "ESDL definition file source %s is empty", optSource.get()) );
                }

            }
            else
            {
                throw( MakeStringException(0, "ESDL definition file source %s is not a file", optSource.get()) );
            }
        }
        else
        {
            throw( MakeStringException(0, "ESDL definition file source %s does not exist", optSource.get()) );
        }
    }

    virtual void outputToFile()
    {
        if (!optOutDirPath.isEmpty())
        {
            StringBuffer filename;
            generateOutputFileName(filename);
            saveAsFile(optOutDirPath.get(), filename, outputBuffer.str(), NULL);
        }
    }

    StringBuffer & generateNamespace(StringBuffer &ns)
    {
       ns.appendf("%s:%s", optTargetNamespace.get(), optService.get());
       //only add methodname if single method used.
       if (!optMethod.isEmpty() && !strstr(optMethod.get(), ESDLOPTLIST_DELIMITER))
           ns.append(':').append(optMethod.get());

       //todo
       /*
       StringBuffer ns_optionals;
       //IProperties *params = context.queryRequestParameters();
       Owned<IPropertyIterator> esdl_optionals = cmdHelper.esdlDef->queryOptionals()->getIterator();
       ForEach(*esdl_optionals)
       {
           const char *key = esdl_optionals->getPropKey();
           if (params->hasProp(key))
           {
               if (ns_optionals.length())
                   ns_optionals.append(',');
               ns_optionals.append(key);
           }
       }
       if (ns_optionals.length())
           ns.append('(').append(ns_optionals.str()).append(')');
        */

       if (optVersion > 0)
        ns.append("@ver=").appendf("%g", optVersion);
       return ns.toLowerCase();
    }

    virtual StringBuffer & generateOutputFileName( StringBuffer &filename)
    {
        filename.appendf("%s", optService.get());
        if (!optMethod.isEmpty() && !strstr(optMethod.get(), ESDLOPTLIST_DELIMITER))
            filename.append('-').append(optMethod.get());

        filename.append(outfileext);

        return filename.toLowerCase();
    }

    void saveAsFile(const char * dir, StringBuffer &outname, const char *text, const char *ext="")
    {
        StringBuffer path(dir);

        if( outname.length()>0 && path.charAt(path.length()) != PATHSEPCHAR &&  outname.charAt(0) != PATHSEPCHAR)
        {
            path.append(PATHSEPCHAR);
            path.append(outname);
        }

        if( ext && *ext )
        {
            path.append(ext);
        }

        Owned<IFile> file = createIFile(path.str());
        Owned<IFileIO> io;
        io.setown(file->open(IFOcreaterw));

        DBGLOG("Writing to file %s", file->queryFilename());

        if (io.get())
            io->write(0, strlen(text), text);
        else
            DBGLOG("File %s can't be created", file->queryFilename());
    }

    void setFlag( unsigned f ) { optFlags |= f; }
    void unsetFlag( unsigned f ) { optFlags &= ~f; }


public:
    StringAttr optService;
    StringAttr optXsltPath;
    StringAttr optMethod;
    StringAttr optOptional;
    bool optEnforceOptional;
    StringAttr optAnnotate;
    bool optAllAnnot, optNoAnnot;
    StringAttr optTargetNamespace;
    StringAttr optPreprocessOutputDir;
    bool optRawOutput;
    StringAttr optVersionStr;
    double optVersion;
    unsigned int optXformTimes;
    unsigned optFlags;
    bool optNoCollapse;
    bool optNoArrayOf;

protected:
    Owned<IFile> serviceDef;
    StringBuffer outputBuffer;
    StringBuffer fullxsltpath;
    Owned<IProperties> opts;
    Owned<IProperties> params;
    StringBuffer tns;
    StringBuffer outfileext;
};

class Esdl2WSDLCmd : public Esdl2XSDCmd
{
public:
    Esdl2WSDLCmd()
    {
         outfileext.set(".wsdl");
    }
    virtual bool parseCommandLineOption(ArgvIterator &iter)
    {
        if (iter.matchFlag(optWsdlAddress, ESDLOPT_WSDL_ADDRESS))
            return true;

        if (Esdl2XSDCmd::parseCommandLineOption(iter))
            return true;

        return false;
    }

    esdlCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
    {
        return Esdl2XSDCmd::matchCommandLineOption(iter, true);
    }

    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optWsdlAddress.isEmpty())
            optWsdlAddress.set("localhost");

        return Esdl2XSDCmd::finalizeOptions(globals);
    }

    virtual void doTransform(IEsdlDefObjectIterator& objs, StringBuffer &target, double version=0, IProperties *opts=NULL, const char *ns=NULL, unsigned flags=0 )
    {
        TimeSection ts("transforming via XSLT");
        cmdHelper.defHelper->toWSDL(objs, target, EsdlXslToWsdl, optVersion, opts, NULL, optFlags);
    }

    virtual void loadTransform( StringBuffer &xsltpath, IProperties *params)
    {
        TimeSection ts("loading XSLT");
        cmdHelper.defHelper->loadTransform( xsltpath, params, EsdlXslToWsdl );
    }

    virtual void setTransformParams(IProperties *params )
    {
        cmdHelper.defHelper->setTransformParams(EsdlXslToWsdl, params);
    }

    virtual int processCMD()
    {
        loadServiceDef();
        createOptionals();

        Owned<IEsdlDefObjectIterator> structs = cmdHelper.esdlDef->getDependencies( optService.get(), optMethod.get(), ESDLOPTLIST_DELIMITER, optVersion, opts.get(), optFlags );

        if( optRawOutput )
        {
            outputRaw(*structs);
        }

        if( !optXsltPath.isEmpty() )
        {
            createParams();
            loadTransform( fullxsltpath, params);

            for( unsigned i=0; i < optXformTimes; i++ )
            {
                doTransform( *structs, outputBuffer, optVersion, opts.get(), NULL, optFlags );
            }

            outputToFile();

            printf( "%s\n", outputBuffer.str() );
        }
        else
        {
            throw( MakeStringException(0, "Path to /xslt/esxdl2xsd.xslt is empty, cannot perform transform.") );
        }

        return 0;
    }

    virtual void usage()
    {
        puts("Usage:");
        puts("esdl wsdl sourcePath serviceName [options]\n" );
        puts("\nsourcePath must be absolute path to the ESDL Definition file containing the" );
        puts("EsdlService definition for the service you want to work with.\n" );
        puts("serviceName EsdlService definition for the service you want to work with.\n" );

        printOptions();
        puts("  --wsdladdress  Defines the output WSDL's location address\n");
        EsdlConvertCmd::usage();
    }

    virtual void createParams()
    {
        params.set(createProperties());
        generateNamespace(tns);

        params->setProp( "tnsParam", tns );
        params->setProp( "optional", optOptional );

        if( optAllAnnot )
        {
            params->setProp( "all_annot_Param", 1 );
        }

        if( optNoAnnot )
        {
            params->setProp( "no_annot_Param", 1 );
        }

        params->setProp( "create_wsdl", "true()" );
        params->setProp( "location", optWsdlAddress.get());
    }

public:
    StringAttr optWsdlAddress;
};

#define XSLT_ESDL2JAVABASE "esdl2java_srvbase.xslt"
#define XSLT_ESDL2JAVADUMMY "esdl2java_srvdummy.xslt"

class Esdl2JavaCmd : public EsdlHelperConvertCmd
{
public:
    Esdl2JavaCmd() : optVersion(0), optFlags(0)
    {}

    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        if (iter.done())
        {
            usage();
            return false;
        }

        //First two parameters' order is fixed.
        for (int par = 0; par < 2 && !iter.done(); par++)
        {
            const char *arg = iter.query();
            if (*arg != '-')
            {
                if (optSource.isEmpty())
                    optSource.set(arg);
                else if (optService.isEmpty())
                    optService.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument detected before required parameters: %s\n", arg);
                    usage();
                    return false;
                }
            }
            else
            {
                fprintf(stderr, "\noption detected before required parameters: %s\n", arg);
                usage();
                return false;
            }

            iter.next();
        }

        for (; !iter.done(); iter.next())
        {
            if (parseCommandLineOption(iter))
                continue;

            if (matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
                return false;
        }

        return true;
    }

    virtual bool parseCommandLineOption(ArgvIterator &iter)
    {
        if (iter.matchOption(optVersionStr, ESDLOPT_VERSION))
            return true;
        if (iter.matchOption(optService, ESDLOPT_SERVICE))
            return true;
        if (iter.matchOption(optMethod, ESDLOPT_METHOD))
            return true;
        if (iter.matchOption(optXsltPath, ESDLOPT_XSLT_PATH))
            return true;
        if (iter.matchOption(optPreprocessOutputDir, ESDLOPT_PREPROCESS_OUT))
            return true;
        if (EsdlConvertCmd::parseCommandLineOption(iter))
            return true;

        return false;
    }

    esdlCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
    {
        return EsdlConvertCmd::matchCommandLineOption(iter, true);
    }

    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optSource.isEmpty())
        {
            usage();
            throw( MakeStringException(0, "\nError: Source file parameter required\n"));
        }

        if( optService.isEmpty() )
        {
            usage();
            throw( MakeStringException(0, "A service name must be provided") );
        }

        if (!optVersionStr.isEmpty())
        {
            optVersion = atof( optVersionStr.get() );
            if( optVersion <= 0 )
                throw MakeStringException( 0, "Version option must be followed by a real number > 0" );
        }

        if (!optXsltPath.length())
        {
            StringBuffer binXsltPath;
            getComponentFilesRelPathFromBin(binXsltPath);
            binXsltPath.append("/xslt/");
            StringBuffer temp;
            if (checkFileExists(temp.append(binXsltPath).append(XSLT_ESDL2JAVABASE)))
                optXsltPath.set(binXsltPath);
            else
                optXsltPath.set(temp.set(COMPONENTFILES_DIR).append("/xslt/"));
        }
        return true;
    }

    virtual void doTransform(IEsdlDefObjectIterator& objs, StringBuffer &out, double version=0, IProperties *opts=NULL, const char *ns=NULL, unsigned flags=0 )
    {
    }
    virtual void loadTransform( StringBuffer &xsltpath, IProperties *params )
    {
    }

    virtual void setTransformParams(IProperties *params )
    {
    }

    virtual int processCMD()
    {
        cmdHelper.loadDefinition(optSource, optService, optVersion);
        Owned<IEsdlDefObjectIterator> structs = cmdHelper.esdlDef->getDependencies( optService, optMethod, ESDLOPTLIST_DELIMITER, optVersion, NULL, optFlags );

        if(!optPreprocessOutputDir.isEmpty())
        {
            outputRaw(*structs);
        }

        StringBuffer xsltpathServiceBase(optXsltPath);
        xsltpathServiceBase.append(XSLT_ESDL2JAVABASE);
        cmdHelper.defHelper->loadTransform( xsltpathServiceBase, NULL, EsdlXslToJavaServiceBase);
        cmdHelper.defHelper->toJavaService( *structs, outputBuffer, EsdlXslToJavaServiceBase, NULL, optFlags );

        VStringBuffer javaFileNameBase("%sServiceBase.java", optService.get());
        saveAsFile(".", javaFileNameBase, outputBuffer.str(), NULL);

        StringBuffer xsltpathServiceDummy(optXsltPath);
        xsltpathServiceDummy.append(XSLT_ESDL2JAVADUMMY);
        cmdHelper.defHelper->loadTransform( xsltpathServiceDummy, NULL, EsdlXslToJavaServiceDummy);
        cmdHelper.defHelper->toJavaService( *structs, outputBuffer.clear(), EsdlXslToJavaServiceDummy, NULL, optFlags );

        VStringBuffer javaFileNameDummy("%sServiceDummy.java", optService.get());
        saveAsFile(".", javaFileNameDummy, outputBuffer.str(), NULL);

        return 0;
    }

    void printOptions()
    {
        puts("Options:");
        puts("  --version <version number> : Constrain to interface version\n");
        puts("  --method <method name>[;<method name>]* : Constrain to list of specific method(s)\n" );
        puts("  --xslt <xslt file path> : Path to xslt files used to transform EsdlDef to Java code\n" );
        puts("  --preprocess-output <raw output directory> : Output pre-processed xml file to specified directory before applying XSLT transform\n" );
        puts("  --show-inheritance : Turns off the collapse feature. Collapsing optimizes the XML output to strip out structures\n" );
        puts("                        only used for inheritance, and collapses their elements into their child. That simplifies the\n" );
        puts("                        stylesheet. By default this option is on.");
    }

    virtual void usage()
    {
        puts("Usage:");
        puts("esdl java sourcePath serviceName [options]\n" );
        puts("\nsourcePath must be absolute path to the ESDL Definition file containing the" );
        puts("EsdlService definition for the service you want to work with.\n" );
        puts("serviceName EsdlService definition for the service you want to work with.\n" );

        printOptions();
        EsdlConvertCmd::usage();
    }

    virtual void outputRaw( IEsdlDefObjectIterator& obj)
    {
        if (optPreprocessOutputDir.isEmpty())
            return;

        StringBuffer xml;

        xml.appendf( "<esxdl name='%s'>", optService.get());
        cmdHelper.defHelper->toXML( obj, xml, optVersion, NULL, optFlags );
        xml.append("</esxdl>");
        saveAsFile(optPreprocessOutputDir, NULL, xml, NULL );
    }

    void saveAsFile(const char * dir, const char *name, const char *text, const char *ext="")
    {
        StringBuffer path(dir);
        if (name && *name)
        {
            if (*name!=PATHSEPCHAR)
                addPathSepChar(path);
            path.append(name);
        }

        if( ext && *ext )
            path.append(ext);

        Owned<IFile> file = createIFile(path);
        Owned<IFileIO> io;
        io.setown(file->open(IFOcreaterw));

        DBGLOG("Writing java to file %s", file->queryFilename());

        if (io.get())
            io->write(0, strlen(text), text);
        else
            DBGLOG("File %s can't be created", file->queryFilename());
    }

    void setFlag( unsigned f ) { optFlags |= f; }
    void unsetFlag( unsigned f ) { optFlags &= ~f; }

public:
    StringAttr optService;
    StringAttr optXsltPath;
    StringAttr optMethod;
    StringAttr optPreprocessOutputDir;
    StringAttr optVersionStr;
    double optVersion;
    unsigned optFlags;

protected:
    StringBuffer outputBuffer;
    Owned<IProperties> params;
};

//=========================================================================================

IEsdlCommand *createCoreEsdlCommand(const char *cmdname)
{
    if (!cmdname || !*cmdname)
        return NULL;
    if (strieq(cmdname, "XSD"))
        return new Esdl2XSDCmd();
    if (strieq(cmdname, "ECL"))
        return new Esdl2EclCmd();
    if (strieq(cmdname, "JAVA"))
       return new Esdl2JavaCmd();
    if (strieq(cmdname, "WSDL"))
        return new Esdl2WSDLCmd();
    if (strieq(cmdname, "PUBLISH"))
        return new EsdlPublishCmd();
    if (strieq(cmdname, "DELETE"))
        return new EsdlDeleteESDLDefCmd();
    if (strieq(cmdname, "BIND-SERVICE"))
        return new EsdlBindServiceCmd();
    if (strieq(cmdname, "BIND-METHOD"))
        return new EsdlBindMethodCmd();
    if (strieq(cmdname, "GET-BINDING"))
        return new EsdlGetBindingCmd();
    if (strieq(cmdname, "GET-DEFINITION"))
        return new EsdlGetDefinitionCmd();
    if (strieq(cmdname, "UNBIND-SERVICE"))
        return new EsdlUnBindServiceCmd();
    if (strieq(cmdname, "LIST-DEFINITIONS"))
        return new EsdlListESDLDefCmd();
    if (strieq(cmdname, "LIST-BINDINGS"))
        return new EsdlListESDLBindingsCmd();

    return NULL;
}
