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
    Esdl2XSDCmd() : optUnversionedNamespace(false), optInterfaceVersion(0), optAllAnnot(false), optNoAnnot(false),
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
        if (iter.matchFlag(optUnversionedNamespace, ESDLOPT_UNVERSIONED_NAMESPACE) || iter.matchFlag(optUnversionedNamespace, ESDLOPT_UNVERSIONED_NAMESPACE_S))
            return true;
        if (iter.matchOption(optInterfaceVersionStr, ESDLOPT_INTERFACE_VERSION) || iter.matchOption(optInterfaceVersionStr, ESDLOPT_INTERFACE_VERSION_S))
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
        StringAttr oneOption;
        if (iter.matchOption(oneOption, ESDLOPT_INCLUDE_PATH) || iter.matchOption(oneOption, ESDLOPT_INCLUDE_PATH_S))
            return false;  //Return false to negate allowing the include path options from parent class

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

        if (!optInterfaceVersionStr.isEmpty())
        {
            optInterfaceVersion = atof( optInterfaceVersionStr.get() );
            if ( optInterfaceVersion <= 0 )
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
            optTargetNamespace.set(DEFAULT_NAMESPACE_BASE);
        }
        cmdHelper.verbose = optVerbose;

        return true;
    }

    virtual void doTransform(IEsdlDefObjectIterator& objs, StringBuffer &target, double version=0, IProperties *opts=NULL, const char *ns=NULL, unsigned flags=0 )
    {
        TimeSection ts("transforming via XSLT");
        cmdHelper.defHelper->toXSD( objs, target, EsdlXslToXsd, optInterfaceVersion, opts, NULL, optFlags );
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
        cmdHelper.loadDefinition(optSource, optService.get(), optInterfaceVersion,"", optTraceFlags());
        createOptionals();

        Owned<IEsdlDefObjectIterator> structs = cmdHelper.esdlDef->getDependencies( optService.get(), optMethod.get(), ESDLOPTLIST_DELIMITER, optInterfaceVersion, opts.get(), optFlags );

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
                doTransform( *structs, outputBuffer, optInterfaceVersion, opts.get(), NULL, optFlags );
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
        puts("   -iv,--interface-version <version>    Constrain to interface version");
        puts("   --method <meth name>[;<meth name>]*  Constrain to list of specific method(s)" );
        puts("   --xslt <xslt file path>              Path to '/xslt/esxdl2xsd.xslt' file to transform EsdlDef to XSD" );
        puts("   --preprocess-output <rawoutput dir>  Output pre-processed xml file to specified directory before applying XSLT transform" );
        puts("   --annotate <all | none>              Flag turning on either all annotations or none. By default annotations are generated " );
        puts("                                        for Enumerations. Setting the flag to 'none' will disable even those. Setting it" );
        puts("                                        to 'all' will enable additional annotations such as collapsed, cols, form_ui, html_head and rows.");
        puts("   --noopt                              Turns off the enforcement of 'optional' attributes on elements. If no -noopt is specified then all elements with an 'optional'" );
        puts("                                        will be included in the output. By default 'optional' filtering is enforced.");
        puts("   -opt,--optional <param value>        Value to use for optional tag filter when gathering dependencies" );
        puts("                                        An example: passing 'internal' when some Esdl definition objects have the attribute");
        puts("                                        optional(\"internal\") will ensure they appear in the XSD, otherwise they'd be filtered out");
        puts("   -tns,--target-namespace <target ns>  The target namespace, passed to the transform via the parameter 'tnsParam'" );
        puts("                                        used for the final output of the XSD. If not supplied will default to " );
        puts("                                        http://webservices.seisint.com/<service name>" );
        puts("   -uvns,--unversioned-ns               Do not append service interface version to namespace" );
        puts("   -n <int>                             Number of times to run transform after loading XSLT. Defaults to 1." );
        puts("   --show-inheritance                   Turns off the collapse feature. Collapsing optimizes the XML output to strip out structures" );
        puts("                                        only used for inheritance, and collapses their elements into their child. That simplifies the" );
        puts("                                        stylesheet. By default this option is on.");
        puts("   --no-arrayof                         Supresses the use of the arrrayof element. arrayof optimizes the XML output to include 'ArrayOf...'" );
        puts("                                        structure definitions for those EsdlArray elements with no item_tag attribute. Works in conjunction" );
        puts("                                        with an optimized stylesheet that doesn't generate these itself. This defaults to on.");
    }

    virtual void usage()
    {
        puts("Usage:");
        puts("esdl xsd sourcePath serviceName [options]\n\n" );
        puts("sourcePath   - Absolute path to ESDL definition file" );
        puts("               which contains ESDL Service definition." );
        puts("serviceName  - Name of ESDL Service defined in the given definition file.\n" );

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
            cmdHelper.defHelper->toXML( obj, xmlOut, optInterfaceVersion, opts.get(), optFlags );
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
        bool urlNamespace = false;
        if (startsWith(optTargetNamespace.get(), "http://"))
            urlNamespace = true;

       ns.appendf("%s%c%s", optTargetNamespace.get(), urlNamespace ? '/' : ':', optService.get());
       //only add methodname if single method used.
       if (!optMethod.isEmpty() && !strstr(optMethod.get(), ESDLOPTLIST_DELIMITER))
           ns.append(urlNamespace ? '/' : ':').append(optMethod.get());

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

       if (optInterfaceVersion > 0 && !optUnversionedNamespace)
           ns.append("@ver=").appendf("%g", optInterfaceVersion);
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
    StringAttr optInterfaceVersionStr;
    double optInterfaceVersion;
    unsigned int optXformTimes;
    unsigned optFlags;
    bool optNoCollapse;
    bool optNoArrayOf;
    bool optUnversionedNamespace;

protected:
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
        cmdHelper.defHelper->toWSDL(objs, target, EsdlXslToWsdl, optInterfaceVersion, opts, NULL, optFlags);
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
        cmdHelper.loadDefinition(optSource, optService.get(), optInterfaceVersion, "", optTraceFlags());
        createOptionals();

        Owned<IEsdlDefObjectIterator> structs = cmdHelper.esdlDef->getDependencies( optService.get(), optMethod.get(), ESDLOPTLIST_DELIMITER, optInterfaceVersion, opts.get(), optFlags );

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
                doTransform( *structs, outputBuffer, optInterfaceVersion, opts.get(), NULL, optFlags );
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
        puts("esdl wsdl sourcePath serviceName [options]\n\n" );
        puts("sourcePath   - Absolute path to the ESDL definition file" );
        puts("               which contains ESDL Service definition." );
        puts("serviceName  - Name of ESDL Service defined in the given definition file.\n" );

        printOptions();
        puts("   --wsdladdress                        Defines the output WSDL's location address\n");
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
    Esdl2JavaCmd() : optFlags(0)
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
        extractEsdlCmdOption(optIncludePath, globals, ESDLOPT_INCLUDE_PATH_ENV, ESDLOPT_INCLUDE_PATH_INI, NULL, NULL);

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
        cmdHelper.verbose = optVerbose;
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
        cmdHelper.loadDefinition(optSource, optService, 0, optIncludePath, optTraceFlags());
        Owned<IEsdlDefObjectIterator> structs = cmdHelper.esdlDef->getDependencies( optService, optMethod, ESDLOPTLIST_DELIMITER, 0, NULL, optFlags );

        if(!optPreprocessOutputDir.isEmpty())
        {
            outputRaw(*structs);
        }

        StringBuffer xsltpathServiceBase(optXsltPath);
        xsltpathServiceBase.append(XSLT_ESDL2JAVABASE);
        cmdHelper.defHelper->loadTransform( xsltpathServiceBase, NULL, EsdlXslToJavaServiceBase);
        cmdHelper.defHelper->toMicroService( *structs, outputBuffer, EsdlXslToJavaServiceBase, NULL, optFlags );

        VStringBuffer javaFileNameBase("%sServiceBase.java", optService.get());
        saveAsFile(".", javaFileNameBase, outputBuffer.str(), NULL);

        StringBuffer xsltpathServiceDummy(optXsltPath);
        xsltpathServiceDummy.append(XSLT_ESDL2JAVADUMMY);
        cmdHelper.defHelper->loadTransform( xsltpathServiceDummy, NULL, EsdlXslToJavaServiceDummy);
        cmdHelper.defHelper->toMicroService( *structs, outputBuffer.clear(), EsdlXslToJavaServiceDummy, NULL, optFlags );

        VStringBuffer javaFileNameDummy("%sServiceDummy.java", optService.get());
        saveAsFile(".", javaFileNameDummy, outputBuffer.str(), NULL);

        return 0;
    }

    void printOptions()
    {
        puts("Options:");
        puts("   --method <meth name>[;<meth name>]* Constrain to list of specific method(s)" );
        puts("   --xslt <xslt file path>             Path to xslt files used to transform EsdlDef to Java code" );
        puts("   --preprocess-output <raw output directory> : Output pre-processed xml file to specified directory before applying XSLT transform" );
        puts("   --show-inheritance                  Turns off the collapse feature. Collapsing optimizes the XML output to strip out structures" );
        puts("                                       only used for inheritance, and collapses their elements into their child. That simplifies the" );
        puts("                                       stylesheet. By default this option is on.");
        puts(ESDLOPT_INCLUDE_PATH_USAGE);
    }

    virtual void usage()
    {
        puts("Usage:");

        puts("esdl java sourcePath serviceName [options]\n" );
        puts("\nsourcePath - Absolute path to the EXSDL Definition file ( XML generated from ECM )" );
        puts("               which contains ESDL Service definition.\n" );
        puts("serviceName  - Name of ESDL Service defined in the given EXSDL file.\n" );

        printOptions();
        EsdlConvertCmd::usage();
    }

    virtual void outputRaw( IEsdlDefObjectIterator& obj)
    {
        if (optPreprocessOutputDir.isEmpty())
            return;

        StringBuffer xml;

        xml.appendf( "<esxdl name='%s'>", optService.get());
        cmdHelper.defHelper->toXML( obj, xml, 0, NULL, optFlags );
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
    unsigned optFlags;

protected:
    StringBuffer outputBuffer;
    Owned<IProperties> params;
};

#define XSLT_ESDL2CPPBASEHPP "esdl2cpp_srvbasehpp.xslt"
#define XSLT_ESDL2CPPBASECPP "esdl2cpp_srvbasecpp.xslt"
#define XSLT_ESDL2CPPSRVHPP "esdl2cpp_srvhpp.xslt"
#define XSLT_ESDL2CPPSRVCPP "esdl2cpp_srvcpp.xslt"
#define XSLT_ESDL2CPPCMAKE "esdl2cpp_cmake.xslt"
#define XSLT_ESDL2CPPTYPES "esdl2cpp_types.xslt"

class Esdl2CppCmd : public EsdlHelperConvertCmd
{
public:
    Esdl2CppCmd() : optFlags(0)
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
        if (iter.matchOption(optService, ESDLOPT_SERVICE))
            return true;
        if (iter.matchOption(optMethod, ESDLOPT_METHOD))
            return true;
        if (iter.matchOption(optXsltPath, ESDLOPT_XSLT_PATH))
            return true;
        if (iter.matchOption(optOutDirPath, ESDL_CONVERT_OUTDIR))
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
        extractEsdlCmdOption(optIncludePath, globals, ESDLOPT_INCLUDE_PATH_ENV, ESDLOPT_INCLUDE_PATH_INI, NULL, NULL);

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

        if (!optXsltPath.length())
        {
            StringBuffer binXsltPath;
            getComponentFilesRelPathFromBin(binXsltPath);
            binXsltPath.append("/xslt/");
            StringBuffer temp;
            if (checkFileExists(temp.append(binXsltPath).append(XSLT_ESDL2CPPBASEHPP)))
                optXsltPath.set(binXsltPath);
            else
                optXsltPath.set(temp.set(COMPONENTFILES_DIR).append("/xslt/"));
        }
        cmdHelper.verbose = optVerbose;
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
        cmdHelper.loadDefinition(optSource, optService, 0, optIncludePath, optTraceFlags());
        Owned<IEsdlDefObjectIterator> structs = cmdHelper.esdlDef->getDependencies( optService, optMethod, ESDLOPTLIST_DELIMITER, 0, NULL, optFlags );

        if(!optPreprocessOutputDir.isEmpty())
        {
            outputRaw(*structs);
        }


        StringBuffer outdir;
        if (optOutDirPath.length() > 0)
            outdir.append(optOutDirPath);
        else
            outdir.append(".");
        StringBuffer sourcedir(outdir);
        sourcedir.append(PATHSEPCHAR).append("source");
        StringBuffer builddir(outdir);
        builddir.append(PATHSEPCHAR).append("build");
        recursiveCreateDirectory(sourcedir.str());
        recursiveCreateDirectory(builddir.str());

        VStringBuffer hppFileNameBase("%sServiceBase.hpp", optService.get());
        StringBuffer xsltpath(optXsltPath);
        xsltpath.append(XSLT_ESDL2CPPBASEHPP);
        StringBuffer filefullpath;
        filefullpath.append(sourcedir).append(PATHSEPCHAR).append(hppFileNameBase);
        if (checkFileExists(filefullpath.str()))
            DBGLOG("ATTENTION: File %s already exists, won't generate again", filefullpath.str());
        else
        {
            cmdHelper.defHelper->loadTransform( xsltpath, NULL, EsdlXslToCppServiceBaseHpp);
            cmdHelper.defHelper->toMicroService( *structs, outputBuffer, EsdlXslToCppServiceBaseHpp, NULL, optFlags );
            saveAsFile(sourcedir, hppFileNameBase, outputBuffer.str(), NULL);
        }

        VStringBuffer cppFileNameBase("%sServiceBase.cpp", optService.get());
        outputBuffer.clear();
        xsltpath.clear().append(optXsltPath);
        xsltpath.append(XSLT_ESDL2CPPBASECPP);
        filefullpath.clear().append(sourcedir).append(PATHSEPCHAR).append(cppFileNameBase);
        if (checkFileExists(filefullpath.str()))
            DBGLOG("ATTENTION: File %s already exists, won't generate again", filefullpath.str());
        else
        {
            cmdHelper.defHelper->loadTransform( xsltpath, NULL, EsdlXslToCppServiceBaseCpp);
            cmdHelper.defHelper->toMicroService( *structs, outputBuffer, EsdlXslToCppServiceBaseCpp, NULL, optFlags );
            saveAsFile(sourcedir.str(), cppFileNameBase, outputBuffer.str(), NULL);
        }

        VStringBuffer srvHppFileNameBase("%sService.hpp", optService.get());
        outputBuffer.clear();
        xsltpath.clear().append(optXsltPath);
        xsltpath.append(XSLT_ESDL2CPPSRVHPP);
        filefullpath.clear().append(sourcedir).append(PATHSEPCHAR).append(srvHppFileNameBase);
        if (checkFileExists(filefullpath.str()))
            DBGLOG("ATTENTION: File %s already exists, won't generate again", filefullpath.str());
        else
        {
            cmdHelper.defHelper->loadTransform( xsltpath, NULL, EsdlXslToCppServiceHpp);
            cmdHelper.defHelper->toMicroService( *structs, outputBuffer, EsdlXslToCppServiceHpp, NULL, optFlags );
            saveAsFile(sourcedir.str(), srvHppFileNameBase, outputBuffer.str(), NULL);
        }

        VStringBuffer srvCppFileNameBase("%sService.cpp", optService.get());
        outputBuffer.clear();
        xsltpath.clear().append(optXsltPath);
        xsltpath.append(XSLT_ESDL2CPPSRVCPP);
        filefullpath.clear().append(sourcedir).append(PATHSEPCHAR).append(srvCppFileNameBase);
        if (checkFileExists(filefullpath.str()))
            DBGLOG("ATTENTION: File %s already exists, won't generate again", filefullpath.str());
        else
        {
            cmdHelper.defHelper->loadTransform( xsltpath, NULL, EsdlXslToCppServiceCpp);
            cmdHelper.defHelper->toMicroService( *structs, outputBuffer, EsdlXslToCppServiceCpp, NULL, optFlags );
            saveAsFile(sourcedir.str(), srvCppFileNameBase, outputBuffer.str(), NULL);
        }

        outputBuffer.clear();
        xsltpath.clear().append(optXsltPath);
        xsltpath.append(XSLT_ESDL2CPPCMAKE);
        filefullpath.clear().append(sourcedir).append(PATHSEPCHAR).append("CMakeLists.txt");
        if (checkFileExists(filefullpath.str()))
            DBGLOG("ATTENTION: File %s already exists, won't generate again", filefullpath.str());
        else
        {
            Owned<IProperties> params = createProperties();
            params->setProp("installdir", INSTALL_DIR);
            cmdHelper.defHelper->loadTransform( xsltpath, params.get(), EsdlXslToCppCMake);
            cmdHelper.defHelper->toMicroService( *structs, outputBuffer, EsdlXslToCppCMake, NULL, optFlags );
            saveAsFile(sourcedir.str(), "CMakeLists.txt", outputBuffer.str(), NULL);
        }

        outputBuffer.clear();
        xsltpath.clear().append(optXsltPath);
        xsltpath.append(XSLT_ESDL2CPPTYPES);
        filefullpath.clear().append(sourcedir).append(PATHSEPCHAR).append("primitivetypes.hpp");
        if (checkFileExists(filefullpath.str()))
            DBGLOG("ATTENTION: File %s already exists, won't generate again", filefullpath.str());
        else
        {
            cmdHelper.defHelper->loadTransform( xsltpath, NULL, EsdlXslToCppTypes);
            cmdHelper.defHelper->toMicroService( *structs, outputBuffer, EsdlXslToCppTypes, NULL, optFlags );
            saveAsFile(sourcedir.str(), "primitivetypes.hpp", outputBuffer.str(), NULL);
        }
        return 0;
    }

    void printOptions()
    {
        puts("Options:");
        puts("   --method <meth name>[;<meth name>]* Constrain to list of specific method(s)" );
        puts("   --xslt <xslt file path>             Path to xslt files used to transform EsdlDef to c++ code" );
        puts("   --preprocess-output <raw output directory> : Output pre-processed xml file to specified directory before applying XSLT transform" );
        puts("   --show-inheritance                  Turns off the collapse feature. Collapsing optimizes the XML output to strip out structures" );
        puts("                                       only used for inheritance, and collapses their elements into their child. That simplifies the" );
        puts("                                       stylesheet. By default this option is on.");
        puts(ESDLOPT_INCLUDE_PATH_USAGE);
    }

    virtual void usage()
    {
        puts("Usage:");

        puts("esdl cpp sourcePath serviceName [options]\n" );
        puts("\nsourcePath - Absolute path to the EXSDL Definition file ( XML generated from ECM )" );
        puts("               which contains ESDL Service definition.\n" );
        puts("serviceName  - Name of ESDL Service defined in the given EXSDL file.\n" );

        printOptions();
        EsdlConvertCmd::usage();
    }

    virtual void outputRaw( IEsdlDefObjectIterator& obj)
    {
        if (optPreprocessOutputDir.isEmpty())
            return;

        StringBuffer xml;

        xml.appendf( "<esxdl name='%s'>", optService.get());
        cmdHelper.defHelper->toXML( obj, xml, 0, NULL, optFlags );
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

        DBGLOG("Writing c++ to file %s", file->queryFilename());

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
    StringAttr optOutDirPath;
    StringAttr optPreprocessOutputDir;
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
    if (strieq(cmdname, "CPP"))
       return new Esdl2CppCmd();
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
    if (strieq(cmdname, "UNBIND-METHOD"))
        return new EsdlUnBindMethodCmd();
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
    if (strieq(cmdname, "BIND-LOG-TRANSFORM"))
        return new EsdlBindLogTransformCmd();
    if (strieq(cmdname, "UNBIND-LOG-TRANSFORM"))
        return new EsdlUnBindLogTransformCmd();
    if (strieq(cmdname, "MONITOR"))
        return createEsdlMonitorCommand(cmdname);
    if (strieq(cmdname, "MONITOR-TEMPLATE"))
        return createEsdlMonitorCommand(cmdname);

    return NULL;
}
