/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#include "jliball.hpp"
#include "xslprocessor.hpp"

#include "esdlcmd_core.hpp"
#include "build-config.h"

typedef IPropertyTree * IPTreePtr;

class TypeEntry : public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    StringAttr name;
    StringAttr src;
    StringBuffer comment;
    StringAttr base_type;
    IPTreePtr ptree;

    TypeEntry(const char *name_, const char *src_, IPropertyTree *ptree_) :
        name(name_), src(src_), ptree(ptree_)
    {}
};

typedef TypeEntry * TypeEntryPtr;

typedef MapStringTo<IPTreePtr> AddedList;
typedef MapStringTo<TypeEntryPtr> TypeIndex;


class EsdlIndexedPropertyTrees
{
public:
    CIArrayOf<TypeEntry> types;

    AddedList included;
    TypeIndex index;

    Owned<IPropertyTree> all;

    EsdlIndexedPropertyTrees()
    {
        all.setown(createPTree("ESDL_files", false));
    }

    ~EsdlIndexedPropertyTrees()
    {
        all.clear();
    }

    StringBuffer &getQualifiedTypeName(const char *localsrc, const char *type, StringBuffer &qname)
    {
        if (type && *type)
        {
            TypeEntry **typeEntry = index.getValue(type);
            if (typeEntry && *typeEntry)
            {
                const char *src = (*typeEntry)->src.get();
                if (stricmp(src, localsrc))
                {
                    const char *finger = src;
                    if (!strnicmp(finger, "wsm_", 4))
                    {
                        finger+=4;
                    }
                    qname.appendf("%s.", finger);
                }
                qname.appendf("t_%s", type);
            }
        }
        return qname;
    }

    void loadFile(const char *srcpath, const char *srcfile, const char *srcext="", IProperties *versions=NULL, bool loadincludes=false)
    {
        if (!srcfile || !*srcfile)
            throw MakeStringException(-1, "EsdlInclude no file name");

        if (!included.getValue(srcfile))
        {
            DBGLOG("ESDL Loading include: %s", srcfile);

            StringBuffer fileName(srcpath);
            fileName.append(srcfile);

            IPropertyTree *src = NULL;

            if (stricmp(srcext,".ecm")==0)
            {
                fileName.append(".ecm");
                StringBuffer esxml;
                EsdlCmdHelper::convertECMtoESXDL(fileName.str(), srcfile, esxml, false, true, true);
                src = createPTreeFromXMLString(esxml, 0);
            }
            else if (!srcext || !*srcext || stricmp(srcext, ".xml")==0)
            {
                fileName.append(".xml");
                src = createPTreeFromXMLFile(fileName.str(), false);
            }
            else
            {
                StringBuffer msg("Unsupported file type: ");
                msg.append(srcfile);

                throw MakeStringExceptionDirect(-1, msg.str());
            }

            if (!src)
            {
                StringBuffer msg("EsdlInclude file not found - ");
                msg.append(fileName);
                throw MakeStringExceptionDirect(-1, msg.str());
            }

            included.setValue(srcfile, src);
            all->addPropTree("esxdl", src);

            StringArray add_includes;

            Owned<IPropertyTreeIterator> iter = src->getElements("*");
            ForEach (*iter)
            {
                IPropertyTree &e = iter->query();
                const char *tag = e.queryName();
                if (!stricmp(tag, "EsdlInclude"))
                    add_includes.append(e.queryProp("@file"));
                else if (!stricmp(tag, "EsdlVersion"))
                {
                    if (versions)
                        versions->setProp(e.queryProp("@name"), e.queryProp("@version"));
                }
                else
                {
                    e.setProp("@src", srcfile);
                    const char *name = e.queryProp("@name");
                    if (name && *name)
                    {
                        TypeEntry *te =  new TypeEntry(name, srcfile, &e);
                        // types.append(*dynamic_cast<CInterface*>(te));
                        types.append(*(te));
                        index.setValue(name, te);
                        if (!stricmp(tag, "EsdlEnumType"))
                        {
                            te->base_type.set(e.queryProp("base_type"));
                            te->comment.append("values[");
                            bool start=1;
                            Owned<IPropertyTreeIterator> it = e.getElements("EsdlEnumItem");
                            for (it->first(); it->isValid(); it->next())
                            {
                                IPropertyTree &item = it->query();
                                te->comment.appendf("'%s',", item.queryProp("@enum"));
                            }
                            te->comment.append("'']");
                        }
                    }
                }
            }

            if (loadincludes)
            {
                ForEachItemIn(idx, add_includes)
                {
                    const char *file=add_includes.item(idx);
                    loadFile(srcpath, file, srcext, versions, loadincludes);
                }
            }
        }
    }
};

class Esdl2EclCmd : public EsdlConvertCmd
{
public:
    Esdl2EclCmd() : optGenerateAllIncludes(false), optOutputExpandedXML(false)
    {
        StringBuffer componentsfolder;
        if (getComponentFilesRelPathFromBin(componentsfolder))
            optHPCCCompFilesDir.set(componentsfolder.str());
        else
            optHPCCCompFilesDir.set(COMPONENTFILES_DIR);
    }

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
                else if (optOutDirPath.isEmpty())
                    optOutDirPath.set(arg);
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
            if (iter.matchFlag(optGenerateAllIncludes, ESDL_CONVERT_ALL))
                continue;
            if (iter.matchFlag(optOutputExpandedXML, ESDL_CONVERT_EXPANDEDXML) || iter.matchFlag(optOutputExpandedXML, ESDL_CONVERT_EXPANDEDXML_x))
                continue;
            if (iter.matchFlag(optHPCCCompFilesDir, HPCC_COMPONENT_FILES_DIR) || iter.matchFlag(optHPCCCompFilesDir, HPCC_COMPONENT_FILES_DIR_CDE))
                continue;

            if (matchCommandLineOption(iter, true)!=EsdlCmdOptionMatch)
                return false;
        }

        return true;
    }

    esdlCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
    {
        if (iter.matchOption(optSource, ESDL_CONVERT_SOURCE))
            return EsdlCmdOptionMatch;
        if (iter.matchOption(optOutDirPath, ESDL_CONVERT_OUTDIR))
            return EsdlCmdOptionMatch;
        if (iter.matchFlag(optOutputExpandedXML, ESDL_CONVERT_EXPANDEDXML) || iter.matchFlag(optOutputExpandedXML, ESDL_CONVERT_EXPANDEDXML_x))
            return EsdlCmdOptionMatch;
        if (iter.matchFlag(optHPCCCompFilesDir, HPCC_COMPONENT_FILES_DIR) || iter.matchFlag(optHPCCCompFilesDir, HPCC_COMPONENT_FILES_DIR_CDE))
            return EsdlCmdOptionMatch;

        return EsdlCmdCommon::matchCommandLineOption(iter, true);
    }


    virtual bool finalizeOptions(IProperties *globals)
    {
        return EsdlConvertCmd::finalizeOptions(globals);
    }

    virtual int processCMD()
    {
        if (!optOutDirPath.isEmpty())
            recursiveCreateDirectory(optOutDirPath.get());

        StringBuffer srcPath;
        StringBuffer srcName;
        StringBuffer srcExt;

        splitFilename(optSource.get(), NULL, &srcPath, &srcName, &srcExt);

        unsigned start = msTick();
        EsdlIndexedPropertyTrees trees;
        trees.loadFile(srcPath.str(), srcName.str(), srcExt.str(), NULL, optGenerateAllIncludes);
        DBGLOG("Time taken to load ESDL files %u", msTick() - start);

        StringBuffer idxxml("<types><type name=\"StringArrayItem\" src=\"share\"/>");

        DBGLOG("\tSTART INDEX");
        HashIterator iit(trees.index);
        for (iit.first(); iit.isValid(); iit.next())
        {
            const char *key = (const char *) iit.get().getKey();
            TypeEntry ** typ = trees.index.getValue(key);
            if (typ)
            {
                StringBuffer src((*typ)->src.get());
                if (!strnicmp(src.str(), "wsm_", 4))
                    src.remove(0, 4);
                idxxml.appendf("<type name=\"%s\" src=\"%s\" ", (*typ)->name.get(), src.str());
                if ((*typ)->comment.length())
                    idxxml.appendf("comment=\"%s\" ", (*typ)->comment.str());
                if ((*typ)->base_type.length())
                    idxxml.appendf("base_type=\"%s\" ", (*typ)->base_type.get());
                idxxml.append("/>");
            }
        }

        DBGLOG("\tEND INDEX");
        idxxml.append("</types>");

        idxxml.append("<keywords>");
        idxxml.append("<keyword word=\"shared\"/>");
        idxxml.append("<keyword word=\"function\"/>");
        idxxml.append("<keyword word=\"record\"/>");
        idxxml.append("<keyword word=\"header\"/>");
        idxxml.append("<keyword word=\"service\"/>");
        idxxml.append("<keyword word=\"type\"/>");
        idxxml.append("<keyword word=\"penalty\"/>");
        idxxml.append("<keyword word=\"isvalid\"/>");
        idxxml.append("</keywords>");

        if (optGenerateAllIncludes)
        {
            Owned<IPropertyTreeIterator> files = trees.all->getElements("esxdl");
            ForEach(*files)
            {

                IPropertyTree &file = files->query();
                const char * filename = file.queryProp("@name");
                StringBuffer xmlfile;
                toXML(&file, xmlfile, 0,0);

                //expandEsxdlForEclGeneration(trees, src);
                outputEcl(srcPath.str(), filename, optOutDirPath.get(), idxxml.str(), xmlfile);
            }
        }
        else
        {
            int count = trees.all->getCount("esxdl");
            if (trees.all->getCount("esxdl") > 0)
            {
                IPropertyTree *file = trees.all->getPropTree("esxdl[1]");
                if (file)
                {
                    StringBuffer xmlfile;
                    toXML(file, xmlfile, 0,0);

                    //expandEsxdlForEclGeneration(trees, srcName.str());
                    outputEcl(srcPath.str(), srcName.str(), optOutDirPath.get(), idxxml.str(), xmlfile); //rodrigo
                }
            }
        }

        return 0;
    }

    virtual void usage()
    {
        fputs("\nUsage:\n\n"
                "esdl ecl sourcePath outputPath [options]\n"
                "\nsourcePath must be absolute path to the ESDL Definition file containing the"
                "EsdlService definition for the service you want to work with.\n"
                "outputPath must be the absolute path where the ECL output with be created."
                "   Options:\n"
                "      -x, --expandedxml     Output expanded XML files\n"
                "      --all                 Generate ECL files for all includes\n"
                ,stdout);
    }

    void outputEcl(const char *srcpath, const char *file, const char *path, const char *types, const char * xml)
    {
        DBGLOG("Generating ECL file for %s", file);


        StringBuffer filePath;
        StringBuffer fileName;
        StringBuffer fileExt;

        splitFilename(file, NULL, &filePath, &fileName, &fileExt);

        const char *finger = fileName.str();
        if (!strnicmp(finger, "wsm_", 4))
            finger+=4;

        StringBuffer outfile;
        if (path && *path)
        {
            outfile.append(path);
            if (outfile.length() && !strchr("/\\", outfile.charAt(outfile.length()-1)))
                outfile.append('/');
        }
        outfile.append(finger).append(".ecl");

        StringBuffer fullname(srcpath);
        if (fullname.length() && !strchr("/\\", fullname.charAt(fullname.length()-1)))
            fullname.append('/');
        fullname.append(fileName.str()).append(".xml");

        StringBuffer expstr;
        expstr.append("<expesdl>");
        expstr.append(types);
        expstr.append(xml);
        expstr.append("</expesdl>");

        if (optOutputExpandedXML)
        {
            StringBuffer xmlfile;
            if (path && *path)
            {
                xmlfile.append(path);
                if (xmlfile.length() && !strchr("/\\", xmlfile.charAt(xmlfile.length()-1)))
                    xmlfile.append('/');
            }
            xmlfile.append(finger).append("_expanded.xml");
            Owned<IFile> ifile =  createIFile(xmlfile.str());
            if (ifile)
            {
                Owned<IFileIO> ifio = ifile->open(IFOcreate);
                if (ifio)
                {
                    ifio->write(0, expstr.length(), expstr.str());
                }
            }
        }

        Owned<IProperties> params = createProperties();
        params->setProp("sourceFileName", finger);
        StringBuffer esdl2eclxslt (optHPCCCompFilesDir.get());
        esdl2eclxslt.append("/xslt/esdl2ecl.xslt");
        esdl2eclxsltTransform(expstr.str(), esdl2eclxslt.toCharArray(), params, outfile.str());
    }

    void esdl2eclxsltTransform(const char* xml, const char* sheet, IProperties *params, const char *filename)
    {
        StringBuffer xsl;
        xsl.loadFile(sheet);

        Owned<IXslProcessor> proc  = getXslProcessor();
        Owned<IXslTransform> trans = proc->createXslTransform();

        trans->setXmlSource(xml, strlen(xml));
        trans->setXslSource(xsl, xsl.length(), "esdl2ecl", ".");

        if (params)
        {
            Owned<IPropertyIterator> it = params->getIterator();
            for (it->first(); it->isValid(); it->next())
            {
                const char *key = it->getPropKey();
                //set parameter in the XSL transform skipping over the @ prefix, if any
                const char* paramName = *key == '@' ? key+1 : key;
                trans->setParameter(paramName, StringBuffer().append('\'').append(params->queryProp(key)).append('\'').str());
            }
        }

        trans->setResultTarget(filename);
        trans->transform();
    }

public:
    bool optGenerateAllIncludes;
    bool optOutputExpandedXML;
    StringAttr optHPCCCompFilesDir;
};
