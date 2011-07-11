/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

// esdl2ecl.cpp : Defines the entry point for the console application.
//
#include <vld.h>

#include "jliball.hpp"
#include "xslprocessor.hpp"

typedef IPropertyTree * IPTreePtr;


class TypeEntry : public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    StringAttr name;
    StringAttr src;
    IPTreePtr ptree;

    TypeEntry(const char *name_, const char *src_, IPropertyTree *ptree_) : 
        name(name_), src(src_), ptree(ptree_) 
    {
    }

};

typedef TypeEntry * TypeEntryPtr;

MAKESTRINGMAPPING(IPTreePtr, IPTreePtr, AddedList);
MAKESTRINGMAPPING(TypeEntryPtr, TypeEntryPtr, TypeIndex);


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

    void loadFile(const char *srcpath, const char *srcfile, IProperties *versions=NULL)
    {
        if (!srcfile || !*srcfile)
            throw MakeStringException(-1, "EsdlInclude no file name");
        if (!included.getValue(srcfile))
        {
            DBGLOG("ESDL Loading include: %s", srcfile);

            StringBuffer FileName;
            if (srcpath && *srcpath)
            {
                FileName.append(srcpath);
                if (FileName.length() && !strchr("/\\", FileName.charAt(FileName.length()-1)))
                    FileName.append('/');
            }
            FileName.append(srcfile).append(".xml");

            IPropertyTree *src = createPTreeFromXMLFile(FileName.str(), false);
            if (!src)
            {
                StringBuffer msg("EsdlInclude file not found - ");
                msg.append(FileName);
                throw MakeStringException(-1, msg.str());
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
                        types.append(*dynamic_cast<CInterface*>(te));                       
                        index.setValue(name, te);
                    }
                }
            }

            ForEachItemIn(idx, add_includes)
            {
                const char *file=add_includes.item(idx);
                loadFile(srcpath, file, versions);
            }
        }
    }
};



void xsltTransform(const char* xml, const char* sheet, IProperties *params, const char *filename)
{
    StringBuffer xsl;
    xsl.loadFile(sheet);

    Owned<IXslProcessor> proc  = getXslProcessor();
    Owned<IXslTransform> trans = proc->createXslTransform();

    trans->setXmlSource(xml, strlen(xml));
    trans->setXslSource(xsl, xsl.length());

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


bool isEclKeyword(const char *name)
{
    //complete list from hash table later
    return (!stricmp(name, "shared") || 
            !stricmp(name, "function") || 
            !stricmp(name, "record") || 
            !stricmp(name, "header") || 
            !stricmp(name, "service") ||
            !stricmp(name, "type") ||
            !stricmp(name, "penalty") ||
            !stricmp(name, "isvalid"));
}

void updateEclName(IPropertyTree &item)
{
    const char *name = item.queryProp("@name");
    if (name && *name)
    {
        if (!item.hasProp("@ecl_name") && isEclKeyword(name))
        {
            StringBuffer ecl_name("_");
            ecl_name.append(name);
            item.addProp("@ecl_name", ecl_name.str());
        }
    }
}

void updateEclType(const char *curfile, const char *type_attr, IPropertyTree &item, EsdlIndexedPropertyTrees &trees, const char *ecl_type_attr="@ecl_type")
{
    if (!item.hasProp("@ecl_type"))
    {
        const char *type = item.queryProp(type_attr);
        StringBuffer ecltype;
        trees.getQualifiedTypeName(curfile, type, ecltype);
        if (ecltype.length())
            item.setProp(ecl_type_attr, ecltype.str());
    }
}


void expandEsxdlStructure(EsdlIndexedPropertyTrees &trees, IPropertyTree &item, const char *file)
{
    //updateEclName(item);
    updateEclType(file, "@base_type", item, trees, "@ecl_base_type");
    
    Owned<IPropertyTreeIterator> children = item.getElements("*");
    ForEach(*children)
    {
        IPropertyTree &child = children->query();
        updateEclName(child);
        if (!stricmp(child.queryName(), "EsdlElement"))
            updateEclType(file, "@complex_type", child, trees);
        else if (!stricmp(child.queryName(), "EsdlArray"))
        {
            if (!child.hasProp("@max_count") && !child.hasProp("@max_count_var"))
                child.addPropInt("@max_count", 1);
            updateEclType(file, "@type", child, trees);
        }
        else if (!stricmp(child.queryName(), "EsdlEnum"))
        {
            const char *enum_type = child.queryProp("@enum_type");
            if (enum_type && *enum_type)
            {
                TypeEntry **typeEntry = trees.index.getValue(enum_type);
                if (typeEntry && *typeEntry)
                {
                    StringBuffer ecl_type("string");
                    if ((*typeEntry)->ptree->hasProp("@max_len"))
                        ecl_type.append((*typeEntry)->ptree->queryProp("@max_len"));
                    child.addProp("@ecl_type", ecl_type.str());
                    StringBuffer comment;
                    Owned<IPropertyTreeIterator> items = (*typeEntry)->ptree->getElements("EsdlEnumItem");
                    bool emptydefined=false;
                    ForEach(*items)
                    {
                        if (!comment.length())
                            comment.append("//values[");
                        else
                            comment.append(',');
                        const char *enum_val=items->query().queryProp("@enum");
                        comment.appendf("'%s'", enum_val);
                        if (!*enum_val)
                            emptydefined=true;
                    }
                    if (!emptydefined)
                        comment.append(",''");
                    comment.append("]");
                    child.addProp("@ecl_comment", comment.str());
                }
            }
        }
    }
}

void addFlatTagList(EsdlIndexedPropertyTrees &trees, IPropertyTree &dst, IPropertyTree &cur, StringBuffer &path)
{
    const char *base_type=cur.queryProp("@base_type");
    if (base_type && *base_type)
    {
        TypeEntry **base_entry = trees.index.getValue(base_type);
        if (base_entry && *base_entry)
        {
            unsigned len = path.length();
            path.append('(').append(base_type).append(')');;
            addFlatTagList(trees, dst, *((*base_entry)->ptree), path);
            path.setLength(len);
        }
    }

    Owned<IPropertyTreeIterator> elems = cur.getElements("EsdlElement");
    ForEach(*elems)
    {
        IPropertyTree &item = elems->query();
        const char *complex_type = item.queryProp("@complex_type");
        if (complex_type)
        {
            TypeEntry **elem_entry = trees.index.getValue(complex_type);
            if (elem_entry && *elem_entry)
            {
                unsigned len = path.length();
                path.append('/').append(item.queryProp("@name"));
                addFlatTagList(trees, dst, *((*elem_entry)->ptree), path);
                path.setLength(len);
            }
        }
        else
        {
            const char *flat_tag = item.queryProp("@flat_tag");
            if (flat_tag && *flat_tag=='.')
                flat_tag=item.queryProp("@name");
            StringBuffer xml;
            if (flat_tag && *flat_tag)
            {
                xml.appendf("<InputTag flat_name=\"%s\" path=\"%s/%s\" type=\"%s\"/>", flat_tag, path.str(), item.queryProp("@name"), item.queryProp("@type"));
                dst.addPropTree("InputTag", createPTreeFromXMLString(xml.str(), false));
            }
            else
            {
                xml.appendf("<NoInputTag path=\"%s/%s\" type=\"%s\"/>", path.str(), item.queryProp("@name"), item.queryProp("@type"));
                dst.addPropTree("NoInputTag", createPTreeFromXMLString(xml.str(), false));
            }

        }
    }
}


void expandEsxdlRequest(EsdlIndexedPropertyTrees &trees, IPropertyTree &item, const char *file)
{
    expandEsxdlStructure(trees, item, file);
    StringBuffer path;
    addFlatTagList(trees, item, item, path);
}

void expandEsxdlForEclGeneration(EsdlIndexedPropertyTrees &trees, const char *file)
{
    IPropertyTree **content = trees.included.getValue(file);
    if (content && *content)
    {
        DBGLOG("Expanding the ESDL XML content for %s", file);
        Owned<IPropertyTreeIterator> structs = (*content)->getElements("*");
        ForEach(*structs)
        {
            IPropertyTree &item = structs->query();
            const char *esdltag = item.queryName();
            if (!strnicmp(esdltag, "Esdl", 4))
            {
                if (!stricmp(esdltag+4, "Request"))
                    expandEsxdlRequest(trees, item, file);
                else if (!stricmp(esdltag+4, "Struct") || !stricmp(esdltag+4, "Response"))
                    expandEsxdlStructure(trees, item, file);
            }
        }
    }
}

void outputEcl(EsdlIndexedPropertyTrees &trees, const char *file, const char *path, bool savexml)
{
    IPropertyTree **content = trees.included.getValue(file);
    if (content && *content)
    {
        DBGLOG("Generating ECL file for %s", file);
        
        const char *finger = file;
        if (!strnicmp(finger, "wsm_", 4))
            finger+=4;

        if (savexml)
        {
            StringBuffer xmlfile;
            if (path && *path)
            {
                xmlfile.append(path);
                if (xmlfile.length() && !strchr("/\\", xmlfile.charAt(xmlfile.length()-1)))
                    xmlfile.append('/');
            }
            xmlfile.append(finger).append("_expanded.xml");
            saveXML(xmlfile.str(), *content);
        }
        
        StringBuffer outfile;
        if (path && *path)
        {
            outfile.append(path);
            if (outfile.length() && !strchr("/\\", outfile.charAt(outfile.length()-1)))
                outfile.append('/');
        }
        outfile.append(finger).append(".attr");

        StringBuffer expstr;
        toXML(*content, expstr);
        Owned<IProperties> params = createProperties();
        params->setProp("sourceFileName", file);
        xsltTransform(expstr.str(), "xslt/esdl2ecl.xslt", params, outfile.str());
    }
}


void usage()
{
    puts("Usage:");
    printf(" esdl2ecl srcfile [options]\n");
    puts("Options:");
    puts("  -?/-h: display this usage");
    puts("  -x: output expanded xml files");
    puts("  -p=dir: path to output ecl files");
    puts("  -all: generate ECL files for all includes");
    
    exit(1);
}

class esdl2ecl_options
{
public:
    bool generateAllIncludes;
    bool outputExpandedXML;

    StringBuffer srcFile;
    StringBuffer outputPath;

    esdl2ecl_options() : generateAllIncludes(false), outputExpandedXML(false) {}
};

void parseCommandLineOptions(esdl2ecl_options &options, int argc, char** argv)
{
    if (argc<2)
        usage();

    options.srcFile.append(argv[1]);
    
    int i;
    for (i=2;i<argc;i++)
    {
        if (*(argv[i]) == '-')
        {
            if (!strcmp(argv[i], "-?") || !strcmp(argv[i], "-h"))
                usage();
            else if (!strcmp(argv[i],"-all"))
                options.generateAllIncludes = true;
            else if (!strcmp(argv[i],"-x"))
                options.outputExpandedXML = true;
            else if (!strncmp(argv[i],"-p=",3))
                options.outputPath.append(argv[i]+3);
            // unknown
            else {
                fprintf(stderr, "Unknown options: %s\n", argv[i]);
                exit(1);
            }               
        }
    }
}



int main(int argc, char* argv[])
{
    InitModuleObjects();

    esdl2ecl_options opts;
    parseCommandLineOptions(opts, argc, argv);

    if (opts.outputPath.length())
        recursiveCreateDirectory(opts.outputPath.str());

    StringBuffer srcPath;
    StringBuffer srcName;
    StringBuffer srcExt;

    splitFilename(opts.srcFile.str(), NULL, &srcPath, &srcName, &srcExt);

    try
    {
        unsigned start = msTick();
        EsdlIndexedPropertyTrees trees;
        trees.loadFile(srcPath.str(), srcName.str());
        DBGLOG("Time taken to load ESDL files %u", msTick() - start);

        if (opts.generateAllIncludes)
        {
            Owned<IPropertyTreeIterator> files = trees.all->getElements("esxdl");
            ForEach(*files)
            {
                IPropertyTree &incl = files->query();
                const char *src = incl.queryProp("@name");
                expandEsxdlForEclGeneration(trees, src);
                outputEcl(trees, src, opts.outputPath.str(), opts.outputExpandedXML);
            }
        }
        else
        {
            expandEsxdlForEclGeneration(trees, srcName.str());
            outputEcl(trees, srcName.str(), opts.outputPath.str(), opts.outputExpandedXML);
        }
    }
    catch(IException* e) 
    {
        StringBuffer msg;
        DBGLOG("Exception: %s", e->errorMessage(msg).str());
        e->Release();
    }
    catch(...)
    {
        DBGLOG("Unknown exception caught");
    }

    ExitModuleObjects();
    releaseAtoms();
    return 0;
}

