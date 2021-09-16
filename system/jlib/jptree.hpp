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


#ifndef _PTREE_HPP
#define _PTREE_HPP

#include <vector>
#include <functional>

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jiter.hpp"
#include "jprop.hpp"

#include <initializer_list>

enum TextMarkupFormat
{
    MarkupFmt_Unknown=0,
    MarkupFmt_XML,
    MarkupFmt_JSON,
    MarkupFmt_URL
};
enum PTreeExceptionCodes
{
    PTreeExcpt_XPath_Ambiguity,
    PTreeExcpt_XPath_ParseError,
    PTreeExcpt_XPath_Unsupported,
    PTreeExcpt_InvalidTagName,
    PTreeExcpt_Unsupported,
    PTreeExcpt_InternalError,
};
interface jlib_thrown_decl IPTreeException : extends IException { };
interface IAttributeIterator : extends IInterface
{
    virtual bool first()=0;
    virtual bool next()=0;
    virtual bool isValid()=0;
    virtual unsigned count()=0;
    virtual const char *queryName() const = 0;
    virtual const char *queryValue() const = 0;
    virtual StringBuffer &getValue(StringBuffer &out) = 0;
};

interface IPropertyTree;
interface IPropertyTreeIterator : extends IIteratorOf<IPropertyTree> { };

typedef unsigned IPTIteratorCodes;
#define iptiter_null 0x00
#define iptiter_sort 0x01
#define iptiter_remote 0x02
#define iptiter_remoteget 0x06
#define iptiter_remotegetbranch 0x0e

extern jlib_decl unsigned queryNumLocalTrees();
extern jlib_decl unsigned queryNumAtomTrees();

interface jlib_decl IPropertyTree : extends serializable
{
    virtual bool hasProp(const char *xpath) const = 0;
    virtual bool isBinary(const char *xpath=NULL) const = 0;
    virtual bool isCompressed(const char *xpath=NULL) const = 0; // needed for external/internal efficiency e.g. for clone
    virtual bool renameProp(const char *xpath, const char *newName) = 0;
    virtual bool renameTree(IPropertyTree *tree, const char *newName) = 0;

    // string - all types can be converted to string
    virtual bool getProp(const char *xpath, StringBuffer &ret) const = 0;
    virtual const char *queryProp(const char * xpath) const = 0;
    virtual void setProp(const char *xpath, const char *val) = 0;
    virtual void addProp(const char *xpath, const char *val) = 0;
    virtual void appendProp(const char *xpath, const char *val) = 0;
    
    virtual bool getPropBool(const char *xpath, bool dft=false) const = 0;
    virtual void setPropBool(const char *xpath, bool val) = 0;
    virtual void addPropBool(const char *xpath, bool val) = 0;

    virtual int  getPropInt(const char *xpath, int dft=0) const = 0;
    virtual void setPropInt(const char *xpath, int val) = 0;
    virtual void addPropInt(const char *xpath, int val) = 0;

    virtual __int64 getPropInt64(const char *xpath, __int64 dft=0) const = 0;
    virtual void setPropInt64(const char *xpath, __int64 val) = 0;
    virtual void addPropInt64(const char *xpath, __int64 val) = 0;

    virtual double getPropReal(const char *xpath, double dft=0.0) const = 0;

    virtual bool getPropBin(const char *xpath, MemoryBuffer &ret) const = 0;
    virtual void setPropBin(const char *xpath, size32_t size, const void *data) = 0;
    virtual void addPropBin(const char *xpath, size32_t size, const void *data) = 0;
    virtual void appendPropBin(const char *xpath, size32_t size, const void *data) = 0;

    virtual IPropertyTree *getPropTree(const char *xpath) const = 0;
    virtual IPropertyTree *queryPropTree(const char *xpath) const = 0;
    virtual IPropertyTree *setPropTree(const char *xpath, IPropertyTree *val) = 0;
    virtual IPropertyTree *addPropTree(const char *xpath, IPropertyTree *val) = 0;

    virtual IPropertyTree *setPropTree(const char *xpath) = 0;
    virtual IPropertyTree *addPropTree(const char *xpath) = 0;

    virtual bool removeProp(const char *xpath) = 0;
    virtual bool removeTree(IPropertyTree *child) = 0;
    virtual aindex_t queryChildIndex(IPropertyTree *child) = 0;

    virtual StringBuffer &getName(StringBuffer &) const = 0;
    virtual const char *queryName() const = 0;

    virtual IPropertyTreeIterator *getElements(const char *xpath, IPTIteratorCodes flags = iptiter_null) const = 0;
    virtual IAttributeIterator *getAttributes(bool sorted=false) const = 0;

    virtual IPropertyTree *getBranch(const char *xpath) const = 0;
    virtual IPropertyTree *queryBranch(const char *xpath) const = 0;
    virtual bool hasChildren() const = 0;
    virtual unsigned numUniq() const = 0;
    virtual unsigned numChildren() const = 0;
    virtual bool isCaseInsensitive() const = 0;
    virtual bool IsShared() const = 0;
    virtual void localizeElements(const char *xpath, bool allTail=false) = 0;
    virtual unsigned getCount(const char *xpath) const = 0;
    virtual IPropertyTree *addPropTreeArrayItem(const char *xpath, IPropertyTree *val) = 0;
    virtual bool isArray(const char *xpath=NULL) const = 0;
    virtual unsigned getAttributeCount() const = 0;

private:
    void setProp(const char *, int); // dummy to catch accidental use of setProp when setPropInt() intended
    void addProp(const char *, int); // likewise
};

jlib_decl bool validateXMLTag(const char *name);

interface IPTreeNotifyEvent : extends IInterface
{
    virtual void beginNode(const char *tag, bool sequence, offset_t startOffset) = 0;
    virtual void newAttribute(const char *name, const char *value) = 0;
    virtual void beginNodeContent(const char *tag) = 0; // attributes parsed
    virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset) = 0;
};

enum PTreeReadExcptCode { PTreeRead_undefined, PTreeRead_EOS, PTreeRead_syntax };
interface jlib_thrown_decl IPTreeReadException : extends IException
{
    virtual const char *queryDescription() = 0;
    virtual unsigned queryLine() = 0;
    virtual offset_t queryOffset() = 0;
    virtual const char *queryContext() = 0;
};
extern jlib_decl IPTreeReadException *createPTreeReadException(int code, const char *msg, const char *context, unsigned line, offset_t offset);

enum PTreeReaderOptions { ptr_none=0x00, ptr_ignoreWhiteSpace=0x01, ptr_noRoot=0x02, ptr_ignoreNameSpaces=0x04 };

interface IPTreeReader : extends IInterface
{
    virtual void load() = 0;
    virtual offset_t queryOffset() = 0;
};

interface IPullPTreeReader : extends IPTreeReader
{
    virtual bool next() = 0;
    virtual void reset() = 0;
};

interface IPTreeMaker : extends IPTreeNotifyEvent
{
    virtual IPropertyTree *queryRoot() = 0;
    virtual IPropertyTree *queryCurrentNode() = 0;
    virtual void reset() = 0;
    virtual IPropertyTree *create(const char *tag) = 0;
};

interface IPTreeNodeCreator : extends IInterface
{
    virtual IPropertyTree *create(const char *tag) = 0;
};

enum ipt_flags
{
    ipt_none=0x00,
    ipt_caseInsensitive=0x01,
    ipt_binary  = 0x02,
    ipt_ordered = 0x04,   // Preserve element ordering
    ipt_fast    = 0x08,   // Prioritize speed over low memory usage
    ipt_lowmem  = 0x10,   // Prioritize low memory usage over speed
    ipt_ext3    = 0x20,   // Unused
    ipt_ext4    = 0x40,   // Used internally in Dali
    ipt_ext5    = 0x80    // Used internally in Dali
};

jlib_decl IPTreeMaker *createPTreeMaker(byte flags=ipt_none, IPropertyTree *root=NULL, IPTreeNodeCreator *nodeCreator=NULL);
jlib_decl IPTreeMaker *createRootLessPTreeMaker(byte flags=ipt_none, IPropertyTree *root=NULL, IPTreeNodeCreator *nodeCreator=NULL);
jlib_decl IPTreeReader *createXMLStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions=ptr_ignoreWhiteSpace, size32_t bufSize=0);
jlib_decl IPTreeReader *createXMLStringReader(const char *xml, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions=ptr_ignoreWhiteSpace);
jlib_decl IPTreeReader *createXMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions=ptr_ignoreWhiteSpace);
jlib_decl IPullPTreeReader *createPullXMLStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions=ptr_ignoreWhiteSpace, size32_t bufSize=0);
jlib_decl IPullPTreeReader *createPullXMLStringReader(const char *xml, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions=ptr_ignoreWhiteSpace);
jlib_decl IPullPTreeReader *createPullXMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions xmlReaderOptions=ptr_ignoreWhiteSpace);

jlib_decl IPTreeReader *createJSONStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions=ptr_ignoreWhiteSpace, size32_t bufSize=0);
jlib_decl IPTreeReader *createJSONStringReader(const char *json, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions=ptr_ignoreWhiteSpace);
jlib_decl IPTreeReader *createJSONBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions jsonReaderOptions=ptr_ignoreWhiteSpace);
jlib_decl IPullPTreeReader *createPullJSONStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions=ptr_ignoreWhiteSpace, size32_t bufSize=0);
jlib_decl IPullPTreeReader *createPullJSONStringReader(const char *json, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions=ptr_ignoreWhiteSpace);
jlib_decl IPullPTreeReader *createPullJSONBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, PTreeReaderOptions readerOptions=ptr_ignoreWhiteSpace);

jlib_decl void mergePTree(IPropertyTree *target, IPropertyTree *toMerge);
jlib_decl void synchronizePTree(IPropertyTree *target, const IPropertyTree *source, bool removeTargetsNotInSource=true, bool rootsMustMatch=true);
jlib_decl IPropertyTree *ensurePTree(IPropertyTree *root, const char *xpath);
jlib_decl bool areMatchingPTrees(const IPropertyTree * left, const IPropertyTree * right);

//Similar to ptree->addProp(name, value) except it ensures the appended item is part of a list.  Ensures that YAML is regenerated correctly.
jlib_decl void addPTreeItem(IPropertyTree *ptree, const char * name, const char * value);

jlib_decl IPropertyTree *createPTree(MemoryBuffer &src, byte flags=ipt_none);

jlib_decl IPropertyTree *createPTree(byte flags=ipt_none);
jlib_decl IPropertyTree *createPTree(const char *name, byte flags=ipt_none);
jlib_decl IPropertyTree *createPTree(IFile &ifile, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTree(IFileIO &ifileio, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTree(ISimpleReadStream &stream, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromXMLString(const char *xml, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromXMLString(unsigned len, const char *xml, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromXMLFile(const char *filename, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromIPT(const IPropertyTree *srcTree, ipt_flags flags=ipt_none);
jlib_decl IPropertyTree *createPTreeFromJSONString(const char *json, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromJSONString(unsigned len, const char *json, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);

//URL node nameWithAttrs is of the form: "TagName/attr1('abc')/attr2/attr3('')"
jlib_decl IPropertyTree *createPTreeFromHttpPath(const char *nameWithAttrs, IPropertyTree *content, bool nestedRoot, ipt_flags flags);
jlib_decl IPropertyTree *createPTreeFromHttpParameters(const char *nameWithAttrs, IProperties *parameters, bool skipLeadingDotParameters, bool nestedRoot, ipt_flags flags=ipt_none);
jlib_decl bool checkParseUrlPathNodeValue(const char *s, StringBuffer &name, StringAttr &value);

typedef int (*TreeCompareFunc)(IInterface * const *ll, IInterface * const *rr);
jlib_decl IPropertyTreeIterator * createSortedIterator(IPropertyTreeIterator & iter, TreeCompareFunc compare);
jlib_decl IPropertyTreeIterator * createSortedIterator(IArrayOf<IPropertyTree> & ownedElems, TreeCompareFunc compare);


#define XML_SortTags 0x01
#define XML_Embed    0x02
#define XML_NoEncode 0x04
#define XML_Sanitize 0x08
#define XML_SanitizeAttributeValues 0x10
#define XML_SingleQuoteAttributeValues 0x20
#define XML_NoBinaryEncode64 0x40
#define XML_LineBreak 0x100
#define XML_LineBreakAttributes 0x80
#define XML_Format (XML_Embed|XML_LineBreakAttributes|XML_LineBreak)

jlib_decl StringBuffer &toXML(const IPropertyTree *tree, StringBuffer &ret, unsigned indent = 0, unsigned flags=XML_Format);
jlib_decl void toXML(const IPropertyTree *tree, IIOStream &out, unsigned indent = 0, unsigned flags=XML_Format);
jlib_decl void saveXML(const char *filename, const IPropertyTree *tree, unsigned indent = 0, unsigned flags=XML_Format);
jlib_decl void saveXML(IFile &ifile, const IPropertyTree *tree, unsigned indent = 0, unsigned=XML_Format);
jlib_decl void saveXML(IFileIO &ifileio, const IPropertyTree *tree, unsigned indent = 0, unsigned flags=XML_Format);
jlib_decl void saveXML(IIOStream &stream, const IPropertyTree *tree, unsigned indent = 0, unsigned flags=XML_Format);
jlib_decl void printXML(const IPropertyTree *tree, unsigned indent = 0, unsigned flags=XML_Format);
jlib_decl void dbglogXML(const IPropertyTree *tree, unsigned indent = 0, unsigned flags=XML_Format);

#define JSON_SortTags XML_SortTags
#define JSON_Format   0x02
#define JSON_HideRootArrayObject 0x04
#define JSON_Sanitize XML_Sanitize
#define JSON_SanitizeAttributeValues XML_SanitizeAttributeValues

jlib_decl StringBuffer &toJSON(const IPropertyTree *tree, StringBuffer &ret, unsigned indent = 0, byte flags=JSON_Format|JSON_HideRootArrayObject);
jlib_decl void toJSON(const IPropertyTree *tree, IIOStream &out, unsigned indent = 0, byte flags=JSON_Format|JSON_HideRootArrayObject);
jlib_decl void printJSON(const IPropertyTree *tree, unsigned indent = 0, byte flags=JSON_Format|JSON_HideRootArrayObject);
jlib_decl void dbglogJSON(const IPropertyTree *tree, unsigned indent = 0, byte flags=JSON_Format|JSON_HideRootArrayObject);

jlib_decl const char *splitXPath(const char *xpath, StringBuffer &head); // returns tail, fills 'head' with leading xpath
jlib_decl bool validateXPathSyntax(const char *xpath, StringBuffer *error=NULL);
jlib_decl bool validateXMLParseXPath(const char *xpath, StringBuffer *error=NULL);
jlib_decl IPropertyTree *getXPathMatchTree(IPropertyTree &parent, const char *xpath);
jlib_decl IPropertyTreeIterator *createNullPTreeIterator();
jlib_decl bool isEmptyPTree(const IPropertyTree *t);

jlib_decl void extractJavadoc(IPropertyTree * result, const char * text);       // Pass in a javadoc style comment (without head/tail) and extract information into a property tree.

typedef IPropertyTree IPTree;
typedef IPropertyTreeIterator IPTreeIterator;
typedef Owned<IPTree> OwnedPTree;

// http://www.w3.org/TR/REC-xml#xml-names
static const char *validStartChrs = ":_";
inline static bool isValidXPathStartChr(char c)
{
    return ('\0' != c && (isalpha(c) || strchr(validStartChrs, c)));
}

static const char *validChrs = ":_.-";
inline static bool isValidXPathChr(char c)
{
    return ('\0' != c && (isalnum(c) || strchr(validChrs, c)));
}

//export for unit test
jlib_decl void mergeConfiguration(IPropertyTree & target, const IPropertyTree & source, const char *altNameAttribute=nullptr, bool overwriteAttr=true);

jlib_decl IPropertyTree * loadArgsIntoConfiguration(IPropertyTree *config, const char * * argv, std::initializer_list<const char *> ignoreOptions = {});
jlib_decl IPropertyTree * loadConfiguration(IPropertyTree * defaultConfig, const char * * argv, const char * componentTag, const char * envPrefix, const char * legacyFilename, IPropertyTree * (mapper)(IPropertyTree *), const char *altNameAttribute=nullptr, bool monitor=true);
jlib_decl IPropertyTree * loadConfiguration(const char * defaultYaml, const char * * argv, const char * componentTag, const char * envPrefix, const char * legacyFilename, IPropertyTree * (mapper)(IPropertyTree *), const char *altNameAttribute=nullptr, bool monitor=true);
jlib_decl IPropertyTree * getCostsConfiguration();

//The following can only be called after loadConfiguration has been called.  All components must call loadConfiguration().
jlib_decl IPropertyTree * getGlobalConfig();
jlib_decl IPropertyTree * getComponentConfig();
jlib_decl Owned<IPropertyTree> getGlobalConfigSP(); // get smart pointer
jlib_decl Owned<IPropertyTree> getComponentConfigSP(); // get smart pointer

// ConfigUpdateFunc calls are made in a mutex, but after new confis are swapped in
typedef std::function<void (const IPropertyTree *oldComponentConfiguration, const IPropertyTree *oldGlobalConfiguration)> ConfigUpdateFunc;
jlib_decl unsigned installConfigUpdateHook(ConfigUpdateFunc notifyFunc);
jlib_decl void removeConfigUpdateHook(unsigned notifyFuncId);

class jlib_decl CConfigUpdateHook
{
    std::atomic<unsigned> configCBId{(unsigned)-1};
    CriticalSection crit;
public:
    ~CConfigUpdateHook() { clear(); }
    void clear();
    void installOnce(ConfigUpdateFunc callbackFunc, bool callWhenInstalled);
};

/*
 YAML to PTree support
   By default YAML scalars become PTree attributes unless the YAML has an !element or !el YAML tag specifying that the scalar
   should be treated as an element.  Mixed content can be represented in YAML using a scalar named "^" also using the !el YAML tag

 Person:
   name: Adam Smith
   note: Father of Economics

 Becomes:
  <Person name="Adam Smith" note="Father of Economics"/>

 Person:
   name: Adam Smith
   note: !el Father of Economics

 Becomes:
  <Person name="Adam Smith">
   <note>Father of Economics</note>
  </Person>

 Person:
   name: Adam Smith
   ^: !el Father of Economics

 Becomes:
  <Person name="Adam Smith">Father of Economics<Person/>
*/

jlib_decl IPropertyTree *createPTreeFromYAMLString(const char *yaml, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromYAMLString(unsigned len, const char *yaml, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromYAMLFile(const char *filename, byte flags=ipt_none, PTreeReaderOptions readFlags=ptr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);

#define YAML_HideRootArrayObject 0x04
#define YAML_SortTags XML_SortTags
#define YAML_Sanitize XML_Sanitize
#define YAML_SanitizeAttributeValues XML_SanitizeAttributeValues

jlib_decl StringBuffer &toYAML(const IPropertyTree *tree, StringBuffer &ret, unsigned indent, byte flags);
jlib_decl void toYAML(const IPropertyTree *tree, IIOStream &out, unsigned indent, byte flags);

jlib_decl void saveYAML(const char *filename, const IPropertyTree *tree, unsigned indent = 0, unsigned flags=YAML_HideRootArrayObject);
jlib_decl void saveYAML(IFile &ifile, const IPropertyTree *tree, unsigned indent = 0, unsigned=YAML_HideRootArrayObject);
jlib_decl void saveYAML(IFileIO &ifileio, const IPropertyTree *tree, unsigned indent = 0, unsigned flags=YAML_HideRootArrayObject);
jlib_decl void saveYAML(IIOStream &stream, const IPropertyTree *tree, unsigned indent = 0, unsigned flags=YAML_HideRootArrayObject);
jlib_decl void printYAML(const IPropertyTree *tree, unsigned indent = 0, unsigned flags=YAML_HideRootArrayObject);
jlib_decl void dbglogYAML(const IPropertyTree *tree, unsigned indent = 0, unsigned flags=YAML_HideRootArrayObject);

// Defines the threshold where attribute value maps are created for sibling ptrees for fast lookups
jlib_decl void setPTreeMappingThreshold(unsigned threshold);

jlib_decl void copyPropIfMissing(IPropertyTree & target, const char * targetName, IPropertyTree & source, const char * sourceName);

#endif
