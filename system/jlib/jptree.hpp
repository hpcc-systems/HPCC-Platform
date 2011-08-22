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


#ifndef _PTREE_HPP
#define _PTREE_HPP

#include "jexpdef.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jiter.hpp"

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

    virtual bool getPropBin(const char *xpath, MemoryBuffer &ret) const = 0;
    virtual void setPropBin(const char *xpath, size32_t size, const void *data) = 0;
    virtual void addPropBin(const char *xpath, size32_t size, const void *data) = 0;
    virtual void appendPropBin(const char *xpath, size32_t size, const void *data) = 0;

    virtual IPropertyTree *getPropTree(const char *xpath) const = 0;
    virtual IPropertyTree *queryPropTree(const char *xpath) const = 0;
    virtual IPropertyTree *setPropTree(const char *xpath, IPropertyTree *val) = 0;
    virtual IPropertyTree *addPropTree(const char *xpath, IPropertyTree *val) = 0;

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
    virtual unsigned numUniq() = 0;
    virtual unsigned numChildren() = 0;
    virtual bool isCaseInsensitive() = 0;
    virtual bool IsShared() const = 0;
    virtual void localizeElements(const char *xpath, bool allTail=false) = 0;
    
private:
    void setProp(const char *, int); // dummy to catch accidental use of setProp when setPropInt() intended
    void addProp(const char *, int); // likewise
};

jlib_decl bool validateXMLTag(const char *name);

interface IPTreeNotifyEvent : extends IInterface
{
    virtual void beginNode(const char *tag, offset_t startOffset) = 0;
    virtual void newAttribute(const char *name, const char *value) = 0;
    virtual void beginNodeContent(const char *tag) = 0; // attributes parsed
    virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset) = 0;
};

enum XmlReadExcptCode { XmlRead_undefined, XmlRead_EOS, XmlRead_syntax };
interface jlib_thrown_decl IXMLReadException : extends IException
{
    virtual const char *queryDescription() = 0;
    virtual unsigned queryLine() = 0;
    virtual offset_t queryOffset() = 0;
    virtual const char *queryContext() = 0;
};
extern jlib_decl IXMLReadException *createXmlReadException(int code, const char *msg, const char *context, unsigned line, offset_t offset);

enum XmlReaderOptions { xr_none=0x00, xr_ignoreWhiteSpace=0x01, xr_noRoot=0x02, xr_ignoreNameSpaces=0x04 };

interface IXMLReader : extends IInterface
{
    virtual void load() = 0;
    virtual offset_t queryOffset() = 0;
};

interface IPullXMLReader : extends IXMLReader
{
    virtual bool next() = 0;
    virtual void reset() = 0;
};

interface IPTreeMaker : extends IPTreeNotifyEvent
{
    virtual IPropertyTree *queryRoot() = 0;
    virtual IPropertyTree *queryCurrentNode() = 0;
    virtual void reset() = 0;
};

interface IPTreeNodeCreator : extends IInterface
{
    virtual IPropertyTree *create(const char *tag) = 0;
};

// NB ipt_ext5 - used by SDS
enum ipt_flags { ipt_none=0x00, ipt_caseInsensitive=0x01, ipt_binary=0x02, ipt_ordered=0x04, ipt_ext1=0x08, ipt_ext2=16, ipt_ext3=32, ipt_ext4=64, ipt_ext5=128 };
jlib_decl IPTreeMaker *createPTreeMaker(byte flags=ipt_none, IPropertyTree *root=NULL, IPTreeNodeCreator *nodeCreator=NULL);
jlib_decl IPTreeMaker *createRootLessPTreeMaker(byte flags=ipt_none, IPropertyTree *root=NULL, IPTreeNodeCreator *nodeCreator=NULL);
jlib_decl IXMLReader *createXMLStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, XmlReaderOptions xmlReaderOptions=xr_ignoreWhiteSpace, size32_t bufSize=0);
jlib_decl IXMLReader *createXMLStringReader(const char *xml, IPTreeNotifyEvent &iEvent, XmlReaderOptions xmlReaderOptions=xr_ignoreWhiteSpace);
jlib_decl IXMLReader *createXMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, XmlReaderOptions xmlReaderOptions=xr_ignoreWhiteSpace);
jlib_decl IPullXMLReader *createPullXMLStreamReader(ISimpleReadStream &stream, IPTreeNotifyEvent &iEvent, XmlReaderOptions xmlReaderOptions=xr_ignoreWhiteSpace, size32_t bufSize=0);
jlib_decl IPullXMLReader *createPullXMLStringReader(const char *xml, IPTreeNotifyEvent &iEvent, XmlReaderOptions xmlReaderOptions=xr_ignoreWhiteSpace);
jlib_decl IPullXMLReader *createPullXMLBufferReader(const void *buf, size32_t bufLength, IPTreeNotifyEvent &iEvent, XmlReaderOptions xmlReaderOptions=xr_ignoreWhiteSpace);

jlib_decl void mergePTree(IPropertyTree *target, IPropertyTree *toMerge);
jlib_decl void synchronizePTree(IPropertyTree *target, IPropertyTree *source);
jlib_decl IPropertyTree *ensurePTree(IPropertyTree *root, const char *xpath);
jlib_decl bool areMatchingPTrees(IPropertyTree * left, IPropertyTree * right);

jlib_decl IPropertyTree *createPTree(MemoryBuffer &src);

jlib_decl IPropertyTree *createPTree(byte flags=ipt_none);
jlib_decl IPropertyTree *createPTree(const char *name, byte flags=ipt_none);
jlib_decl IPropertyTree *createPTree(IFile &ifile, byte flags=ipt_none, XmlReaderOptions readFlags=xr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTree(IFileIO &ifileio, byte flags=ipt_none, XmlReaderOptions readFlags=xr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTree(ISimpleReadStream &stream, byte flags=ipt_none, XmlReaderOptions readFlags=xr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromXMLString(const char *xml, byte flags=ipt_none, XmlReaderOptions readFlags=xr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromXMLString(unsigned len, const char *xml, byte flags=ipt_none, XmlReaderOptions readFlags=xr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromXMLFile(const char *filename, byte flags=ipt_none, XmlReaderOptions readFlags=xr_ignoreWhiteSpace, IPTreeMaker *iMaker=NULL);
jlib_decl IPropertyTree *createPTreeFromIPT(const IPropertyTree *srcTree, ipt_flags flags=ipt_none);

#define XML_SortTags 0x01
#define XML_Format   0x02
#define XML_NoEncode 0x04
#define XML_Sanitize 0x08
#define XML_SanitizeAttributeValues 0x10
#define XML_SingleQuoteAttributeValues 0x20
#define XML_NoBinaryEncode64 0x40

jlib_decl StringBuffer &toXML(const IPropertyTree *tree, StringBuffer &ret, unsigned indent = 0, byte flags=XML_Format);
jlib_decl void toXML(const IPropertyTree *tree, IIOStream &out, unsigned indent = 0, byte flags=XML_Format);
jlib_decl void saveXML(const char *filename, const IPropertyTree *tree, unsigned indent = 0, byte flags=XML_Format);
jlib_decl void saveXML(IFile &ifile, const IPropertyTree *tree, unsigned indent = 0, byte flags=XML_Format);
jlib_decl void saveXML(IFileIO &ifileio, const IPropertyTree *tree, unsigned indent = 0, byte flags=XML_Format);
jlib_decl void saveXML(IIOStream &stream, const IPropertyTree *tree, unsigned indent = 0, byte flags=XML_Format);

jlib_decl const char *splitXPath(const char *xpath, StringBuffer &head); // returns tail, fills 'head' with leading xpath
jlib_decl bool validateXPathSyntax(const char *xpath, StringBuffer *error=NULL);
jlib_decl bool validateXMLParseXPath(const char *xpath, StringBuffer *error=NULL);
jlib_decl IPropertyTree *getXPathMatchTree(IPropertyTree &parent, const char *xpath);
jlib_decl IPropertyTreeIterator *createNullPTreeIterator();
jlib_decl bool isEmptyPTree(IPropertyTree *t);

jlib_decl void extractJavadoc(IPropertyTree * result, const char * text);       // Pass in a javadoc style comment (without head/tail) and extract information into a property tree.

typedef IPropertyTree IPTree;
typedef IPropertyTreeIterator IPTreeIterator;
typedef Owned<IPTree> OwnedPTree;

#endif
