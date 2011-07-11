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

#ifndef FVTRANSFORM_IPP
#define FVTRANSFORM_IPP

#include "fvrelate.hpp"
#include "hqlexpr.hpp"
#include "thorplugin.hpp"

struct CardinalityElement
{
//  static const unsigned Unbounded = (unsigned)-1;     // vc6 doesn't like this.
    enum { Unbounded = -1 };        // almost the same

public:
    CardinalityElement() { min = 0; max = 0; }

    void init(unsigned len, const char * _text);

public:
    unsigned min;
    unsigned max;
    StringAttr text;
};


struct CardinalityMapping
{
public:
    inline CardinalityMapping() {}
    inline CardinalityMapping(const char * _text) { init(_text); }

    void init(const char * text);

public:
    CardinalityElement primary;
    CardinalityElement secondary;
};

void getInvertedCardinality(StringBuffer & out, const char * cardinality);

//---------------------------------------------------------------------------

//Field values are always extracted/added as utf8 values.  The transformation functions may need to translate formats to work correctly
//MORE: Support different types, and auto translate from string<->utf8<->unicode
class FILEVIEW_API ViewFieldTransformer : public CInterface
{
public:
    ViewFieldTransformer(_ATOM _name)
        : name(_name)
    {}

    inline bool matches(_ATOM search) const { return name == search; }
    inline _ATOM queryName() const { return name; }
    
    virtual ViewFieldTransformer * bind(const HqlExprArray & args);

    void transform(MemoryAttr & utfTarget, const MemoryAttr & utfSrc);

protected:
    virtual void transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source) = 0;

protected:
    _ATOM name;
};

class FILEVIEW_API ViewFailTransformer : public ViewFieldTransformer
{
public:
    ViewFailTransformer();
    
    virtual void transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source);
};


class FILEVIEW_API ViewAddTransformer : public ViewFieldTransformer
{
public:
    ViewAddTransformer();
    ViewAddTransformer(const HqlExprArray & _args);
    
    virtual void transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source);
    virtual ViewFieldTransformer * bind(const HqlExprArray & args);

protected:
    HqlExprArray args;
};


class FILEVIEW_API ViewFieldUtf8Transformer : public ViewFieldTransformer
{
public:
    ViewFieldUtf8Transformer(_ATOM _name, utf8FieldTransformerFunction _function)
        : ViewFieldTransformer(_name), function(_function)
    {}

protected:
    virtual void transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source);

protected:
    utf8FieldTransformerFunction function;
};



class FILEVIEW_API ViewFieldUnicodeTransformer : public ViewFieldTransformer
{
public:
    ViewFieldUnicodeTransformer(_ATOM _name, unicodeFieldTransformerFunction _function)
        : ViewFieldTransformer(_name), function(_function)
    {}

protected:
    virtual void transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source);

protected:
    unicodeFieldTransformerFunction function;
};



class FILEVIEW_API ViewFieldStringTransformer : public ViewFieldTransformer
{
public:
    ViewFieldStringTransformer(_ATOM _name, stringFieldTransformerFunction _function)
        : ViewFieldTransformer(_name), function(_function)
    {}

protected:
    virtual void transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source);

protected:
    stringFieldTransformerFunction function;
};

class FILEVIEW_API ViewFieldECLTransformer : public ViewFieldTransformer
{
    friend class ViewFieldBoundECLTransformer;
public:
    ViewFieldECLTransformer(IHqlExpression * _function)
        : ViewFieldTransformer(_function->queryName()), function(_function)
    {}

protected:
    virtual void transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source);
    virtual ViewFieldTransformer * bind(const HqlExprArray & args);

    void transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source, const HqlExprArray & extraArgs);

protected:
    LinkedHqlExpr function;
};

class FILEVIEW_API ViewFieldBoundECLTransformer : public ViewFieldTransformer
{
public:
    ViewFieldBoundECLTransformer(ViewFieldECLTransformer * _transformer, const HqlExprArray & _args)
        : ViewFieldTransformer(_transformer->queryName()), transformer(_transformer)
    {
        appendArray(args, _args);
    }

protected:
    virtual void transform(unsigned & lenTarget, char * & target, unsigned lenSource, const char * source);

protected:
    Linked<ViewFieldECLTransformer> transformer;
    HqlExprArray args;
};

typedef CIArrayOf<ViewFieldTransformer> ViewFieldTransformerArray;

class ViewServiceEntry : public CInterface
{
public:
    StringAttr name;
    //HINSTANCE handle;         // to prevent it being loaded and unloaded.
    ViewFieldTransformerArray transformers;
};

class FILEVIEW_API ViewTransformerRegistry : implements IViewTransformerRegistry
{
public:
    virtual void addFieldStringTransformer(const char * name, stringFieldTransformerFunction func);
    virtual void addFieldUtf8Transformer(const char * name, utf8FieldTransformerFunction func);
    virtual void addFieldUnicodeTransformer(const char * name, unicodeFieldTransformerFunction func);
    virtual void addPlugins(const char * pluginname);
            
    void addTransformer(ViewFieldTransformer * ownedTransformer);

    ViewFieldTransformer * resolve(const char * name, const HqlExprArray & args);
    ViewFieldTransformer * resolve(const char * servicename, const char * functionname, const HqlExprArray & args);

protected:
    void addServiceDefinition(IHqlExpression * service);
    void * resolveExternal(IHqlExpression * funcdef);
    ViewFieldTransformer * createTransformer(IHqlExpression * funcdef);

protected:
    //Could use a hash table if the number of entries is likely to get large, unlikely to be faster though
    ViewFieldTransformerArray transformers;
    CIArrayOf<ViewServiceEntry> plugins;
    Owned<SafePluginMap> loadedPlugins;
    Owned<IEclRepository> dataServer;
    SimplePluginCtx pluginCtx;
};


//---------------------------------------------------------------------------

typedef CIArrayOf<ViewFieldTransformer> ViewFieldTransformerArray;
void translateValue(MemoryAttr & result, const MemoryAttr & value, const ViewFieldTransformerArray & transforms);

bool containsFail(const ViewFieldTransformerArray & transforms);

class FILEVIEW_API ViewJoinColumn : public CInterface
{
public:
    ViewJoinColumn(unsigned _whichColumn, const ViewFieldTransformerArray & _getTransforms, const ViewFieldTransformerArray & _setTransforms);

    void addFilter(IFilteredResultSet * resultSet, const MemoryAttr & value);
    void clearFilter(IFilteredResultSet * resultSet);
    void getValue(MemoryAttr & value, IResultSetCursor * cursor);
    unsigned queryBaseColumn() const { return whichColumn; }

    bool canGet() const { return !getContainsFail; }
    bool canSet() const { return !setContainsFail; }
    bool hasGetTranslation() const { return getTransforms.ordinality() != 0; }
    bool hasSetTranslation() const { return setTransforms.ordinality() != 0; }

protected:
    unsigned whichColumn;
    ViewFieldTransformerArray getTransforms;
    ViewFieldTransformerArray setTransforms;
    bool getContainsFail;
    bool setContainsFail;
};
typedef CIArrayOf<ViewJoinColumn> ViewJoinColumnArray;


struct FieldTransformInfo : public CInterface
{
//output 
    unsigned datasetColumn;
    unsigned column;
    ViewFieldTransformerArray getTransforms;
    ViewFieldTransformerArray setTransforms;
    StringAttr naturalName;
    Owned<ITypeInfo> type;
};

typedef CIArrayOf<FieldTransformInfo> FieldTransformInfoArray;

void parseFileColumnMapping(FieldTransformInfoArray & mappings, const char * text, const IResultSetMetaData & fieldMeta);
void parseColumnMappingList(FieldTransformInfoArray & results,
                            const IResultSetMetaData & fieldMeta,
                            bool isDatasetAllowed,      // if non null dataset.x is allowed, and column returned via pointer
                            const char * text);

#endif
