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

#ifndef ESDLDEF_HPP
#define ESDLDEF_HPP

#include "jliball.hpp"
#include "jqueue.tpp"

#ifdef _WIN32
 #ifdef ESDLLIB_EXPORTS
  #define esdl_decl __declspec(dllexport)
 #else
  #define esdl_decl
 #endif
#else
 #define esdl_decl
#endif

typedef enum
{
    ESDLT_UNKOWN,
    ESDLT_STRUCT,
    ESDLT_REQUEST,
    ESDLT_RESPONSE,
    ESDLT_COMPLEX,
    ESDLT_STRING,
    ESDLT_INT8,
    ESDLT_INT16,
    ESDLT_INT32,
    ESDLT_INT64,
    ESDLT_UINT8,
    ESDLT_UINT16,
    ESDLT_UINT32,
    ESDLT_UINT64,
    ESDLT_BOOL,
    ESDLT_FLOAT,
    ESDLT_DOUBLE,
    ESDLT_BYTE,
    ESDLT_UBYTE
} EsdlBasicElementType;

typedef enum EsdlDefTypeId_
{
    EsdlTypeElement,
    EsdlTypeAttribute,
    EsdlTypeArray,
    EsdlTypeEnumDef,
    EsdlTypeEnumRef,
    EsdlTypeStruct,
    EsdlTypeRequest,
    EsdlTypeResponse,
    EsdlTypeMethod,
    EsdlTypeService,
    EsdlTypeVersion,
    EsdlTypeEnumDefItem
} EsdlDefTypeId;

#define DEPFLAG_COLLAPSE    (0x01)
#define DEPFLAG_ARRAYOF     (0x02)
#define DEPFLAG_STRINGARRAY (0x04)    // Set dynamically by gatherDependencies to indicate stylesheet should output
                                    // an EspStringArray structure definition.

interface IEsdlDefObject : extends IInterface
{
    virtual const char *queryName()=0;
    virtual const char *queryProp(const char *name)=0;
    virtual bool hasProp(const char *name)=0;
    virtual int getPropInt(const char *pname, int def = 0)=0;
    virtual void toXML(StringBuffer &xml, double version = 0, IProperties *opts=NULL, unsigned flags=0)=0;
    virtual EsdlDefTypeId getEsdlType()=0;
    virtual bool checkVersion(double ver)=0;
    virtual bool checkFLVersion(double ver)=0;
    virtual bool checkOptional(IProperties *opts)=0;
    virtual IPropertyIterator* getProps()=0;
};

typedef IEsdlDefObject * IEsdlDefObjPtr;

interface IEsdlDefObjectIterator : extends IInterface
{
    virtual bool        first(void)=0;
    virtual bool        next(void)=0;
    virtual bool        isValid(void)=0;
    virtual unsigned    getFlags(void)=0;
    virtual IEsdlDefObject& query()=0;
    virtual IEsdlDefObjectIterator* queryBaseTypesIterator()=0;
};

interface IEsdlDefAttribute : extends IEsdlDefObject
{
};

interface IEsdlDefElement : extends IEsdlDefObject
{
    virtual IProperties *queryRecSelectors()=0;
    virtual IPropertyTree *queryPtdSelectors()=0;
};

interface IEsdlDefArray : extends IEsdlDefObject
{
    virtual IProperties *queryRecSelectors()=0;
};

interface IEsdlDefEnumDef : extends IEsdlDefObject
{
};

interface IEsdlDefEnumRef : extends IEsdlDefObject
{
};

interface IEsdlDefEnumDefItem : extends IEsdlDefObject
{
};

interface IEsdlDefVersion : extends IEsdlDefObject
{
};

interface IEsdlDefStruct : extends IEsdlDefObject
{
    virtual IEsdlDefObjectIterator *getChildren()=0;
    virtual IEsdlDefObject *queryChild(const char *name, bool nocase=false)=0;
};



interface IEsdlDefMethod : extends IEsdlDefObject
{
    virtual const char *queryRequestType()=0;
    virtual const char *queryResponseType()=0;
    virtual const char *queryProductAssociation()=0;
    virtual bool isProductDefault()=0;
    virtual const char *queryLogMethodName()=0;
    virtual const char *queryMethodFLReqFormat()=0;
    virtual const char *queryMethodFLRespFormat()=0;
    virtual const char *queryMethodName()=0;
    virtual const char *queryMbsiProductAssociations()=0;
    virtual int queryVariableLengthRecordProcessing()=0;
    virtual int queryAllowMultipleEntryPerUnitNumber()=0;
};

interface IEsdlDefMethodIterator : extends IInterface
{
    virtual bool        first(void)=0;
    virtual bool        next(void)=0;
    virtual bool        isValid(void)=0;
    virtual IEsdlDefMethod &query()=0;
};

interface IEsdlDefService : extends IEsdlDefObject
{
    virtual IEsdlDefMethodIterator *getMethods()=0;
    virtual IEsdlDefMethod *queryMethodByName(const char *name)=0;
    virtual IEsdlDefMethod *queryMethodByRequest(const char *reqname)=0;
    virtual void methodsNamesToXML(StringBuffer& xml, const char* ver, IProperties* opts)=0;
};

interface IEsdlDefFile : extends IInterface
{
    virtual IEsdlDefObjectIterator *getChildren()=0;
};

interface IEsdlDefFileIterator : extends IIteratorOf<IEsdlDefFile>
{
};

interface IEsdlDefinition : extends IInterface
{
    virtual void addDefinitionsFromFile(const char *filename)=0;
    virtual void addDefinitionFromXML(const StringBuffer & xmlDef, const char * esdlDefName, int ver)=0;
    virtual void addDefinitionFromXML(const StringBuffer & xmlDef, const char * esdlDefId)=0;

    virtual IEsdlDefStruct *queryStruct(const char *name)=0;
    virtual IEsdlDefObject *queryObj(const char *name)=0;
    virtual IEsdlDefService *queryService(const char *name)=0;

    virtual IEsdlDefFileIterator *getFiles()=0;

    // A service name is required, the method name(s) provided must be part of the service
    //
    // To support the calculation and return of just the dependent elements for the given service and/or
    // method(s). If provided with requestedVersion and requestedOptional parameters it will only return
    // structure defs for those structures satisfying the request. Note that elements within structures
    // will are not filtered by optional & version, as we can't modify the EsdlDefObjects in place.
    //
    virtual IEsdlDefObjectIterator *getDependencies( const char* service, const char* method, double requestedVer=0, IProperties *opts=NULL, unsigned flags=0  )=0;
    virtual IEsdlDefObjectIterator *getDependencies( const char* service, StringArray &methods, double requestedVer=0, IProperties *opts=NULL, unsigned flags=0 )=0;
    virtual IEsdlDefObjectIterator *getDependencies( const char* service, const char* delimethodlist, const char* delim, double requestedVer, IProperties *opts, unsigned flags )=0;

    virtual IProperties *queryOptionals()=0;
    virtual void setFlConfig(IPropertyTree* p)=0;
    virtual IPropertyTree* getFlConfig()=0;
    virtual bool hasFileLoaded(const char *filename)=0;
    virtual bool hasXMLDefintionLoaded(const char *esdlDefName, int ver)=0;
    virtual bool hasXMLDefintionLoaded(const char *esdlDefId)=0;
    virtual EsdlBasicElementType translateSimpleType(const char *type)=0;
};

esdl_decl IEsdlDefinition *createNewEsdlDefinition(const char *esdl_ns=NULL);
esdl_decl IEsdlDefinition *createEsdlDefinition(const char *esdl_ns=NULL);
esdl_decl IEsdlDefinition *queryEsdlDefinition(const char *esdl_ns=NULL);
esdl_decl void releaseEsdlDefinition(const char *esdl_ns=NULL);

esdl_decl void initEsdlTypeList();
esdl_decl EsdlBasicElementType esdlSimpleType(const char *type);

#endif //ESDLDEF_HPP
