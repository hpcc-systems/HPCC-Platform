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
#include "seclib.hpp"

#ifdef ESDLLIB_EXPORTS
 #define esdl_decl DECL_EXPORT
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
#define DEPFLAG_STRINGARRAY (0x04)  //Set dynamically by gatherDependencies to indicate stylesheet should output an EspStringArray structure definition.
#define DEPFLAG_ECL_ONLY    (0x08)  //ignore anything tagged with ecl_hide

#define DEPFLAG_INCLUDE_REQUEST     (0x10)
#define DEPFLAG_INCLUDE_RESPONSE    (0x20)
#define DEPFLAG_INCLUDE_METHOD      (0x40)
#define DEPFLAG_INCLUDE_SERVICE     (0x80)
#define DEPFLAG_INCLUDE_TYPES       (DEPFLAG_INCLUDE_REQUEST | DEPFLAG_INCLUDE_RESPONSE | DEPFLAG_INCLUDE_METHOD | DEPFLAG_INCLUDE_SERVICE)

interface IEsdlDefObject : extends IInterface
{
    virtual const char *queryName()=0;
    virtual const char *queryProp(const char *name)=0;
    virtual bool hasProp(const char *name)=0;
    virtual int getPropInt(const char *pname, int def = 0)=0;
    virtual bool getPropBool(const char *pname, bool def = false)=0;
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
    virtual const bool checkIsEsdlList()=0;
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
    virtual const char *queryMetaData(const char* tag)=0;
    virtual const char *queryProductAssociation()=0;
    virtual bool isProductDefault()=0;
    virtual const char *queryLogMethodName()=0;
    virtual const char *queryMethodFLReqFormat()=0;
    virtual const char *queryMethodFLRespFormat()=0;
    virtual const char *queryMethodName()=0;
    virtual const char *queryMbsiProductAssociations()=0;
    virtual int queryVariableLengthRecordProcessing()=0;
    virtual int queryAllowMultipleEntryPerUnitNumber()=0;
    virtual const MapStringTo<SecAccessFlags> & queryAccessMap()=0;
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
    virtual const char * queryStaticNamespace()=0;
};

interface IEsdlDefFile : extends IInterface
{
    virtual IEsdlDefObjectIterator *getChildren()=0;
};

interface IEsdlDefFileIterator : extends IIteratorOf<IEsdlDefFile>
{
};

interface IEsdlDefReporter;
interface IEsdlDefinition : extends IInterface
{
    virtual void setReporter(IEsdlDefReporter* reporter)=0;
    virtual IEsdlDefReporter* queryReporter() const=0;
    virtual void addDefinitionsFromFile(const char *filename)=0;
    virtual void addDefinitionFromXML(const StringBuffer & xmlDef, const char * esdlDefName, double ver)=0;
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

    virtual bool isShared() = 0;
};

typedef IEsdlDefReporter* (*EsdlDefReporterFactory)();
esdl_decl IEsdlDefinition *createNewEsdlDefinition(const char *esdl_ns=NULL, EsdlDefReporterFactory factory = nullptr);
esdl_decl IEsdlDefinition *createEsdlDefinition(const char *esdl_ns=nullptr, EsdlDefReporterFactory factory = nullptr);
esdl_decl IEsdlDefinition *queryEsdlDefinition(const char *esdl_ns=NULL);
esdl_decl void releaseEsdlDefinition(const char *esdl_ns=NULL);

esdl_decl void initEsdlTypeList();
esdl_decl EsdlBasicElementType esdlSimpleType(const char *type);

interface IEsdlDefReporter : extends IInterface
{
    using Flags = uint64_t;
    enum : Flags {
        ReportDisaster  = 1 << 0,
        ReportAError    = 1 << 1,
        ReportIError    = 1 << 2,
        ReportOError    = 1 << 3,
        ReportUError    = 1 << 4,
        ReportIWarning  = 1 << 5,
        ReportOWarning  = 1 << 6,
        ReportUWarning  = 1 << 7,
        ReportDProgress = 1 << 8,
        ReportOProgress = 1 << 9,
        ReportUProgress = 1 << 10,
        ReportDInfo     = 1 << 11,
        ReportOInfo     = 1 << 12,
        ReportUInfo     = 1 << 13,
        ReportStats     = 1 << 14,
        ReportCategoryMask =
                ReportDisaster |
                ReportAError | ReportIError | ReportOError | ReportUError |
                ReportIWarning | ReportOWarning | ReportUWarning |
                ReportDProgress | ReportOProgress | ReportUProgress |
                ReportDInfo | ReportOInfo | ReportUInfo |
                ReportStats,
        ReportErrorClass = ReportIError | ReportOError | ReportUError,
        ReportWarningClass = ReportIWarning | ReportOWarning | ReportUWarning,
        ReportProgressClass = ReportDProgress | ReportOProgress | ReportUProgress,
        ReportInfoClass = ReportDInfo | ReportOInfo | ReportUInfo,
        ReportDeveloperAudience = ReportIError | ReportIWarning | ReportDProgress | ReportDInfo,
        ReportOperatorAudience = ReportOError | ReportOWarning | ReportOProgress | ReportOInfo,
        ReportUserAudience = ReportUError | ReportUWarning | ReportUProgress | ReportUInfo,

        ReportDefinition = Flags(1) << 63,
        ReportService    = Flags(1) << 62,
        ReportMethod     = Flags(1) << 61,
        ReportComponentMask = Flags(UINT64_MAX) & ~ReportCategoryMask,
    };

    virtual Flags queryFlags() const = 0;
    virtual Flags getFlags(Flags mask = Flags(-1)) const = 0;
    virtual bool testFlags(Flags flags) const = 0;
    virtual void setFlags(Flags flags, bool state) = 0;
    virtual void report(Flags flags, const char* fmt, ...) const = 0;
    virtual void report(Flags flags, const char* fmt, va_list& args) const = 0;
    virtual void report(Flags flags, const StringBuffer& msg) const = 0;
};

class EsdlDefReporter : public IEsdlDefReporter, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Flags queryFlags() const override { return m_flags; }
    Flags getFlags(Flags mask) const override { return m_flags & mask; }
    bool  testFlags(Flags flags) const override { return (m_flags & flags) == flags; }

    void  setFlags(Flags flags, bool state) override
    {
        if (state)
            m_flags = m_flags | flags;
        else
            m_flags = m_flags & ~flags;
    }

    void report(Flags flags, const char* fmt, ...) const  __attribute__((format(printf,3,4)))
    {
        if (testFlags(flags))
        {
            va_list args;
            va_start(args, fmt);
            reportSelf(flags, fmt, args);
            va_end(args);
        }
    }

    void report(Flags flags, const char* fmt, va_list& args) const __attribute__((format(printf,3,0)))
    {
        if (testFlags(flags))
            reportSelf(flags, fmt, args);
    }

    void report(Flags flags, const StringBuffer& msg) const
    {
        if (testFlags(flags))
            reportSelf(flags, msg);
    }

protected:
    void reportSelf(Flags flags, const char* fmt, va_list& args) const  __attribute__((format(printf,3,0)))
    {
        StringBuffer msg;
        msg.valist_appendf(fmt, args);
        reportSelf(flags, msg);
    }

    virtual void reportSelf(Flags flags, const StringBuffer& msg) const
    {
        Flags masked = getFlags(flags);
        const char* componentLabel = getComponentLabel(flags);
        const char* levelLabel = getLevelLabel(flags);

        if (componentLabel != nullptr && levelLabel != nullptr)
            reportSelf(flags, componentLabel, levelLabel, msg);
    }

    const char* getComponentLabel(Flags flags) const
    {
        switch (getFlags(ReportComponentMask))
        {
        case ReportDefinition: return "EsdlDefinition";
        case ReportService: return "EsdlDefService";
        case ReportMethod: return "EsdlDefMethod";
        default: return nullptr;
        }
    }

    const char* getLevelLabel(Flags flags) const
    {
        switch (flags & getFlags(ReportCategoryMask))
        {
        case ReportDisaster : return "Disaster";
        case ReportAError : return "Audit Error";
        case ReportIError: return "Internal Error";
        case ReportOError: return "Operator Error";
        case ReportUError: return "User Error";
        case ReportIWarning: return "Internal Warning";
        case ReportOWarning: return "Operator Warning";
        case ReportUWarning: return "User Warning";
        case ReportDProgress: return "Debug Progress";
        case ReportOProgress: return "Operator Progress";
        case ReportUProgress: return "User Progress";
        case ReportDInfo: return "Debug Info";
        case ReportOInfo: return "Operator Info";
        case ReportUInfo: return "User Info";
        case ReportStats: return "Stats";
        default: return nullptr;
        }
    }

    virtual void reportSelf(Flags flags, const char* component, const char* level, const char* msg) const = 0;

private:
    Flags m_flags = 0;
};

#endif //ESDLDEF_HPP
