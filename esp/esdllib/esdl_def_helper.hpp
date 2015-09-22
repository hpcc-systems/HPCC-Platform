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

// esdl_def_helper.hpp : interface for the IEsdlDefinitionHelper class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(ESDL_DEF_HELPER_HPP)
#define ESDL_DEF_HELPER_HPP

#include "esp.hpp"
#include "esdl_def.hpp"

#pragma warning(disable: 4786)

typedef enum EsdlXslTypeId_
{
    EsdlXslToXsd,
    EsdlXslToWsdl,
    EsdlXslToJavaServiceBase,
    EsdlXslToJavaServiceDummy
} EsdlXslTypeId;

interface IEsdlDefinitionHelper : extends IInterface
{
    virtual void loadTransform( StringBuffer &path, IProperties *params, EsdlXslTypeId xslId )=0;
    virtual void setTransformParams( EsdlXslTypeId xslId, IProperties *params )=0;

    virtual void toXML( IEsdlDefObjectIterator& objs, StringBuffer &xml, double version=0, IProperties *opts=NULL, unsigned flags=0 )=0;
    virtual void toXSD( IEsdlDefObjectIterator& objs, StringBuffer &xsd, EsdlXslTypeId xslId, double version=0, IProperties *opts=NULL, const char *ns=NULL, unsigned flags=0 )=0;
    virtual void toXSD( IEsdlDefObjectIterator& objs, StringBuffer &xsd, StringBuffer& xslt, double version=0, IProperties *opts=NULL, const char *ns=NULL, unsigned flags=0 )=0;
    virtual void toWSDL( IEsdlDefObjectIterator& objs, StringBuffer &wsdl, EsdlXslTypeId xslId, double version=0, IProperties *opts=NULL, const char *ns=NULL, unsigned flags=0 )=0;
    virtual void toJavaService( IEsdlDefObjectIterator& objs, StringBuffer &content, EsdlXslTypeId implType, IProperties *opts=NULL, unsigned flags=0 )=0;
};


esdl_decl IEsdlDefinitionHelper* createEsdlDefinitionHelper( );

#endif // !defined(ESDL_DEF_HELPER_HPP)
