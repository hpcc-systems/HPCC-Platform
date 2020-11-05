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

#pragma warning(disable: 4786)

#include "xslprocessor.hpp"
#include "esdl_def.hpp"
#include "esdl_def_helper.hpp"

typedef IXslTransform* IXslTransformPtr;
typedef IProperties* IPropertiesPtr;


typedef MapBetween<EsdlXslTypeId, EsdlXslTypeId, IXslTransformPtr, IXslTransformPtr> MapXslIdToTransform;
typedef MapBetween<EsdlXslTypeId, EsdlXslTypeId, IPropertiesPtr, IPropertiesPtr> MapXslIdToProperties;


class EsdlDefinitionHelper : public CInterface, implements IEsdlDefinitionHelper
{

private:
    MapXslIdToTransform transforms;
    MapXslIdToProperties parameters;

    void insertAtRoot(StringBuffer& target, StringBuffer& value, bool insertWithinRootStartTag=false);
    StringBuffer& setEffectiveUnquotedNamespace(StringBuffer& tns, EsdlXslTypeId xslId, const char* ns);

public:
    IMPLEMENT_IINTERFACE;

    EsdlDefinitionHelper() {}

    ~EsdlDefinitionHelper()
    {
        HashIterator tit(transforms);
        for(tit.first(); tit.isValid(); tit.next())
        {
            IMapping& et = tit.query();
            IXslTransform** trans = transforms.mapToValue(&et);
            if(trans && *trans)
                (*trans)->Release();
        }

        HashIterator pit(parameters);
        for(pit.first(); pit.isValid(); pit.next())
        {
            IMapping& et = pit.query();
            IProperties** props = parameters.mapToValue(&et);
            if(props && *props)
                (*props)->Release();
        }
    }

    virtual void loadTransform( StringBuffer &path, IProperties *params, EsdlXslTypeId xslId );
    virtual void setTransformParams( EsdlXslTypeId xslId, IProperties *params );

    virtual void toXML( IEsdlDefObjectIterator &objs, StringBuffer &xml, double version, IProperties *opts, unsigned requestedFlags=0 );

    virtual void toXSD( IEsdlDefObjectIterator &objs, StringBuffer &xsd, EsdlXslTypeId xslId, double version, IProperties *opts, const char *ns=NULL, unsigned flags=0, IProperties* overrideParams=nullptr );
    virtual void toXSD( IEsdlDefObjectIterator &objs, StringBuffer &xsd, StringBuffer &xslt, double version, IProperties *opts, const char *ns=NULL, unsigned flags=0, IProperties* overrideParams=nullptr );
    virtual void toWSDL( IEsdlDefObjectIterator &objs, StringBuffer &xsd, EsdlXslTypeId xslId, double version, IProperties *opts, const char *ns=NULL, unsigned flags=0, IProperties* overrideParams=nullptr );
    virtual void toMicroService( IEsdlDefObjectIterator& objs, StringBuffer &content, EsdlXslTypeId classType, IProperties *opts, unsigned flags, IProperties* overrideParams=nullptr);

    void loadTransformParams( EsdlXslTypeId xslId, IProperties* overrideParams=nullptr );


};

// Simplified from the insertAtRoot function in quickxsltlib misc.cpp
void EsdlDefinitionHelper::insertAtRoot(StringBuffer& target, StringBuffer& value, bool insertWithinRootStartTag/*=false*/)
{
    const char* p = strchr(target.str(), '>');
    if (p)
    {
        int pos = p-target.str();
        if (!insertWithinRootStartTag)
            pos++;
        target.insert(pos,value);
    }
}

void EsdlDefinitionHelper::loadTransform( StringBuffer &path, IProperties *params, EsdlXslTypeId xslId )
{
    StringBuffer xsl;
    xsl.loadFile(path);

    Owned<IXslProcessor> proc  = getXslProcessor();
    IXslTransform* trans = proc->createXslTransform();

    trans->setXslSource(xsl, xsl.length(), "esdldefhelper", ".");

    transforms.setValue( xslId, trans );
    parameters.setValue( xslId, params );

    return;
}

void EsdlDefinitionHelper::setTransformParams( EsdlXslTypeId xslId, IProperties *params )
{
    parameters.setValue( xslId, params );
}

void removeEclHiddenStructs(IPropertyTree &depTree)
{
    Owned<IPropertyTreeIterator> it = depTree.getElements("*[@ecl_hide='1']");
    ForEach(*it)
        depTree.removeTree(&it->query());
}
void removeEclHiddenElements(IPropertyTree &depTree, bool cloneGetDataFrom)
{
    Owned<IPropertyTreeIterator> it = depTree.getElements("*");
    ForEach(*it)
    {
        StringArray namesToBeRemoved;
        IPropertyTree &depChild = it->query();
        Owned<IPropertyTreeIterator> elements = depChild.getElements("*[@get_data_from]");
        ForEach(*elements)
        {
            IPropertyTree &element = elements->query();
            StringAttr name = element.queryProp("@name");
            if (!cloneGetDataFrom)
                namesToBeRemoved.appendUniq(name);
            else
            {
                const char *from = element.queryProp("@get_data_from");
                if (streq(name, from)) //shouldn't happen, but becomes a no-op
                    continue;

                //to simplify, all properties come from the get_data_from element, except the name which matches original
                VStringBuffer fromXpath("*[@name='%s']", from);
                //More than one match should really be an error, but need to maximize compatibility with old ESDL definitions.
                //Future changes will allow us to report duplicates as a warning or error more gracefully
                Owned<IPropertyTreeIterator> fromElements = depChild.getElements(fromXpath);
                if (fromElements->first())
                {
                    //make everything the same as get_data_from except the name
                    //copy to existing item to keep position if possible
                    synchronizePTree(&element, &fromElements->query(), true);
                    element.setProp("@name", name);
                }
             }
        }
        elements.setown(it->query().getElements("*[@ecl_hide='1']"));
        ForEach(*elements)
            namesToBeRemoved.appendUniq(elements->query().queryProp("@name"));

        ForEachItemIn(i, namesToBeRemoved)
        {
            VStringBuffer xpath("*[@name='%s']", namesToBeRemoved.item(i));
            it->query().removeProp(xpath);
        }
    }
}
esdl_decl void removeEclHidden(IPropertyTree *depTree, bool cloneGetDataFrom)
{
    if (!depTree)
        return;
    removeEclHiddenStructs(*depTree);
    removeEclHiddenElements(*depTree, cloneGetDataFrom);
}

esdl_decl void removeEclHidden(StringBuffer &xml, bool cloneGetDataFrom)
{
    Owned<IPropertyTree> depTree = createPTreeFromXMLString(xml);
    removeEclHidden(depTree, cloneGetDataFrom);
    toXML(depTree, xml.clear());
}

void EsdlDefinitionHelper::toXML( IEsdlDefObjectIterator& objs, StringBuffer &xml, double version, IProperties *opts, unsigned requestedFlags )
{
    TimeSection ts("serializing EsdlObjects to XML");

    ForEach( objs )
    {
        IEsdlDefObject &esdlObj = objs.query();
        unsigned objFlags = objs.getFlags();
        StringBuffer curObjXml;

        // Get the current structure serialized as normal to the curObjXml buffer
        esdlObj.toXML( curObjXml, version, opts );

        if( requestedFlags & objFlags & DEPFLAG_COLLAPSE )
        {
            IEsdlDefObjectIterator* baseTypes = objs.queryBaseTypesIterator();

            if( baseTypes == NULL )
                throw MakeStringException(0, "EsdlDefinitionHelper::toXML - Collapsed flag enabled, but unable to query the base types iterator");

            ForEach( *baseTypes )
            {
                StringBuffer baseStr;
                IEsdlDefObject& baseObj = baseTypes->query();

                // Get the collapsed form of the base_type- just it's child elements
                // not including the start tag, attributes or end tag.
                baseObj.toXML( baseStr, version, opts, DEPFLAG_COLLAPSE );

                // Insert the 'collapsed' form of the current base_type object
                // into the current struct's serialized representation.
                this->insertAtRoot(curObjXml, baseStr);
            }
        } //else { DBGLOG("  <%s> Collapse failed flag check requested[0x%x] & object[0x%x] & const[0x%x]",esdlObj.queryName(), requestedFlags, objFlags, DEPFLAG_COLLAPSE); }

        if( requestedFlags & DEPFLAG_ARRAYOF )
        {
            if( objFlags & DEPFLAG_ARRAYOF )
            {
                // Add arrayOf="1" attribute to all structs with flag set.
                StringBuffer attribute(" arrayOf=\"1\"");
                this->insertAtRoot(curObjXml, attribute, true);
            }

            if( objFlags & DEPFLAG_STRINGARRAY )
            {
                // Add espStringArray="1" attribute to the struct with flag set.
                StringBuffer attribute(" espStringArray=\"1\"");
                this->insertAtRoot(curObjXml, attribute, true);
            }
        }

        // Add the serialization of the current structure to the xml output StringBuffer
        xml.append( curObjXml ).append('\n');
    }

    return;
}

StringBuffer& EsdlDefinitionHelper::setEffectiveUnquotedNamespace(StringBuffer& tns, EsdlXslTypeId xslId, const char* ns)
{
    IProperties* params = *( parameters.getValue(xslId) );
    tns.set((ns) ? ns :params->queryProp("tnsParam"));
    // Since the xsl string params are quoted with ' we must remove
    // it here before including it as the value of an attribute in
    // the esxdl element
    tns.stripChar('\'');
    if( tns.isEmpty() )
        tns.set("urn:unknown");

    return tns;
}


void EsdlDefinitionHelper::toXSD( IEsdlDefObjectIterator &objs, StringBuffer &xsd, EsdlXslTypeId xslId, double version, IProperties *opts, const char *ns, unsigned flags, IProperties* overrideParams)
{
    StringBuffer xml;
    int xmlLen = 0;
    IXslTransform* trans = *( transforms.getValue( xslId ) );

    this->loadTransformParams( xslId, overrideParams );

    if( trans )
    {
        StringBuffer tns;
        setEffectiveUnquotedNamespace(tns, xslId, ns);

        xml.appendf("<esxdl name=\"custom\" EsdlXslTypeId=\"%d\" xmlns:tns=\"%s\" ns_uri=\"%s\">", xslId, tns.str(), tns.str());
        this->toXML( objs, xml, version, opts, flags );
        xml.append("</esxdl>");

        if (flags & DEPFLAG_ECL_ONLY)
        {
            removeEclHidden(xml,false);
        }

        xmlLen = xml.length();
        trans->setXmlSource( xml.str(), xmlLen );
        trans->transform(xsd);

    } else {
        throw (MakeStringException( 0, "Unable to find transform for EsdlXslTypeId=%d", xslId ));
    }

    return;
}

void EsdlDefinitionHelper::toWSDL( IEsdlDefObjectIterator &objs, StringBuffer &xsd, EsdlXslTypeId xslId, double version, IProperties *opts, const char *ns, unsigned flags, IProperties* overrideParams )
{
    StringBuffer xml;
    int xmlLen = 0;
    IXslTransform* trans = *( transforms.getValue( xslId ) );

    this->loadTransformParams( xslId, overrideParams );

    if( trans )
    {
        StringBuffer tns;
        setEffectiveUnquotedNamespace(tns, xslId, ns);

        xml.appendf("<esxdl name=\"custom\" EsdlXslTypeId=\"%d\" xmlns:tns=\"%s\" ns_uri=\"%s\" version=\"%f\">", xslId, tns.str(), tns.str(), version);
        this->toXML( objs, xml, version, opts, flags );
        xml.append("</esxdl>");

        xmlLen = xml.length();
        trans->setXmlSource( xml.str(), xmlLen );
        trans->transform(xsd);

    } else {
        throw (MakeStringException( 0, "Unable to find transform for EsdlXslTypeId=%d", xslId ));
    }

    return;
}


void EsdlDefinitionHelper::toXSD( IEsdlDefObjectIterator& objs, StringBuffer &xml, StringBuffer &xslt, double version, IProperties *opts, const char *ns, unsigned flags, IProperties* overrideParams )
{
    return;
}

void EsdlDefinitionHelper::loadTransformParams( EsdlXslTypeId xslId, IProperties* overrideParams )
{
    IXslTransform* trans = *( transforms.getValue(xslId) );
    IProperties* params = *( parameters.getValue(xslId) );

    if( !trans )
    {
        throw (MakeStringException( 0, "Unable to find transform for EsdlXslTypeId=%d", xslId ));
    }

    // All setParameter() calls are bare- no implicit quoting-
    // allowing us to set values as expressions. Previously
    // they were quoted which lead to some subtle bugs in the
    // transforms assuming that string values were bool, when
    // it wasn't in fact even possible to set a bool value.

    // Set (default) params associated with the transform in the helper
    if( params )
    {
        Owned<IPropertyIterator> it = params->getIterator();
        for (it->first(); it->isValid(); it->next())
        {
            const char *key = it->getPropKey();
            //set parameter in the XSL transform skipping over the @ prefix, if any
            const char* paramName = *key == '@' ? key+1 : key;
            trans->setParameter(paramName, params->queryProp(key));
        }
    }

    // Set overrides if any. Could be new params or just new values for
    // params set above.
    if( overrideParams )
    {
        Owned<IPropertyIterator> it = overrideParams->getIterator();
        for (it->first(); it->isValid(); it->next())
        {
            const char *key = it->getPropKey();
            //set parameter in the XSL transform skipping over the @ prefix, if any
            const char* paramName = *key == '@' ? key+1 : key;
            trans->setParameter(paramName, overrideParams->queryProp(key));
        }
    }
}

void EsdlDefinitionHelper::toMicroService( IEsdlDefObjectIterator& objs, StringBuffer &content, EsdlXslTypeId implType, IProperties *opts, unsigned flags, IProperties* overrideParams )
{
    StringBuffer xml;
    int xmlLen = 0;
    IXslTransform* trans = *( transforms.getValue( implType ) );

    this->loadTransformParams( implType, overrideParams );

    if( trans )
    {
        IProperties* params = *( parameters.getValue(implType) );

        xml.appendf("<esxdl name='custom' EsdlXslTypeId='%d' xmlns:tns='%s' ns_uri='%s' version='0'>", implType, "urn:unknown", "urn:unknown");
        this->toXML( objs, xml, 0, opts, flags );
        xml.append("</esxdl>");

        trans->setXmlSource( xml.str(), xml.length() );
        trans->transform(content);
        //content.append(xml);
    }
    else
    {
        throw (MakeStringExceptionDirect( 0, "Unable to find transform for creating java service plugin"));
    }

    return;
}

esdl_decl IEsdlDefinitionHelper* createEsdlDefinitionHelper( )
{
    return new EsdlDefinitionHelper( );
}
