/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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


#ifndef _CONFIG2_XSDCONFIGPARSER_HPP_
#define _CONFIG2_XSDCONFIGPARSER_HPP_

#include <string>
#include <memory>
#include <map>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include "ConfigParser.hpp"

namespace pt = boost::property_tree;

class XSDConfigParser : public ConfigParser
{
    public:

        XSDConfigParser(const std::string &basePath, std::shared_ptr<ConfigItem> &pConfig) :
            ConfigParser(basePath, pConfig) { }
        virtual ~XSDConfigParser() { };
    

    protected:

        XSDConfigParser() { };
        virtual bool doParse(const std::vector<std::string> &cfgParms) override;
        virtual void parseXSD(const pt::ptree &tree);
        virtual void parseXSD(const std::string &filename);
        virtual std::string getXSDAttributeValue(const pt::ptree &tree, const std::string &attriName, bool throwIfNotPresent=true, const std::string &defaultVal = "") const;
        virtual void parseAttributeGroup(const pt::ptree &attributeTree);
        virtual void parseAttribute(const pt::ptree &attr);

        virtual void parseSimpleType(const pt::ptree &typeTree);
        virtual void parseComplexType(const pt::ptree &typeTree);
        virtual void parseElement(const pt::ptree &elemTree);

        virtual std::shared_ptr<CfgType> getCfgType(const pt::ptree &typeTree, bool nameRequired=true);
        virtual std::shared_ptr<CfgValue> getCfgValue(const pt::ptree &attr);
        

    protected:
    
        std::string m_buildsetFilename;   

};


#endif // _CONFIG2_XSDCONFIGPARSER_HPP_
