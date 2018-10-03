/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#include "SchemaParser.hpp"
#include "SchemaTypeIntegerLimits.hpp"
#include "SchemaTypeStringLimits.hpp"

namespace pt = boost::property_tree;

class XSDSchemaParser : public SchemaParser
{
    public:

        XSDSchemaParser(std::shared_ptr<SchemaItem> &pConfig) :
            SchemaParser(pConfig) { }
        virtual ~XSDSchemaParser() { };


    protected:

        XSDSchemaParser() { };
        virtual bool doParse(const std::string &configPath, const std::string &masterConfigFile,  const std::map<std::string, std::string> &cfgParms) override;
        void parseXSD(const pt::ptree &tree);
        void parseXSD(const std::string &fullyQualifiedPath);
        std::string getXSDAttributeValue(const pt::ptree &tree, const std::string &attriName, bool throwIfNotPresent=true, const std::string &defaultVal = (std::string(""))) const;
        void parseAttributeGroup(const pt::ptree &attributeTree);
        std::shared_ptr<SchemaValue> parseAttribute(const pt::ptree &attr);

        void parseSimpleType(const pt::ptree &typeTree);
        void parseComplexType(const pt::ptree &typeTree);
        void parseElement(const pt::ptree &elemTree);
        void parseAnnotation(const pt::ptree &elemTree);
        void parseAppInfo(const pt::ptree &elemTree);
        void processXSDFiles(const std::string &path, const std::string &ignore);
        void processSchemaInsert(const pt::ptree &elemTree);

        std::shared_ptr<SchemaType> getType(const pt::ptree &typeTree, bool nameRequired);
        std::shared_ptr<SchemaValue> getSchemaValue(const pt::ptree &attr);

        void parseIntegerTypeLimits(const pt::ptree &restrictTree, std::shared_ptr<SchemaTypeIntegerLimits> &pIntegerLimits);
        void parseStringTypeLimits(const pt::ptree &restrictTree, std::shared_ptr<SchemaTypeStringLimits> &pStringLimits);
        void parseAllowedValue(const pt::ptree &allowedValueTree, SchemaTypeLimits *pTypeLimits);


    protected:

        std::string m_buildsetFilename;
        std::string m_basePath;
        std::string m_masterXSDFilename;
};


#endif // _CONFIG2_XSDCONFIGPARSER_HPP_
