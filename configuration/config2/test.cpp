
#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <string>
#include <exception>
#include <iostream>

#include "Exceptions.hpp"

//#include "libxml/parser.h"
//#include "libxml/tree.h"


// namespace pt = boost::property_tree;

const std::string c_path = "configfiles/"; ///opt/HPCCSystems/componentfiles/config2xml/";

// void parseComponentXSD(pt::ptree &xsdTree);
// void parseIncludes(pt::ptree &xsdTree);

#include "SchemaItem.hpp"
#include "XSDSchemaParser.hpp"
#include "XMLEnvironmentMgr.hpp"


int main()
{

    try
    {
        //std::shared_ptr<ConfigItem> pConfig = std::make_shared<ConfigItem>("root");

        //ConfigParser *pCfgParser = new XSDConfigParser("", pConfig);
        //std::string fpath = c_path + "types.xsd";  //dafilesrv.xsd";
        // std::shared_ptr<ConfigParser> pCfgParser = std::make_shared<XSDConfigParser>("");
        // std::shared_ptr<ConfigItem> pNull;
        // std::shared_ptr<ConfigItem> pConfig = std::make_shared<ConfigItem>("myconfig", pNull);

        //pCfgParser->parseEnvironmentConfig("newenv.xsd", "");

        EnvironmentMgr *pEnvMgr = getEnvironmentMgrInstance("XML");
        std::vector<std::string> cfgParms;
        cfgParms.push_back("buildset.xml");  // not used right now
        pEnvMgr->loadSchema(c_path, "newenv.xsd", cfgParms);
        pEnvMgr->loadEnvironment(c_path + "/environment.xml");

        // 158
        //auto pNode = envMgr.getNodeFromPath("158");
        // auto pNode = pEnvMgr->getEnvironmentNode("74");     // 29 is Hardware/Computer
        auto pNode = pEnvMgr->getEnvironmentNode("35");

        //auto x = pNode->getAllFieldValues("name");

        auto list = pNode->getInsertableItems();

        Status status;
        auto pNewNode = pEnvMgr->addNewEnvironmentNode("35", "ws_ecl", status);
        auto newList = pNewNode->getInsertableItems();
        pEnvMgr->addNewEnvironmentNode("35", "ws_ecl", status);
        pEnvMgr->addNewEnvironmentNode("35", "ws_ecl", status);


        /*auto attributes = pNode->getAttributes();
        for (auto it = attributes.begin(); it != attributes.end(); ++it)
        {
            if ((*it)->getName() == "service")
            {
                std::shared_ptr<EnvironmentValue> pEnvValue = *it;
                const std::shared_ptr<ConfigValue> &pCfgValue = pEnvValue->getCfgValue();
                auto values = pCfgValue->getAllowedValues(pEnvValue.get());
                int i = 3;
            }
        }*/


        // keyref needs to look local first, then search the config tree, but field is ALWAYS local
        // Used during validation.

        //
        // Validate the environment




        //
        // Value set test
        //std::vector<EnvironmentMgr::NameValue> newValues;
        //newValues.push_back({ "name", "namehasbeenchanged" });
        //pEnvMgr->setAttributeValues("158", newValues, "", false);

        //pEnvMgr->saveEnvironment("testout.xml", status);

        auto results = pEnvMgr->getEnvironmentNode(".");

    }
    catch (ParseException &e)
    {
        std::cout << "Error: " << e.what() << "\n";
    }

    //pt::ptree xsdTree;

    // try
    // {
    //     std::string fpath = c_path + "types.xsd";  //dafilesrv.xsd";
    //     pt::read_xml(fpath, xsdTree);

    //     // throw(ParseException("exception throw test"));

    //     parseComponentXSD(xsdTree);


        //
        // Let's try iterating
        // for (pt::ptree::const_iterator it=xsdTree.begin(); it!=xsdTree.end(); ++it)
        // {
        //     std::cout << "here: " << it->first << std::endl;
        // }

        // auto it = xsdTree.find("xs:schema");
        // bool isEnd = (it == xsdTree.not_found());
        // std::cout << "here " << it->first << "  " << it->second.get_value<std::string>() << std::endl;

        // parseIncludes(it->second);

        // const pt::ptree &attributes = it->second.get_child("<xmlattr>", pt::ptree());
        // for (auto attrIt=attributes.begin(); attrIt!=attributes.end(); ++attrIt)
        // {
        //     std::cout << "attr = " << attrIt->first.data() << " second = " << attrIt->second.get_value<std::string>() << std::endl;
        // }


        // std::cout << "First level keys: " << std::endl;
        // const pt::ptree &keys = it->second.get_child("", pt::ptree());

        // std::string val = it->second.get_value<std::string>("xs:include.<xmlattr>.schemaLocation");
        // std::cout << "value: " << val << std::endl;

        // for (auto keyIt=keys.begin(); keyIt!=keys.end(); ++keyIt)
        // {
        //     std::cout << "key = >>" << keyIt->first << "<< " << std::endl;
        //     if (keyIt->first == "xs:include")
        //     {
        //         int i = 5;
        //         std::string val = keyIt->second.get("<xmlattr>.schemaLocation", "not found");
        //         std::cout << "   schema Location = " << val << std::endl;
        //     }
        //     //if (keyIt->first.get_value())
        // }



        std::cout << "Success\n";

        // std::cout << std::endl << "Using Boost "
        //   << BOOST_VERSION / 100000     << "."  // major version
        //   << BOOST_VERSION / 100 % 1000 << "."  // minor version
        //   << BOOST_VERSION % 100                // patch level
        //   << std::endl;
    // }
    // catch (const ParseException &e)
    // {
    //     std::cout << "Error: " << e.what() << "\n";
    // }
    // catch (std::exception &e)
    // {
    //     std::cout << "Error: " << e.what() << "\n";
    // }
    // return 0;
}



// void parseComponentXSD(pt::ptree &xsdTree)
// {
//     //
//     // Get to the schema
//     auto schemaIt = xsdTree.find("xs:schema");

//     //
//     // Since we only support includes for a component schema at the top level, look for includes first and process them.
//     // Note that these includes may NOT define elements or anything like that. Only types, attribute groups, keyrefs
//     const pt::ptree &keys = schemaIt->second.get_child("", pt::ptree());
//     for (auto it=keys.begin(); it!=keys.end(); ++it)
//     {
//         if (it->first == "xs:include")
//         {
//             std::string schema = it->second.get("<xmlattr>.schemaLocation", "not found");
//             std::cout << "Parsing found include: " << schema << std::endl;
//         }
//     }

//     std::vector<std::string> vec;
//     vec.push_back("Hello");
//     vec.push_back("There");


    //
    // Now look for any special sections we support
    // for (auto it=keys.begin(); it!=keys.end(); ++it)
    // {
        // if (it->first == "xs:attributeGroup")
        // {
        //     std::string groupName = it->second.get("<xmlattr>.name", "not found");
        //     std::cout << "Attribute group name: " << groupName << std::endl;

        //     //
        //     // Lets parse the attributes now
        //     const pt::ptree &attrs = it->second.get_child("", pt::ptree());
        //     for (auto attributeIt = attrs.begin(); attributeIt != attrs.end(); ++attributeIt)
        //     {
        //         auto ss = attributeIt->first;
        //         if (attributeIt->first == "xs:attribute")
        //         {
        //             std::cout << "found attribute" << groupName << std::endl;
        //             XSDAttribute attr;
        //             attr.parse(attributeIt->second);
        //         }
        //     }
//         // }
//         if (it->first == "xs:simpleType")
//         {
//             //std::string typeName = it->second.get("<xmlattr>.name", "not found");
//             //int i = 4;
//             std::shared_ptr<CfgType> pType = XSDTypeParser::parseSimpleType(it->second);
//         }
//     }


// }



// void parseIncludes(pt::ptree &xsdTree)
// {
//     const pt::ptree &keys = xsdTree.get_child("", pt::ptree());
//     for (auto it=keys.begin(); it!=keys.end(); ++it)
//     {
//         if (it->first == "xs:include")
//         {
//             std::string schema = it->second.get("<xmlattr>.schemaLocation", "not found");
//             std::cout << "Parsing found include: " << schema << std::endl;
//         }
//     }
// }