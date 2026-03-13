/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#include "xml2ecl.hpp"
#include "jargv.hpp"
#include "jfile.hpp"
#include "jexcept.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <tuple>

namespace {

//==============================================================================
// Constants
//==============================================================================

const char* DEFAULT_STRING_TYPE = "UTF8";
const char* VERSION = "1.0.0";
const char* INNER_TEXT_TAG = "_inner_value";
const char* WRAPPER_XML_TAG = "wrapper";
const size_t MAX_ARRAY_SAMPLES = 500;  // Limit array element sampling to avoid scanning huge files

//==============================================================================
// XML Parsing Implementation
//==============================================================================

// Strip namespace prefix from XML element or attribute names
// e.g., "ns:element" -> "element", "xmlns:soapenv" -> "soapenv"
// This matches the Lisp behavior which uses local-name and ignores namespace prefix
std::string stripNamespacePrefix(const char* name)
{
    if (!name || !*name)
        return "";
    
    std::string result = name;
    
    // Find the colon separator
    size_t colonPos = result.find(':');
    if (colonPos != std::string::npos && colonPos > 0)
    {
        // Return everything after the colon (the local name)
        return result.substr(colonPos + 1);
    }
    
    // No namespace prefix found
    return result;
}

// Forward declarations for internal functions
std::string stripNamespacePrefix(const char* name);
BaseType xmlValueToBaseType(const char* value);
std::unique_ptr<SchemaItem> buildSchemaFromTree(const IPropertyTree* tree, bool fullScan);
std::tuple<std::unique_ptr<SchemaItem>, std::string, bool> unwrapParsedObject(std::unique_ptr<SchemaItem> obj);
std::string generateDatasetComment(const std::string& rootName, const std::string& layoutName, const std::string& xpath, bool needsNoroot);
std::tuple<std::unique_ptr<SchemaItem>, std::string, bool> parseXmlFileImpl(const char* filename, bool fullScan);

// Convert XML value string to our BaseType
BaseType xmlValueToBaseType(const char* value)
{
    return stringValueToBaseType(value);
}

// Build schema structure from IPropertyTree (XML)
std::unique_ptr<SchemaItem> buildSchemaFromTree(const IPropertyTree* tree, bool fullScan)
{
    if (!tree)
        return std::make_unique<ValueItem>();
    
    auto objectItem = std::make_unique<ObjectItem>();
    bool hasChildren = false;
    bool hasAttributes = false;
    bool hasTextContent = false;
    std::string textValue;
    
    // First, process attributes (they appear as @name in ptree)
    Owned<IAttributeIterator> attrIter = tree->getAttributes();
    if (attrIter && attrIter->first())
    {
        do {
            const char* attrName = attrIter->queryName();
            const char* attrValue = attrIter->queryValue();
            
            if (attrName && attrName[0] == '@')
            {
                hasAttributes = true;
                
                // Strip namespace prefix and keep @ prefix for XPATH generation
                std::string localName = stripNamespacePrefix(attrName + 1);  // Skip '@'
                std::string fieldName = "@" + localName;
                
                auto valueItem = std::make_unique<ValueItem>();
                valueItem->addValueType(xmlValueToBaseType(attrValue));
                objectItem->addOrMergeField(fieldName, std::move(valueItem));
            }
        } while (attrIter->next());
    }
    
    // Check for child elements
    Owned<IPropertyTreeIterator> iter = tree->getElements("*");
    if (iter && iter->first())
    {
        // Has child elements - group by name to detect arrays
        std::vector<std::pair<std::string, std::vector<const IPropertyTree*>>> fieldGroups;
        
        do {
            const char* elemName = iter->query().queryName();
            if (!elemName)
                continue;
            
            hasChildren = true;
            
            // Strip namespace prefix from element name
            std::string localName = stripNamespacePrefix(elemName);
            
            // Find existing group or create new one
            auto it = std::find_if(fieldGroups.begin(), fieldGroups.end(),
                                   [&localName](const auto& pair) { return pair.first == localName; });
            
            if (it != fieldGroups.end())
                it->second.push_back(&iter->query());
            else
                fieldGroups.push_back(std::make_pair(localName, 
                                                      std::vector<const IPropertyTree*>{&iter->query()}));
            
        } while (iter->next());
        
        // Process each field group
        for (const auto& fieldGroup : fieldGroups)
        {
            const std::string& fieldName = fieldGroup.first;
            const auto& elements = fieldGroup.second;
            
            if (elements.size() == 1)
            {
                // Single element - not an array
                objectItem->addOrMergeField(fieldName, buildSchemaFromTree(elements[0], fullScan));
            }
            else
            {
                // Multiple elements with same name - this is an array
                // Stop after 500 consecutive unchanged merges (unless fullScan is enabled)
                auto arrayItem = std::make_unique<ArrayItem>();
                size_t unchangedCount = 0;
                size_t limit = fullScan ? elements.size() : MAX_ARRAY_SAMPLES;
                
                for (size_t i = 0; i < elements.size() && unchangedCount < limit; i++)
                {
                    auto elemSchema = buildSchemaFromTree(elements[i], fullScan);
                    bool changed = arrayItem->mergeElementType(std::move(elemSchema));
                    
                    if (changed)
                        unchangedCount = 0;  // Reset counter on change
                    else
                        unchangedCount++;    // Increment on no change
                }
                
                objectItem->addOrMergeField(fieldName, std::move(arrayItem));
            }
        }
    }
    
    // Check for text content (ignore whitespace-only text from pretty-printed XML)
    const char* textProp = tree->queryProp(nullptr);
    if (textProp && *textProp)
    {
        // Check if there's any non-whitespace content
        bool hasNonWhitespace = false;
        for (const char* p = textProp; *p; ++p)
        {
            if (*p != ' ' && *p != '\t' && *p != '\n' && *p != '\r')
            {
                hasNonWhitespace = true;
                break;
            }
        }
        
        if (hasNonWhitespace)
        {
            hasTextContent = true;
            textValue = textProp;
        }
    }
    
    // Handle the case where we have text content
    if (hasTextContent)
    {
        auto valueItem = std::make_unique<ValueItem>();
        valueItem->addValueType(xmlValueToBaseType(textValue.c_str()));
        
        if (hasChildren || hasAttributes)
        {
            // Element has both text and children/attributes
            // Store text as special inner value field
            objectItem->addOrMergeField(INNER_TEXT_TAG, std::move(valueItem));
        }
        else
        {
            // Just text content, no children or attributes - return as simple value
            return valueItem;
        }
    }
    
    // If we have no fields at all, return a simple null value
    if (!hasChildren && !hasTextContent && !hasAttributes)
    {
        auto valueItem = std::make_unique<ValueItem>();
        valueItem->addValueType(BaseType::NullValue);
        return valueItem;
    }
    
    return objectItem;
}

// Unwrap the parsed object to extract the actual root from the wrapper
// Returns tuple of (unwrapped object, xpath string for dataset comment, needs NOROOT flag)
std::tuple<std::unique_ptr<SchemaItem>, std::string, bool> unwrapParsedObject(std::unique_ptr<SchemaItem> obj)
{
    std::string xpath;
    bool needsNoroot = false;
    
    // If not an ObjectItem, return as-is
    ObjectItem* objItem = dynamic_cast<ObjectItem*>(obj.get());
    if (!objItem)
        return std::make_tuple(std::move(obj), xpath, needsNoroot);
    
    //  After parsing wrapped XML, the ObjectItem represents the wrapper element's content
    //  The actual root element(s) appear as fields: e.g., {"node": ObjectItem} or {"test": ArrayItem}
    //  We need to extract these actual root element(s)
    
    const auto& fields = objItem->getFields();
    if (fields.empty())
        return std::make_tuple(std::move(obj), xpath, needsNoroot);
    
    // If there are multiple different top-level elements (e.g., <a/> <b/>), 
    // we cannot unwrap without losing schema information. Keep wrapper intact.
    if (fields.size() != 1)
        return std::make_tuple(std::move(obj), xpath, needsNoroot);
    
    // Get the single field - this is the actual root element
    const std::string& rootElementName = fields[0].first;
    SchemaItem* rootElement = fields[0].second.get();
    
    const ObjectItem* topObj = nullptr;
    
    // Check if it's an ArrayItem (multiple root elements with same name)
    if (ArrayItem* arrayItem = dynamic_cast<ArrayItem*>(rootElement))
    {
        // For arrays, we want to use the array's element type
        if (arrayItem->getElementType())
        {
            topObj = dynamic_cast<const ObjectItem*>(arrayItem->getElementType());
            xpath = rootElementName; // e.g., "node" or "test"
            needsNoroot = true; // Multiple root elements with same name → need NOROOT
        }
    }
    else if (const ObjectItem* rootObj = dynamic_cast<const ObjectItem*>(rootElement))
    {
        // Single root element
        topObj = rootObj;
        xpath = rootElementName; // e.g., "node"
    }
    
    if (!topObj)
        return std::make_tuple(std::move(obj), xpath, needsNoroot);
    
    // Now aggressively unwrap: keep unwrapping while we have exactly one child
    // Continue through ObjectItems AND through ArrayItems (single-child arrays)
    while (topObj)
    {
        const auto& topFields = topObj->getFields();
        
        // Check if we can unwrap further - must have exactly 1 field
        if (topFields.size() != 1)
            break;
        
        const std::string& childName = topFields[0].first;
        const SchemaItem* childItem = topFields[0].second.get();
        
        // Check if the single field is an ArrayItem
        if (const ArrayItem* childArrayItem = dynamic_cast<const ArrayItem*>(childItem))
        {
            // Single child that's an array - unwrap through it
            if (childArrayItem->getElementType())
            {
                const ObjectItem* arrayElement = dynamic_cast<const ObjectItem*>(childArrayItem->getElementType());
                if (arrayElement)
                {
                    // Build up the xpath
                    if (!xpath.empty())
                        xpath += "/";
                    xpath += childName;
                    
                    needsNoroot = true; // We went through an array
                    topObj = arrayElement;
                    continue;
                }
            }
            // Can't unwrap this array
            break;
        }
        else if (const ObjectItem* childObj = dynamic_cast<const ObjectItem*>(childItem))
        {
            // Single child that's an object - unwrap through it
            // Build up the xpath
            if (!xpath.empty())
                xpath += "/";
            xpath += childName;
            
            // Move down to the child
            topObj = childObj;
        }
        else
        {
            // Single child but it's a ValueItem - can't unwrap
            break;
        }
    }
    
    // Clone and return the unwrapped object
    return std::make_tuple(cloneSchemaItem(topObj), xpath, needsNoroot);
}

// Generate the dataset comment line
std::string generateDatasetComment(const std::string& rootName, const std::string& layoutName, const std::string& xpath, bool needsNoroot)
{
    std::string norootOpt;
    if (needsNoroot)
        norootOpt = ", NOROOT";
    
    std::string comment = "// ds := DATASET('~data::";
    
    // Convert root name to lowercase for the dataset name
    std::string lowercaseName = rootName;
    std::transform(lowercaseName.begin(), lowercaseName.end(), lowercaseName.begin(), 
                   [](unsigned char c) { return std::tolower(c); });
    comment += lowercaseName;
    comment += "', ";
    comment += layoutName;
    comment += ", XML('";
    comment += xpath;
    comment += "'";
    comment += norootOpt;
    comment += "));\n\n";
    
    return comment;
}

// Internal implementation that returns unwrapped schema along with xpath and needsNoroot flag
std::tuple<std::unique_ptr<SchemaItem>, std::string, bool> parseXmlFileImpl(const char* filename, bool fullScan)
{
    // MEMORY LIMITATION:
    // This implementation loads the entire XML file into memory before parsing.
    // This is a fundamental requirement of the ptree XML parser architecture, which builds
    // a complete document tree in memory. True streaming XML parsing is not possible with
    // this parser library. For very large XML files (multi-GB), this may exhaust available
    // memory and cause the process to fail.
    //
    // Unlike json2ecl which can stream NDJSON files, XML documents must be fully loaded
    // because elements can reference each other throughout the document structure.
    
    // Read the file content
    Owned<IFile> file = createIFile(filename);
    if (!file->exists())
        throw MakeStringException(-1, "File not found: %s", filename);
    
    // Read file into string to wrap it
    Owned<IFileIO> fileIO = file->open(IFOread);
    if (!fileIO)
        throw MakeStringException(-1, "Failed to open file: %s", filename);
    
    offset_t fileSize = file->size();
    std::string xmlContent;
    xmlContent.resize((size_t)fileSize);
    
    size_t bytesRead = fileIO->read(0, (size_t)fileSize, &xmlContent[0]);
    xmlContent.resize(bytesRead);
    
    // Wrap the XML content with wrapper tags
    std::string wrappedXml = "<";
    wrappedXml += WRAPPER_XML_TAG;
    wrappedXml += ">";
    wrappedXml += xmlContent;
    wrappedXml += "</";
    wrappedXml += WRAPPER_XML_TAG;
    wrappedXml += ">";
    
    // Parse the wrapped XML using ipt_ordered flag to preserve field order
    Owned<IPropertyTree> tree = createPTreeFromXMLString(wrappedXml.c_str(), ipt_ordered);
    if (!tree)
        throw MakeStringException(-1, "Failed to parse XML file: %s", filename);
    
    // Build schema from tree
    auto schema = buildSchemaFromTree(tree, fullScan);
    
    // Unwrap to get xpath and needsNoroot
    return unwrapParsedObject(std::move(schema));
}
} // anonymous namespace

//==============================================================================
// Public API Implementation
//==============================================================================
std::tuple<std::unique_ptr<SchemaItem>, bool> parseXmlFileWithFlags(const char* filename, bool fullScan)
{
    auto result = parseXmlFileImpl(filename, fullScan);
    // Return (schema, needsNoroot), discarding xpath
    return std::make_tuple(std::move(std::get<0>(result)), std::get<2>(result));
}

std::unique_ptr<SchemaItem> parseXmlFile(const char* filename)
{
    // Read the file content
    Owned<IFile> file = createIFile(filename);
    if (!file->exists())
        throw MakeStringException(-1, "File not found: %s", filename);
    
    // Read file into string to wrap it
    Owned<IFileIO> fileIO = file->open(IFOread);
    if (!fileIO)
        throw MakeStringException(-1, "Failed to open file: %s", filename);
    
    offset_t fileSize = file->size();
    std::string xmlContent;
    xmlContent.resize((size_t)fileSize);
    
    size_t bytesRead = fileIO->read(0, (size_t)fileSize, &xmlContent[0]);
    xmlContent.resize(bytesRead);
    
    // Wrap the XML content with wrapper tags
    std::string wrappedXml = "<";
    wrappedXml += WRAPPER_XML_TAG;
    wrappedXml += ">";
    wrappedXml += xmlContent;
    wrappedXml += "</";
    wrappedXml += WRAPPER_XML_TAG;
    wrappedXml += ">";
    
    // Parse the wrapped XML using ipt_ordered flag to preserve field order
    Owned<IPropertyTree> tree = createPTreeFromXMLString(wrappedXml.c_str(), ipt_ordered);
    if (!tree)
        throw MakeStringException(-1, "Failed to parse XML file: %s", filename);
    
    // Build schema from tree (do NOT unwrap here - that happens in processFiles)
    // Note: fullScan=false by default for this public API
    return buildSchemaFromTree(tree, false);
}

std::unique_ptr<SchemaItem> parseXmlStream(std::istream& stream)
{
    // Read entire stream into string
    std::string xmlContent((std::istreambuf_iterator<char>(stream)),
                           std::istreambuf_iterator<char>());
    
    if (xmlContent.empty())
        throw MakeStringException(-1, "Empty XML stream");
    
    // Wrap the XML content with wrapper tags
    std::string wrappedXml = "<";
    wrappedXml += WRAPPER_XML_TAG;
    wrappedXml += ">";
    wrappedXml += xmlContent;
    wrappedXml += "</";
    wrappedXml += WRAPPER_XML_TAG;
    wrappedXml += ">";
    
    // Parse XML from string using ipt_ordered flag to preserve field order
    Owned<IPropertyTree> tree = createPTreeFromXMLString(wrappedXml.c_str(), ipt_ordered);
    if (!tree)
        throw MakeStringException(-1, "Failed to parse XML from stream");
    
    // Build schema from tree (do NOT unwrap here - that happens in processFiles)
    // Note: fullScan=false by default for this public API
    return buildSchemaFromTree(tree, false);
}

//==============================================================================
// Application Functions (CLI - Internal use only)
//==============================================================================

namespace xml2ecl_cli {

int processFiles(const std::vector<std::string>& files, 
                 const std::string& stringType,
                 bool readFromStdin,
                 bool fullScan)
{
    try
    {
        std::unique_ptr<SchemaItem> schema;
        std::string xpath;
        bool needsNoroot = false;
        
        if (readFromStdin)
        {
            // Read from stdin
            schema = parseXmlStream(std::cin);
        }
        else
        {
            // Parse files
            if (files.empty())
                return 0;
            
            // Parse first file to get schema and NOROOT flag
            auto firstFileResult = parseXmlFileImpl(files[0].c_str(), fullScan);
            schema = std::move(std::get<0>(firstFileResult));
            xpath = std::get<1>(firstFileResult);
            needsNoroot = std::get<2>(firstFileResult);
            
            // Merge additional files and validate NOROOT consistency
            for (size_t i = 1; i < files.size(); ++i)
            {
                auto fileResult = parseXmlFileWithFlags(files[i].c_str(), fullScan);
                bool fileNeedsNoroot = std::get<1>(fileResult);
                
                // Validate that all files have consistent NOROOT requirements
                if (fileNeedsNoroot != needsNoroot)
                {
                    throw MakeStringException(-1, 
                        "Incompatible file structures: %s %s NOROOT but %s %s NOROOT. "
                        "All files must have the same structure.",
                        files[0].c_str(), needsNoroot ? "requires" : "does not require",
                        files[i].c_str(), fileNeedsNoroot ? "requires" : "does not require");
                }
                
                std::unique_ptr<SchemaItem> additionalSchema = std::move(std::get<0>(fileResult));
                mergeSchemaItems(schema.get(), additionalSchema.get());
            }
        }
        
        // Unwrap stdin schema (files already unwrapped above)
        if (readFromStdin)
        {
            auto unwrappedResult = unwrapParsedObject(std::move(schema));
            schema = std::move(std::get<0>(unwrappedResult));
            xpath = std::get<1>(unwrappedResult);
            needsNoroot = std::get<2>(unwrappedResult);
        }
        
        // Determine root layout name
        std::string rootName;
        if (!readFromStdin && files.size() == 1)
        {
            // Use filename (without extension) as root layout name
            std::string filename = files[0];
            size_t lastSlash = filename.find_last_of("/\\");
            if (lastSlash != std::string::npos)
                filename = filename.substr(lastSlash + 1);
            size_t lastDot = filename.find_last_of('.');
            if (lastDot != std::string::npos)
                filename = filename.substr(0, lastDot);
            rootName = filename.empty() ? "TOPLEVEL" : filename;
        }
        else
        {
            // Multiple files or stdin - use TOPLEVEL
            rootName = "TOPLEVEL";
        }
        
        // Generate ECL record definitions
        std::set<std::string> usedLayoutNames;
        std::string eclOutput = generateEclRecordDef(schema.get(), rootName, usedLayoutNames, stringType);
        
        // Compute the layout name using the same usedLayoutNames for consistency
        std::string layoutName = asLayoutName(rootName, usedLayoutNames);
        
        // Generate dataset comment
        std::string datasetComment = generateDatasetComment(rootName, layoutName, xpath, needsNoroot);
        
        // Output to stdout
        std::cout << eclOutput;
        std::cout << datasetComment;
        
        return 0;
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        std::cerr << "Error: " << msg.str() << std::endl;
        e->Release();
        return 1;
    }
    catch (std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}

void displayHelp()
{
    std::cout << "xml2ecl - Generate ECL RECORD definitions from XML data\n\n";
    std::cout << "Usage: xml2ecl [OPTIONS] [FILES...]\n\n";
    std::cout << "Options:\n";
    std::cout << "  -h, --help              Display this help message\n";
    std::cout << "  -s, --string-type TYPE  ECL string type (UTF8, STRING, VARSTRING, UNICODE)\n";
    std::cout << "                          Default: UTF8\n";
    std::cout << "  --full-scan             Scan all array elements (no limit)\n";
    std::cout << "  --version               Display version information\n\n";
    std::cout << "Description:\n";
    std::cout << "  xml2ecl examines XML data and deduces the ECL RECORD definitions\n";
    std::cout << "  necessary to parse it. The resulting ECL definitions are returned\n";
    std::cout << "  via standard out.\n\n";
    std::cout << "  XML data can be supplied as one or more files or via standard input.\n\n";
    std::cout << "  Multiple files are parsed as if they have the same record structure,\n";
    std::cout << "  useful when not all fields are defined in a single file.\n\n";
    std::cout << "  XML attributes are included in the schema with their original names.\n";
    std::cout << "  When an element has both attributes and text content, the text is\n";
    std::cout << "  stored in a field named '_inner_value'.\n\n";
    std::cout << "Performance:\n";
    std::cout << "  For efficiency, when scanning repeated elements, the tool stops\n";
    std::cout << "  processing after 500 consecutive elements that don't modify the schema.\n";
    std::cout << "  This prevents unnecessary processing of large uniform datasets while\n";
    std::cout << "  ensuring schema accuracy. Use --full-scan to scan all elements if needed.\n\n";
    std::cout << "Known Limitations:\n";
    std::cout << "  - Namespace declarations (xmlns:* attributes) are preserved as fields\n";
    std::cout << "  - Mixed content (text and elements interleaved) may not be fully supported\n\n";
    std::cout << "Examples:\n";
    std::cout << "  xml2ecl data.xml\n";
    std::cout << "  xml2ecl -s STRING file1.xml file2.xml\n";
    std::cout << "  cat data.xml | xml2ecl\n";
}

void displayVersion()
{
    std::cout << "xml2ecl version " << VERSION << std::endl;
}

ParsedArgs parseCommandLine(int argc, const char* argv[])
{
    ParsedArgs args;
    args.stringType = DEFAULT_STRING_TYPE;
    args.fullScan = false;
    args.readFromStdin = false;
    
    ArgvIterator iter(argc, argv);
    for (iter.first(); iter.isValid(); iter.next())
    {
        const char* arg = iter.query();
        
        if (stricmp(arg, "-h") == 0 || stricmp(arg, "--help") == 0)
        {
            displayHelp();
            exit(0);
        }
        else if (stricmp(arg, "--version") == 0)
        {
            displayVersion();
            exit(0);
        }
        else if (stricmp(arg, "--full-scan") == 0)
        {
            args.fullScan = true;
        }
        else if (stricmp(arg, "-s") == 0 || stricmp(arg, "--string-type") == 0)
        {
            iter.next();
            if (!iter.isValid())
                throw MakeStringException(1, "Error: --string-type requires an argument");
            
            args.stringType = iter.query();
            
            // Validate string type
            if (stricmp(args.stringType.c_str(), "UTF8") != 0 &&
                stricmp(args.stringType.c_str(), "STRING") != 0 &&
                stricmp(args.stringType.c_str(), "VARSTRING") != 0 &&
                stricmp(args.stringType.c_str(), "UNICODE") != 0)
            {
                StringBuffer msg;
                msg.appendf("Error: Invalid string type '%s'\nMust be one of: UTF8, STRING, VARSTRING, UNICODE", args.stringType.c_str());
                throw MakeStringException(1, "%s", msg.str());
            }
        }
        else if (arg[0] == '-')
        {
            StringBuffer msg;
            msg.appendf("Error: Unknown option '%s'\nUse --help for usage information", arg);
            throw MakeStringException(1, "%s", msg.str());
        }
        else
        {
            args.files.push_back(arg);
        }
    }
    
    // If no files specified, read from stdin
    args.readFromStdin = args.files.empty();
    
    return args;
}

} // namespace xml2ecl_cli

