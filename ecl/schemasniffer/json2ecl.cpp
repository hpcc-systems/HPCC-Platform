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

#include "json2ecl.hpp"
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
const size_t MAX_ARRAY_SAMPLES = 500;  // Limit array element sampling to avoid scanning huge files

//==============================================================================
// JSON Parsing Implementation
//==============================================================================

// Forward declarations for internal functions
BaseType jsonValueToBaseType(const IPropertyTree* node);
std::unique_ptr<SchemaItem> buildSchemaFromTree(const IPropertyTree* tree, bool fullScan);
std::string generateDatasetComment(const std::string& rootName, const std::string& layoutName, bool needsNoroot);
std::tuple<std::unique_ptr<SchemaItem>, bool> parseJsonContent(const char* jsonContent, const char* sourceName, bool fullScan);
std::tuple<std::unique_ptr<SchemaItem>, bool> parseJsonFileStreamingImpl(const char* filename, bool fullScan);

// Convert JSON value type to our BaseType
BaseType jsonValueToBaseType(const IPropertyTree* node)
{
    if (!node)
        return BaseType::NullValue;
    
    // Get the node's value
    const char* val = node->queryProp(nullptr);
    if (!val || !*val)
        return BaseType::NullValue;
    
    // Delegate to common implementation
    // Note: ptree converts quoted numbers to numbers, so we can't distinguish
    return stringValueToBaseType(val);
}

// Build schema structure from IPropertyTree
std::unique_ptr<SchemaItem> buildSchemaFromTree(const IPropertyTree* tree, bool fullScan)
{
    if (!tree)
        return std::make_unique<ValueItem>();
    
    // Check if this node has child elements
    Owned<IPropertyTreeIterator> iter = tree->getElements("*");
    if (!iter || !iter->first())
    {
        // No children - check if it's an empty object or a simple value
        const char* val = tree->queryProp(nullptr);
        if (!val || !*val)
        {
            // No value - this is an empty object {}
            return std::make_unique<ObjectItem>();
        }
        
        // Has a value - this is a simple value
        auto valueItem = std::make_unique<ValueItem>();
        BaseType type = jsonValueToBaseType(tree);
        valueItem->addValueType(type);
        return valueItem;
    }
    
    // Has children - collect them and group by name to detect arrays
    // Use vector of pairs to preserve order while grouping
    std::vector<std::pair<std::string, std::vector<const IPropertyTree*>>> fieldGroups;
    
    do {
        const char* fieldName = iter->query().queryName();
        if (!fieldName)
            continue;
        
        // Find existing group or create new one
        auto it = std::find_if(fieldGroups.begin(), fieldGroups.end(),
                               [fieldName](const auto& pair) { return pair.first == fieldName; });
        
        if (it != fieldGroups.end())
            it->second.push_back(&iter->query());
        else
            fieldGroups.push_back({fieldName, {&iter->query()}});
        
    } while (iter->next());
    
    // Now process each field
    auto objectItem = std::make_unique<ObjectItem>();
    
    for (const auto& fieldGroup : fieldGroups)
    {
        const std::string& fieldName = fieldGroup.first;
        const auto& elements = fieldGroup.second;
        
        if (elements.size() == 1)
        {
            // Single element - process normally
            auto fieldSchema = buildSchemaFromTree(elements[0], fullScan);
            objectItem->addOrMergeField(fieldName, std::move(fieldSchema));
        }
        else
        {
            // Multiple elements with same name - this is an array
            // Limit sampling: stop after 500 consecutive unchanged merges (unless fullScan is enabled)
            auto arrayItem = std::make_unique<ArrayItem>();
            size_t unchangedCount = 0;
            size_t limit = fullScan ? elements.size() : MAX_ARRAY_SAMPLES;
            
            for (size_t i = 0; i < elements.size() && unchangedCount < limit; i++)
            {
                auto elementSchema = buildSchemaFromTree(elements[i], fullScan);
                bool changed = arrayItem->mergeElementType(std::move(elementSchema));
                
                if (changed)
                    unchangedCount = 0;  // Reset counter on change
                else
                    unchangedCount++;    // Increment on no change
            }
            
            objectItem->addOrMergeField(fieldName, std::move(arrayItem));
        }
    }
    
    return objectItem;
}

// Generate the dataset comment line for JSON
std::string generateDatasetComment(const std::string& rootName, const std::string& layoutName, bool needsNoroot)
{
    std::string comment = "// ds := DATASET('~data::";
    
    // Convert root name to lowercase for the dataset name
    std::string lowercaseName = rootName;
    std::transform(lowercaseName.begin(), lowercaseName.end(), lowercaseName.begin(), 
                   [](unsigned char c) { return std::tolower(c); });
    comment += lowercaseName;
    comment += "', ";
    comment += layoutName;
    comment += ", JSON(";
    
    if (needsNoroot)
        comment += "NOROOT";
    
    comment += "));\n\n";
    
    return comment;
}

// Parse JSON content, handling both single documents and multiple top-level objects (NDJSON)
// Returns tuple of (schema, needsNoroot)
std::tuple<std::unique_ptr<SchemaItem>, bool> parseJsonContent(const char* jsonContent, const char* sourceName, bool fullScan)
{
    bool needsNoroot = false;
    
    // First, try parsing as a single JSON document
    try
    {
        Owned<IPropertyTree> tree = createPTreeFromJSONString(jsonContent, ipt_ordered, ptr_ignoreWhiteSpace);
        if (tree)
        {
            auto schema = buildSchemaFromTree(tree, fullScan);
            return std::make_tuple(std::move(schema), false);
        }
    }
    catch (...)
    {
        // Fall through to try NDJSON parsing
    }
    
    // If single document parsing failed, try parsing as NDJSON (newline-delimited JSON)
    // Split content by lines and parse each line as a separate JSON object
    // Stop after 500 consecutive unchanged merges (unless fullScan is enabled)
    std::unique_ptr<SchemaItem> mergedSchema;
    const char* lineStart = jsonContent;
    const char* current = jsonContent;
    size_t objectCount = 0;
    size_t unchangedCount = 0;
    size_t limit = fullScan ? SIZE_MAX : MAX_ARRAY_SAMPLES;
    
    while (*current && unchangedCount < limit)
    {
        // Find end of line
        while (*current && *current != '\n' && *current != '\r')
            current++;
        
        // Extract line
        size_t lineLen = current - lineStart;
        if (lineLen > 0)
        {
            // Skip leading whitespace
            const char* trimStart = lineStart;
            while (trimStart < current && (*trimStart == ' ' || *trimStart == '\t'))
                trimStart++;
            
            // Skip trailing whitespace
            const char* trimEnd = current;
            while (trimEnd > trimStart && (*(trimEnd-1) == ' ' || *(trimEnd-1) == '\t'))
                trimEnd--;
            
            size_t trimLen = trimEnd - trimStart;
            
            // Parse this line if it's not empty
            if (trimLen > 0)
            {
                StringBuffer lineContent;
                lineContent.append(trimLen, trimStart);
                
                try
                {
                    Owned<IPropertyTree> lineTree = createPTreeFromJSONString(lineContent.str(), ipt_ordered, ptr_ignoreWhiteSpace);
                    if (lineTree)
                    {
                        auto lineSchema = buildSchemaFromTree(lineTree, fullScan);
                        
                        if (!mergedSchema)
                        {
                            mergedSchema = std::move(lineSchema);
                            unchangedCount = 0;  // First object, reset counter
                        }
                        else
                        {
                            bool changed = mergeSchemaItems(mergedSchema.get(), lineSchema.get());
                            if (changed)
                                unchangedCount = 0;  // Reset counter on change
                            else
                                unchangedCount++;    // Increment on no change
                        }
                        
                        objectCount++;
                    }
                }
                catch (...)
                {
                    // Skip lines that don't parse as JSON
                }
            }
        }
        
        // Skip line endings
        while (*current && (*current == '\n' || *current == '\r'))
            current++;
        
        lineStart = current;
    }
    
    // If we successfully parsed multiple objects, set NOROOT flag
    if (objectCount > 1)
        needsNoroot = true;
    
    if (!mergedSchema)
        throw MakeStringException(0, "Failed to parse JSON from %s", sourceName);
    
    return std::make_tuple(std::move(mergedSchema), needsNoroot);
}

// Streaming NDJSON parser - processes file incrementally without loading entire file into memory
// This enables handling files larger than available RAM (e.g., multi-TB files)
// Returns tuple of (schema, needsNoroot)
std::tuple<std::unique_ptr<SchemaItem>, bool> parseJsonFileStreamingImpl(const char* filename, bool fullScan)
{
    Owned<IFile> file = createIFile(filename);
    if (!file->exists())
        throw MakeStringException(0, "File not found: %s", filename);
    
    Owned<IFileIO> fileIO = file->open(IFOread);
    if (!fileIO)
        throw MakeStringException(0, "Could not open file: %s", filename);
    
    offset_t fileSize = file->size();
    
    // Try to parse as single JSON document first (read up to 10MB)
    const size32_t singleDocLimit = 10 * 1024 * 1024;  // 10MB
    if (fileSize <= singleDocLimit)
    {
        // Small enough - try as single document
        StringBuffer jsonContent;
        jsonContent.ensureCapacity((size32_t)fileSize);
        char* buf = (char*)jsonContent.reserve((size32_t)fileSize);
        size32_t bytesRead = fileIO->read(0, (size32_t)fileSize, buf);
        jsonContent.setLength(bytesRead);
        
        try
        {
            Owned<IPropertyTree> tree = createPTreeFromJSONString(jsonContent.str(), ipt_ordered, ptr_ignoreWhiteSpace);
            if (tree)
            {
                auto schema = buildSchemaFromTree(tree, fullScan);
                return std::make_tuple(std::move(schema), false);
            }
        }
        catch (...)
        {
            // Failed to parse as single document, fall through to NDJSON streaming
        }
    }
    
    // Process as NDJSON with true streaming (constant memory usage)
    std::unique_ptr<SchemaItem> mergedSchema;
    size_t objectCount = 0;
    size_t unchangedCount = 0;
    size_t limit = fullScan ? SIZE_MAX : MAX_ARRAY_SAMPLES;
    
    StringBuffer lineBuffer;  // Accumulates partial lines across chunks
    const size32_t chunkSize = 64 * 1024;  // 64KB chunks
    MemoryAttr chunkBuf(chunkSize);
    offset_t filePos = 0;
    
    while (filePos < fileSize && unchangedCount < limit)
    {
        // Read next chunk
        size32_t toRead = (size32_t)std::min((offset_t)chunkSize, fileSize - filePos);
        size32_t bytesRead = fileIO->read(filePos, toRead, chunkBuf.bufferBase());
        if (bytesRead == 0)
            break;
        filePos += bytesRead;
        
        // Process lines in this chunk
        const char* chunkStart = (const char*)chunkBuf.bufferBase();
        const char* chunkEnd = chunkStart + bytesRead;
        const char* lineStart = chunkStart;
        
        for (const char* p = chunkStart; p < chunkEnd && unchangedCount < limit; ++p)
        {
            if (*p == '\n' || *p == '\r')
            {
                // Found end of line - append to buffer and process
                if (p > lineStart || lineBuffer.length() > 0)
                {
                    lineBuffer.append(p - lineStart, lineStart);
                    
                    // Trim whitespace
                    const char* trimStart = lineBuffer.str();
                    const char* trimEnd = trimStart + lineBuffer.length();
                    while (trimStart < trimEnd && (*trimStart == ' ' || *trimStart == '\t'))
                        trimStart++;
                    while (trimEnd > trimStart && (*(trimEnd-1) == ' ' || *(trimEnd-1) == '\t'))
                        trimEnd--;
                    
                    if (trimEnd > trimStart)
                    {
                        // Parse this line as JSON
                        StringBuffer trimmedLine;
                        trimmedLine.append(trimEnd - trimStart, trimStart);
                        
                        try
                        {
                            Owned<IPropertyTree> lineTree = createPTreeFromJSONString(trimmedLine.str(), ipt_ordered, ptr_ignoreWhiteSpace);
                            if (lineTree)
                            {
                                auto lineSchema = buildSchemaFromTree(lineTree, fullScan);
                                
                                if (!mergedSchema)
                                {
                                    mergedSchema = std::move(lineSchema);
                                    unchangedCount = 0;
                                }
                                else
                                {
                                    bool changed = mergeSchemaItems(mergedSchema.get(), lineSchema.get());
                                    unchangedCount = changed ? 0 : unchangedCount + 1;
                                }
                                
                                objectCount++;
                            }
                        }
                        catch (...)
                        {
                            // Skip lines that don't parse as JSON
                        }
                    }
                    
                    // Clear buffer for next line
                    lineBuffer.clear();
                }
                
                // Skip consecutive newlines
                lineStart = p + 1;
                while (lineStart < chunkEnd && (*lineStart == '\n' || *lineStart == '\r'))
                {
                    lineStart++;
                    p++;
                }
            }
        }
        
        // Save any partial line for next chunk
        if (lineStart < chunkEnd)
        {
            lineBuffer.append(chunkEnd - lineStart, lineStart);
        }
    }
    
    // Process any remaining content in buffer
    if (lineBuffer.length() > 0 && unchangedCount < limit)
    {
        const char* trimStart = lineBuffer.str();
        const char* trimEnd = trimStart + lineBuffer.length();
        while (trimStart < trimEnd && (*trimStart == ' ' || *trimStart == '\t'))
            trimStart++;
        while (trimEnd > trimStart && (*(trimEnd-1) == ' ' || *(trimEnd-1) == '\t' || *(trimEnd-1) == '\n' || *(trimEnd-1) == '\r'))
            trimEnd--;
        
        if (trimEnd > trimStart)
        {
            StringBuffer trimmedLine;
            trimmedLine.append(trimEnd - trimStart, trimStart);
            
            try
            {
                Owned<IPropertyTree> lineTree = createPTreeFromJSONString(trimmedLine.str(), ipt_ordered, ptr_ignoreWhiteSpace);
                if (lineTree)
                {
                    auto lineSchema = buildSchemaFromTree(lineTree, fullScan);
                    
                    if (!mergedSchema)
                    {
                        mergedSchema = std::move(lineSchema);
                    }
                    else
                    {
                        mergeSchemaItems(mergedSchema.get(), lineSchema.get());
                    }
                    
                    objectCount++;
                }
            }
            catch (...)
            {
                // Skip if doesn't parse
            }
        }
    }
    
    bool needsNoroot = (objectCount > 1);
    
    if (!mergedSchema)
        throw MakeStringException(0, "Failed to parse JSON from %s", filename);
    
    return std::make_tuple(std::move(mergedSchema), needsNoroot);
}

} // anonymous namespace

//==============================================================================
// Public API Implementation
//==============================================================================

std::tuple<std::unique_ptr<SchemaItem>, bool> parseJsonFileWithFlags(const char* filename, bool fullScan)
{
    try
    {
        // Use streaming parser for memory-efficient processing
        // Handles files larger than available RAM (e.g., multi-TB files)
        return parseJsonFileStreamingImpl(filename, fullScan);
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        e->Release();
        throw MakeStringException(0, "Error parsing JSON file: %s", msg.str());
    }
}

std::unique_ptr<SchemaItem> parseJsonFile(const char* filename)
{
    auto result = parseJsonFileWithFlags(filename);
    return std::move(std::get<0>(result));
}

std::unique_ptr<SchemaItem> parseJsonStream(std::istream& stream)
{
    try
    {
        // Read stream into string buffer
        // TODO: Could optimize this for streaming
        StringBuffer jsonContent;
        char buffer[1024 * 1024];  // 1MB buffer for better I/O performance
        
        while (stream.read(buffer, sizeof(buffer)) || stream.gcount() > 0)
        {
            jsonContent.append((size32_t)stream.gcount(), buffer);
        }
        
        // Parse JSON content (handles both single documents and NDJSON)
        auto result = parseJsonContent(jsonContent.str(), "stdin", false);
        // For this interface, we only return the schema (NOROOT tracking handled in processFiles)
        return std::move(std::get<0>(result));
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        e->Release();
        throw MakeStringException(0, "Error parsing JSON stream: %s", msg.str());
    }
}

//==============================================================================
// Application Functions (CLI - Internal use only)
//==============================================================================

namespace json2ecl_cli {

int processFiles(const std::vector<std::string>& files, 
                 const std::string& stringType,
                 bool readFromStdin,
                 bool fullScan)
{
    try
    {
        std::unique_ptr<SchemaItem> schema;
        bool needsNoroot = false;
        
        if (readFromStdin)
        {
            // Read from stdin
            StringBuffer jsonContent;
            char buffer[1024 * 1024];  // 1MB buffer for better I/O performance
            
            while (std::cin.read(buffer, sizeof(buffer)) || std::cin.gcount() > 0)
            {
                jsonContent.append((size32_t)std::cin.gcount(), buffer);
            }
            
            auto result = parseJsonContent(jsonContent.str(), "stdin", fullScan);
            schema = std::move(std::get<0>(result));
            needsNoroot = std::get<1>(result);
        }
        else
        {
            // Parse files
            if (files.empty())
                return 0;
            
            // Parse first file and check for NOROOT
            Owned<IFile> file = createIFile(files[0].c_str());
            if (!file->exists())
                throw MakeStringException(0, "File not found: %s", files[0].c_str());
            
            Owned<IFileIO> fileIO = file->open(IFOread);
            if (!fileIO)
                throw MakeStringException(0, "Could not open file: %s", files[0].c_str());
            
            offset_t fileSize = file->size();
            StringBuffer jsonContent;
            size32_t readSize = (size32_t)std::min((offset_t)0x10000000, fileSize);
            MemoryAttr buf(readSize);
            
            offset_t pos = 0;
            while (pos < fileSize)
            {
                size32_t toRead = (size32_t)std::min((offset_t)readSize, fileSize - pos);
                size32_t bytesRead = fileIO->read(pos, toRead, buf.bufferBase());
                if (bytesRead == 0)
                    break;
                jsonContent.append(bytesRead, (const char*)buf.bufferBase());
                pos += bytesRead;
            }
            
            auto result = parseJsonContent(jsonContent.str(), files[0].c_str(), fullScan);
            schema = std::move(std::get<0>(result));
            needsNoroot = std::get<1>(result);
            
            // Merge additional files and validate NOROOT consistency
            for (size_t i = 1; i < files.size(); ++i)
            {
                auto fileResult = parseJsonFileWithFlags(files[i].c_str(), fullScan);
                bool fileNeedsNoroot = std::get<1>(fileResult);
                
                // Validate that all files have consistent NOROOT requirements
                if (fileNeedsNoroot != needsNoroot)
                {
                    throw MakeStringException(0, 
                        "Incompatible file structures: %s %s NOROOT but %s %s NOROOT. "
                        "All files must have the same structure (all single objects or all NDJSON).",
                        files[0].c_str(), needsNoroot ? "requires" : "does not require",
                        files[i].c_str(), fileNeedsNoroot ? "requires" : "does not require");
                }
                
                mergeSchemaItems(schema.get(), std::get<0>(fileResult).get());
            }
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
        std::string datasetComment = generateDatasetComment(rootName, layoutName, needsNoroot);
        
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

//==============================================================================
// Command-Line Interface Helper Functions
//==============================================================================

void displayHelp()
{
    std::cout << "json2ecl - Generate ECL RECORD definitions from JSON data\n\n";
    std::cout << "Usage: json2ecl [OPTIONS] [FILES...]\n\n";
    std::cout << "Options:\n";
    std::cout << "  -h, --help              Display this help message\n";
    std::cout << "  -s, --string-type TYPE  ECL string type (UTF8, STRING, VARSTRING, UNICODE)\n";
    std::cout << "                          Default: UTF8\n";
    std::cout << "  --full-scan             Scan all array/NDJSON elements (no limit)\n";
    std::cout << "  --version               Display version information\n\n";
    std::cout << "Description:\n";
    std::cout << "  json2ecl examines JSON data and deduces the ECL RECORD definitions\n";
    std::cout << "  necessary to parse it. The resulting ECL definitions are returned\n";
    std::cout << "  via standard out.\n\n";
    std::cout << "  JSON data can be supplied as one or more files or via standard input.\n\n";
    std::cout << "  Multiple files are parsed as if they have the same record structure,\n";
    std::cout << "  useful when not all fields are defined in a single file.\n\n";
    std::cout << "  Supports both standard JSON documents and NDJSON (newline-delimited JSON)\n";
    std::cout << "  format. Multiple top-level JSON objects automatically add NOROOT flag.\n\n";
    std::cout << "Performance:\n";
    std::cout << "  For efficiency, when scanning arrays or NDJSON streams, the tool stops\n";
    std::cout << "  processing after 500 consecutive elements that don't modify the schema.\n";
    std::cout << "  This prevents unnecessary processing of large uniform datasets while\n";
    std::cout << "  ensuring schema accuracy. Use --full-scan to scan all elements if needed.\n\n";
    std::cout << "Known Limitations:\n";
    std::cout << "  - Quoted numbers (e.g., \"1234\") are treated as numbers, not strings\n";
    std::cout << "  - JSON null values cannot be distinguished from empty strings\n";
    std::cout << "  - Field names containing spaces may not be handled correctly due to\n";
    std::cout << "    underlying JSON parser behavior (e.g., 'this is a test' may become\n";
    std::cout << "    'this_sis_sa_stest'). Use underscore or camelCase naming instead.\n\n";
    std::cout << "Examples:\n";
    std::cout << "  json2ecl data.json\n";
    std::cout << "  json2ecl -s STRING file1.json file2.json\n";
    std::cout << "  cat data.json | json2ecl\n";
}

void displayVersion()
{
    std::cout << "json2ecl version " << VERSION << std::endl;
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
    
    // If no files specified, will read from stdin
    args.readFromStdin = args.files.empty();
    
    return args;
}

} // namespace json2ecl_cli
