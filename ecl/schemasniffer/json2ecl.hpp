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

#pragma once

#include "common.hpp"
#include "jptree.hpp"
#include <memory>
#include <string>
#include <iostream>

//==============================================================================
// JSON Parsing Functions
//==============================================================================

// Parse JSON from a file and return schema structure
std::unique_ptr<SchemaItem> parseJsonFile(const char* filename);

// Parse JSON from a file and return (schema, needsNoroot) tuple
std::tuple<std::unique_ptr<SchemaItem>, bool> parseJsonFileWithFlags(const char* filename, bool fullScan = false);

// Parse JSON from a stream and return schema structure
std::unique_ptr<SchemaItem> parseJsonStream(std::istream& stream);

//==============================================================================
// Command-line Interface Functions 
//==============================================================================

namespace json2ecl_cli {

// Parse command-line arguments
ParsedArgs parseCommandLine(int argc, const char* argv[]);

// Main entry point for json2ecl processing
int processFiles(const std::vector<std::string>& files, 
                 const std::string& stringType,
                 bool readFromStdin = false,
                 bool fullScan = false);

} // namespace json2ecl_cli

// Display help text
void displayHelp();

// Display version information
void displayVersion();
