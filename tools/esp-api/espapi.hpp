/*##############################################################################

    Copyright (C) 2024 HPCC Systems®.

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
#ifndef HPCC_INIT_HPP
#define HPCC_INIT_HPP

#include <vector>
#include <cstring>
#include "esdl_def.hpp"
#include "jfile.hpp"


class EspDef {
private:
    std::vector<std::string> structTrack;
    void describeType(const char* reqRes, int indent, std::ostream& out, bool Recursive);
    inline bool isStructInTrack(const std::string& reqRes);
    void getAllMethods(const char* serviceName, const char* methodName, bool &flagSuccess);
    void printHelper(const char* props);
    Owned<IEsdlDefinition> esdlDef;

protected:
    std::vector<const char*> allServicesList;
    std::vector<const char*> allMethodsList;
public:
    EspDef();

    int  loadAllServices();
    void loadAllMethods(const char* serviceName);
    void addFilesToDefinition(std::vector<Owned<IFile>> &files);
    void getFiles(std::vector<Owned<IFile>> &files);
    void getFiles(std::vector<Owned<IFile>> &files, const char* path);
    void describe(const char* serviceName, const char* methodName);
    void describe(const char* serviceName, const char* methodName, std::ostream& out);
    void printAllServices();
    void describeAllServices(std::ostream &out);
    void describeAllMethods(const char* serviceName, std::ostream &out);
    void printAllMethods();
    bool checkValidService(const char* serviceName);
    bool checkValidMethod(const char* methodName, const char* serviceName);
};

#endif
