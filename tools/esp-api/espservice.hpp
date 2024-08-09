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
#ifndef HPCC_SERVICE_H
#define HPCC_SERVICE_H

class EspService{
    public:

    const char* reqString = nullptr;
    const char* resType = nullptr;
    const char* reqType = nullptr;
    const char* username = nullptr;
    const char* password = nullptr;
    const char* url = nullptr;

    int sendRequest();

    EspService(const char* serviceName, const char* methodName, const char* reqString, const char* resType, const char* reqType, const char* target,
    const char* username, const char* password);

    EspService(const char* serviceName, const char* methodName, const char* reqString, const char* resType, const char* reqType, const char* target);

};

#endif
