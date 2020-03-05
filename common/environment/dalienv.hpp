/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef DALIENV_HPP
#define DALIENV_HPP

#ifdef ENVIRONMENT_EXPORTS
#define ENVIRONMENT_API DECL_EXPORT
#else
#define ENVIRONMENT_API DECL_IMPORT
#endif

#include "environment.hpp"

interface IFile;
class RemoteFilename;

extern ENVIRONMENT_API EnvMachineOS queryOS(const IpAddress & ip);


extern ENVIRONMENT_API bool canAccessFilesDirectly(const RemoteFilename & file);
extern ENVIRONMENT_API bool canAccessFilesDirectly(const IpAddress & ip);
extern ENVIRONMENT_API bool canAccessFilesDirectly(const char * ipText);
extern ENVIRONMENT_API const char * querySlaveExecutable(const char * keyName, const char * exeName, const char * version, const IpAddress &ip, StringBuffer &progpath, StringBuffer &workdir);
extern ENVIRONMENT_API void setCanAccessDirectly(RemoteFilename & file);

extern ENVIRONMENT_API bool canSpawnChildProcess(const IpAddress & ip);

extern ENVIRONMENT_API bool getRemoteRunInfo(const char * keyName, const char * exeName, const char * version, const IpAddress &ip, StringBuffer &progpath, StringBuffer &workdir,INode *remotedali, unsigned timeout);

extern ENVIRONMENT_API bool envGetConfigurationDirectory(const char *category, const char *component,const char *instance, StringBuffer &dirout);

extern ENVIRONMENT_API IPropertyTree *envGetNASConfiguration(); // return NAS config from environment
extern ENVIRONMENT_API IPropertyTree *envGetNASConfiguration(IPropertyTree *source);
// These methods filter the NAS hooks based on the callers IP, unless 'myEp' is supplied.
extern ENVIRONMENT_API void envInstallNASHooks(SocketEndpoint *myEp=NULL); // gets NAS config and sets up NAS hooks from it
extern ENVIRONMENT_API void envInstallNASHooks(IPropertyTree *nasPTree, SocketEndpoint *myEp=NULL); // Sets NAS hooks from user-supplied info
// like envInstallNASHooks but also returns which filters were installed
extern ENVIRONMENT_API IPropertyTree *envGetInstallNASHooks(SocketEndpoint *myEp=NULL);
extern ENVIRONMENT_API IPropertyTree *envGetInstallNASHooks(IPropertyTree *nasPTree, SocketEndpoint *myEp=NULL);

#endif
