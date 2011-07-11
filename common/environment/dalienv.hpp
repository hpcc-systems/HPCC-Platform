/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef DALIENV_HPP
#define DALIENV_HPP

#ifdef _WIN32
#ifdef ENVIRONMENT_EXPORTS
#define ENVIRONMENT_API __declspec(dllexport)
#else
#define ENVIRONMENT_API __declspec(dllimport)
#endif
#else
#define ENVIRONMENT_API
#endif

#include "environment.hpp"

interface IFile;
class RemoteFilename;

extern ENVIRONMENT_API EnvMachineOS queryOS(const IpAddress & ip);
extern "C" ENVIRONMENT_API void __stdcall setPasswordsFromSDS();
extern "C" ENVIRONMENT_API void __stdcall resetPasswordsFromSDS();
extern "C" ENVIRONMENT_API void __stdcall clearPasswordsFromSDS();


extern ENVIRONMENT_API bool canAccessFilesDirectly(const RemoteFilename & file);
extern ENVIRONMENT_API bool canAccessFilesDirectly(const IpAddress & ip);
extern ENVIRONMENT_API bool canAccessFilesDirectly(const char * ipText);
extern ENVIRONMENT_API const char * querySlaveExecutable(const char * keyName, const char * exeName, const char * version, const IpAddress &ip, StringBuffer &progpath, StringBuffer &workdir);
extern ENVIRONMENT_API void setCanAccessDirectly(RemoteFilename & file);

extern ENVIRONMENT_API bool canSpawnChildProcess(const IpAddress & ip);

extern ENVIRONMENT_API bool getRemoteRunInfo(const char * keyName, const char * exeName, const char * version, const IpAddress &ip, StringBuffer &progpath, StringBuffer &workdir,INode *remotedali, unsigned timeout);

extern ENVIRONMENT_API bool envGetConfigurationDirectory(const char *category, const char *component,const char *instance, StringBuffer &dirout);

#endif
