/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#ifndef __HQLCACHE_HPP_
#define __HQLCACHE_HPP_

/*
 * Expand an archive as a collection of files with a directory.  Effectively the inverse of creating an archive.
 *
 * @param path          The root directory to create the file structure in.
 * @param archive       The archive to expand.
 * @param includePlugins Set to true if modules that are defined within a single source file should be expanded.
 */
extern HQL_API void expandArchive(const char * path, IPropertyTree * archive, bool includePlugins);

//Shared functions
void setDefinitionText(IPropertyTree * target, const char * prop, IFileContents * contents, bool checkDirty);

#endif
