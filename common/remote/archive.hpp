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

#ifndef ARCHIVE_FILE_HPP
#define ARCHIVE_FILE_HPP

#include "jfile.hpp"

#ifdef REMOTE_EXPORTS
#define REMOTE_API __declspec(dllexport)
#else
#define REMOTE_API __declspec(dllimport)
#endif


/*
 * Direct access to files in zip archives, without needing to extract them first
 * Installs hooks into createIFile, spotting filenames of the form /my/directory/myfile.zip/{password}/path/within/archive
 */

extern REMOTE_API IFile * createIFileInArchive(const char * _filename); // create directly without installing createIFile hook
extern REMOTE_API void installArchiveFileHook();
extern REMOTE_API void removeArchiveFileHook();

#endif
