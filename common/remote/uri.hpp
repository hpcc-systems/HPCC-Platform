/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

#ifndef __JURI__
#define __JURI__

#include <uriparser/Uri.h>
#include "jlib.hpp"

// MORE - This is just a stub, structs below need to be commoned up with existing definitions elsewhere

// Supported URI schemes
enum URISchemeType
{
    URIScheme_error,
    URIScheme_hpcc,
    URIScheme_file
};

// Supported server types
enum URIServerType
{
    URIServer_local,  // Local file
    URIServer_dali,   // Names resolved by Dali
    URIServer_host    // Names resolved by DNS
};

// Supported file types
enum URIFileType
{
    URIFile_local,    // Local files
    URIFile_logic,    // Normal files
    URIFile_super,    // Super files
    URIFile_stream    // Stream files (to be implemented)
};

struct URIServerDescription
{
    StringAttr user;
    StringAttr host;
    unsigned port;
};

struct URIPathDescription
{
    StringAttr path;
    URIFileType type;
    StringAttr subname;  // Super files' sub
    unsigned index;      // Stream files
};

// ==================================================================================
/*
 * URIFileResolver is the interface that any resolver should implement to be used
 * by the URIResolution scheme, to provide a seamless interface to any HPCC engine
 * to handle files in a plethora of environments.
 *
 * This has not be thought about properly and does not concern
 * much of the initial URI investigations.
 */
//interface URIFileResolver
//{
//    // Returns a Read-only descriptor of a file. No Dali locks.
//    virtual IFileDescriptor *getFileDescriptor(StringAttr &filePath) = 0;
//    // Returns a distributed dali / local file
//    virtual IResolvedFile *getFile(StringAttr &filePath) = 0;
//    // Returns a distributed dali / local file form a pre-existing descriptor
//    virtual IResolvedFile *getFile(IFileDescriptor &fileDesc) = 0;
//    // Releases any lock and re-reads the information
//    virtual IFileDescriptor *releaseFile(IResolvedFile &file) = 0;
//};

// ==================================================================================
/*
 * URI deals with strings referring to paths that can be resolved in
 * many different ways. This object is immutable.
 *
 * Dali files (logic, super, stream), local files (on disk),
 * Web files (http, ftp, webdav) have different ways of resolving, and all of them
 * should have a consistent query mechanism from the HPCC engines point of view.
 *
 * The URI parser used is uriparser, from http://uriparser.sourceforge.net/
 */
class URI
{
    URISchemeType scheme;
    URIServerDescription server;
    URIPathDescription path;
    UriParserStateA state;
    UriUriA uri;

    void populateFields();

public:
    URI(const char* path);

    // Helper, to validate URI before creating object
    static bool isURI(const char *path);

    // Immutable
    URISchemeType getScheme() const
    {
        return scheme;
    }
    // Immutable
    const URIServerDescription * const getServer() const
    {
        return &server;
    }
    // Immutable
    const URIPathDescription * const getPath() const
    {
        return &path;
    }

    // MORE - is this the best way?
    void appendSchemeStr(StringBuffer& buf);
    void appendServerStr(StringBuffer& buf);
    void appendPathStr(StringBuffer& buf);
};

#endif /* __JURI__ */
