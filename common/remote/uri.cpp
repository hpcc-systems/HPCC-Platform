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

#include "uri.hpp"
#include "jexcept.hpp"

URI::URI(const char* path)
{
    state.uri = &uri;
    try {
        if (uriParseUriA(&state, path) != URI_SUCCESS)
            throw MakeStringException(-1, "Invalid URI '%s'", path);
        populateFields(); // In a format we understand
    }
    // On parser failure, but also system exceptions (bad alloc, etc)
    catch (IException *)
    {
        uriFreeUriMembersA(&uri);
        throw;
    }
    uriFreeUriMembersA(&uri);
}

// Helper, to validate URI before creating object
bool URI::isURI(const char *path)
{
    UriParserStateA state;
    UriUriA uri;
    state.uri = &uri;
    bool match = (uriParseUriA(&state, path) == URI_SUCCESS);
    uriFreeUriMembersA(&uri);
    return match;
}

void URI::populateFields()
{
    // Scheme (defines which resolver to use, see above)
    StringBuffer schemeStr(uri.scheme.afterLast - uri.scheme.first, uri.scheme.first);
    schemeStr.toLowerCase();
    if (strcmp(schemeStr.str(), "hpcc") == 0)
        scheme = URIScheme_hpcc;
    else if (strcmp(schemeStr.str(), "file") == 0)
        scheme = URIScheme_file;
    else
        scheme = URIScheme_error;

    // Server
    server.user.set(uri.userInfo.first, uri.userInfo.afterLast - uri.userInfo.first);
    server.host.set(uri.hostText.first, uri.hostText.afterLast - uri.hostText.first);
    StringAttr portStr(uri.portText.first, uri.portText.afterLast - uri.portText.first);
    server.port = atoi(portStr.get()); // More - use default ports?

    // Path
    UriPathSegmentA* cur = uri.pathHead;
    StringBuffer pathStr;
    if (uri.absolutePath || scheme == URIScheme_file)
        pathStr.append("/");
    bool first = true;
    while (cur)
    {
        if (!first)
            pathStr.append("/");
        pathStr.append(cur->text.afterLast - cur->text.first, cur->text.first);
        first = false;
        cur = cur->next;
    }
    path.path.set(pathStr.str());

    // Extra info
    if (scheme == URIScheme_hpcc)
    {
        StringBuffer query(uri.query.afterLast - uri.query.first, uri.query.first);
        query.toLowerCase();
        if (strcmp(query.str(), "super") == 0)
        {
            path.type = URIFile_super;
            path.subname.set(uri.fragment.first, uri.fragment.afterLast - uri.fragment.first);
            path.index = 0;
        }
        else if (strcmp(query.str(), "stream") == 0)
        {
            path.type = URIFile_stream;
            StringAttr index(uri.fragment.first, uri.fragment.afterLast - uri.fragment.first);
            path.index = atoi(index.get());
        }
        else
        {
            path.type = URIFile_logic;
            path.index = 0;
        }
    }
    else
    {
        path.type = URIFile_local;
        path.index = 0;
    }
}

void URI::appendSchemeStr(StringBuffer& buf)
{
    switch(scheme)
    {
    case URIScheme_hpcc:
        buf.append("hpcc");
        return;
    case URIScheme_file:
        buf.append("file");
        return;
    default:
        buf.append("unknown");
        return;
    }
}

void URI::appendServerStr(StringBuffer& buf)
{
    if (!server.user.isEmpty())
        buf.append(server.user.get()).append("@");
    buf.append(server.host.get());
    if (server.port)
        buf.append(":").append(server.port);
}

void URI::appendPathStr(StringBuffer& buf)
{
    buf.append(path.path.get());
    switch(path.type)
    {
    case URIFile_super:
        buf.append("?super");
        break;
    case URIFile_stream:
        buf.append("?stream");
        break;
    }
    if (path.index)
        buf.append("#").append(path.index);
    else if (path.subname.length())
        buf.append("#").append(path.subname.get());
}
