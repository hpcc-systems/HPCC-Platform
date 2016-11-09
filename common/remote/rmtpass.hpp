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

#ifndef RMTPASS_HPP
#define RMTPASS_HPP

#ifdef REMOTE_EXPORTS
#define REMOTE_API DECL_EXPORT
#else
#define REMOTE_API DECL_IMPORT
#endif

class REMOTE_API CachedPassword : public CInterface
{
public:
    void deserialize(MemoryBuffer & in)     { ip.ipdeserialize(in); in.read(password).read(username); }
    void serialize(MemoryBuffer & out)      { ip.ipserialize(out); out.append(password).append(username); }

public:
    IpAddress ip;
    StringAttr password;
    StringAttr username;
};


class RemoteFilename;
class REMOTE_API CachedPasswordProvider : public CInterface, implements IPasswordProvider
{
public:
    IMPLEMENT_IINTERFACE

    virtual bool getPassword(const IpAddress & ip, StringBuffer & username, StringBuffer & password);

    void addPasswordForFilename(const char * filename);
    void addPasswordForFilename(RemoteFilename & filename);
    void addPasswordForIp(const IpAddress & ip);
    void clear();
    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out);
    bool hasPassword(const IpAddress & ip) const;

protected:
    CIArrayOf<CachedPassword>   passwords;
};


#endif
