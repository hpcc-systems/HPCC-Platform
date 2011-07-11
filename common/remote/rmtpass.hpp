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

#ifndef RMTPASS_HPP
#define RMTPASS_HPP

#ifdef REMOTE_EXPORTS
#define REMOTE_API __declspec(dllexport)
#else
#define REMOTE_API __declspec(dllimport)
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
