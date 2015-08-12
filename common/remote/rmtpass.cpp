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

#include "jliball.hpp"

#include "platform.h"

#include "jlib.hpp"
#include "jsem.hpp"
#include "jthread.hpp"

#include "portlist.h"
#include "jsocket.hpp"

#include "rmtfile.hpp"
#include "rmtpass.hpp"

void CachedPasswordProvider::addPasswordForFilename(const char * filename)
{
    RemoteFilename remote;
    remote.setRemotePath(filename);
    addPasswordForIp(remote.queryIP());
}


void CachedPasswordProvider::addPasswordForFilename(RemoteFilename & filename)
{
    addPasswordForIp(filename.queryIP());
}

void CachedPasswordProvider::addPasswordForIp(const IpAddress & ip)
{
    if (!hasPassword(ip))
    {
        IPasswordProvider * provider = queryPasswordProvider();
        StringBuffer username, password;

        if (provider && provider->getPassword(ip, username, password))
        {
            CachedPassword & cur = * new CachedPassword;
            cur.ip.ipset(ip);
            cur.password.set(password.str());
            cur.username.set(username.str());
            passwords.append(cur);
        }
    }
}


void CachedPasswordProvider::clear()
{
    passwords.kill();
}

void CachedPasswordProvider::deserialize(MemoryBuffer & in)
{
    unsigned num;
    in.read(num);
    while (num--)
    {
        CachedPassword & cur = * new CachedPassword;
        cur.deserialize(in);
        passwords.append(cur);
    }
}

void CachedPasswordProvider::serialize(MemoryBuffer & out)
{
    unsigned num = passwords.ordinality();
    out.append(num);
    ForEachItemIn(idx, passwords)
        passwords.item(idx).serialize(out);
}

bool CachedPasswordProvider::getPassword(const IpAddress & ip, StringBuffer & username, StringBuffer & password)
{
    ForEachItemIn(idx, passwords)
    {
        CachedPassword & cur = passwords.item(idx);
        if (cur.ip.ipequals(ip))
        {
            username.append(cur.username);
            password.append(cur.password);
            return true;
        }
    }
    StringBuffer iptext;
    LOG(MCdebugInfo, unknownJob, "cached:getPassword(%s) failed", ip.getIpText(iptext).str());
    return false;
}

bool CachedPasswordProvider::hasPassword(const IpAddress & ip) const
{
    ForEachItemIn(idx, passwords)
    {
        CachedPassword & cur = passwords.item(idx);
        if (cur.ip.ipequals(ip))
            return true;
    }
    return false;
}

