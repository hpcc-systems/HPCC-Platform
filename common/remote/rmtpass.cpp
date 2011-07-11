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

#include "jliball.hpp"

#include "platform.h"

#include "jlib.hpp"
#include "jsem.hpp"
#include "jthread.hpp"

#include "portlist.h"
#include "jsocket.hpp"

#include "rmtfile.hpp"
#include "rmtpass.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/common/remote/rmtpass.cpp $ $Id: rmtpass.cpp 62376 2011-02-04 21:59:58Z sort $");

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

