/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#include "platform.h"
#include "jsocket.hpp"
#include <thread>
#include "udpipmap.hpp"

// Look up an IP to get a sender


#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class IpMapTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(IpMapTest);
        CPPUNIT_TEST(testIpMap);
        // CPPUNIT_TEST(testIpV6);
        CPPUNIT_TEST(testThread);
    CPPUNIT_TEST_SUITE_END();

    static unsigned *createMapEntry(const ServerIdentifier &)
    {
        return new unsigned(3);
    }

    void testIpMap()
    {
        unsigned five = 5;
        auto createMapEntry = [five](const ServerIdentifier &ip)
        {
            StringBuffer s;
            printf("adding ip %s\n", ip.getTraceText(s).str());
            return new unsigned(five);
        };
        IpMapOf<unsigned> map(createMapEntry);
        IpAddress notLocal("123.4.5.1");
        IpAddress notLocal2("123.4.6.1");
        IpAddress notLocal3("123.4.5.1");
        ASSERT(map.lookup(queryLocalIP())==five);
        ASSERT(&map.lookup(queryLocalIP())==&map.lookup(queryLocalIP()));
        ASSERT(&map.lookup(notLocal)!=&map.lookup(notLocal2));
        ASSERT(&map.lookup(notLocal)==&map.lookup(notLocal3));
        unsigned entries = 0;
        for (auto v:map)
        {
            ASSERT(v == 5);
            entries++;
        }
        printf("entries = %d\n", entries);
        ASSERT(entries == 3);
    }
#if 0
    // Current implementation of IpMapOf assumes ipv4
    void testIpV6()
    {
        IpAddress ip1("fe80::1c7e:ebe8:4ee8:6154");
        IpAddress ip2("fe80::1c37:fb7f:f657:d57a");
        ASSERT(ip1.fasthash() != ip2.fasthash());
        IpMapOf<unsigned> map(createMapEntry);
        ASSERT(&map.lookup(ip1)!=&map.lookup(ip2));
        ASSERT(&map.lookup(ip1)==&map.lookup(ip1));
    }
#endif

    class IpEntry
    {
    public:
        IpEntry()
        {
            numCreated++;
        }
        static RelaxedAtomic<unsigned> numCreated;
    };

    void testThread()
    {
        IpMapOf<IpEntry> map([](const ServerIdentifier &){return new IpEntry; });
        std::thread threads[100];
        Semaphore ready;
        for (int i = 0; i < 100; i++)
        {
            threads[i] = std::thread([&]()
            {
                ready.wait();
                IpAddress startIP("10.0.0.1");
                for (int i = 0; i < 1000; i++)
                {
                    map.lookup(startIP);
                    startIP.ipincrement(1);
                }
            });
        }
        ready.signal(100);
        for (int i = 0; i < 100; i++)
        {
            threads[i].join();
        }
        ASSERT(IpEntry::numCreated == 1000)
    }
};

RelaxedAtomic<unsigned> IpMapTest::IpEntry::numCreated {0};

CPPUNIT_TEST_SUITE_REGISTRATION( IpMapTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( IpMapTest, "IpMapTest" );


#endif
