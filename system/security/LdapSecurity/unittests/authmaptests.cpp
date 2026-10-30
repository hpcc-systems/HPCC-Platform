/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#ifdef _USE_CPPUNIT

#include "authmap.ipp"
#include "SecurityResource.hpp"
#include "SecurityResourceList.hpp"
#include "unittests.hpp"

// CAuthMap is a public class (system/security/shared/authmap.ipp) reached via IAuthMap in
// production code (e.g. ESP binding setup); these tests exercise it directly through its public
// API -- no white-boxing required.
class LdapAuthMapTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(LdapAuthMapTest);
    CPPUNIT_TEST(testAddRejectsNullOrEmptyPath);
    CPPUNIT_TEST(testShouldAuthExactAndPrefixMatch);
    CPPUNIT_TEST(testShouldAuthDoesNotMatchPartialPathSegment);
    CPPUNIT_TEST(testShouldAuthNullOrEmptyPath);
    CPPUNIT_TEST(testQueryResourceListPrefersLongestMatch);
    CPPUNIT_TEST(testQueryResourceListNoMatch);
    CPPUNIT_TEST(testGetResourceListWildcardMergesAllLists);
    CPPUNIT_TEST(testGetResourceListClonesRatherThanAliases);
    CPPUNIT_TEST_SUITE_END();

public:
    // *(spec-driven)*: add() with a NULL/empty path is documented (DBGLOG message) as a rejected
    // input, and must not be added to the lookup table. add() returns before taking ownership of
    // the list on this branch, so the caller (here, the test) remains responsible for releasing
    // it -- each list is kept in an Owned<> so it's released regardless of add()'s outcome.
    void testAddRejectsNullOrEmptyPath()
    {
        CAuthMap authMap;
        Owned<ISecResourceList> list1 = new CSecurityResourceList("r");
        CPPUNIT_ASSERT_EQUAL(-1, authMap.add(nullptr, list1));
        Owned<ISecResourceList> list2 = new CSecurityResourceList("r");
        CPPUNIT_ASSERT_EQUAL(-1, authMap.add("", list2));
        CPPUNIT_ASSERT(authMap.queryResourceList("/anything") == nullptr);
    }

    // *(characterization)*: shouldAuth()'s path-prefix matching is CAuthMap's core authorization
    // gate; pinning its current segment-boundary semantics (a registered prefix only matches at a
    // '/' boundary or end-of-string, not mid-segment) so a future edit can't silently loosen or
    // tighten it without the change being deliberate. This is *not* a claim that the current
    // semantics are the intended design -- see the doc note on CAuthMap::shouldAuth().
    void testShouldAuthExactAndPrefixMatch()
    {
        CAuthMap authMap;
        authMap.add("/ws_workunits", new CSecurityResourceList("wu"));

        CPPUNIT_ASSERT(authMap.shouldAuth("/ws_workunits"));
        CPPUNIT_ASSERT(authMap.shouldAuth("/ws_workunits/"));
        CPPUNIT_ASSERT(authMap.shouldAuth("/ws_workunits/submit"));
    }

    void testShouldAuthDoesNotMatchPartialPathSegment()
    {
        CAuthMap authMap;
        authMap.add("/ws_workunits", new CSecurityResourceList("wu"));

        // "/ws_workunits2" shares the registered prefix as a string, but is not a path-segment
        // match -- must not be treated as authenticated.
        CPPUNIT_ASSERT(!authMap.shouldAuth("/ws_workunits2"));
        CPPUNIT_ASSERT(!authMap.shouldAuth("/ws_work"));
        CPPUNIT_ASSERT(!authMap.shouldAuth("/other"));
    }

    void testShouldAuthNullOrEmptyPath()
    {
        CAuthMap authMap;
        authMap.add("/ws_workunits", new CSecurityResourceList("wu"));

        CPPUNIT_ASSERT(!authMap.shouldAuth(nullptr));
        CPPUNIT_ASSERT(!authMap.shouldAuth(""));
    }

    // *(spec-driven)*: queryResourceList() is documented (comment in authmap.ipp) to find the
    // longest matching path prefix when more than one registered prefix matches.
    void testQueryResourceListPrefersLongestMatch()
    {
        CAuthMap authMap;
        authMap.add("/a", new CSecurityResourceList("short"));
        authMap.add("/a/b", new CSecurityResourceList("long"));

        ISecResourceList *found = authMap.queryResourceList("/a/b/c");
        CPPUNIT_ASSERT(found != nullptr);
        CPPUNIT_ASSERT_EQUAL_STR("long", found->getName());
    }

    void testQueryResourceListNoMatch()
    {
        CAuthMap authMap;
        authMap.add("/a/b", new CSecurityResourceList("long"));

        CPPUNIT_ASSERT(authMap.queryResourceList("/other") == nullptr);
        CPPUNIT_ASSERT(authMap.queryResourceList(nullptr) == nullptr);
        CPPUNIT_ASSERT(authMap.queryResourceList("") == nullptr);
    }

    // *(spec-driven)*: getResourceList("*") is documented to merge every registered resource
    // list's contents into one combined clone.
    void testGetResourceListWildcardMergesAllLists()
    {
        CAuthMap authMap;
        CSecurityResourceList *list1 = new CSecurityResourceList("list1");
        list1->addResource("res1");
        CSecurityResourceList *list2 = new CSecurityResourceList("list2");
        list2->addResource("res2");
        authMap.add("/a", list1);
        authMap.add("/b", list2);

        Owned<ISecResourceList> merged = authMap.getResourceList("*");
        CPPUNIT_ASSERT(merged != nullptr);
        CPPUNIT_ASSERT_EQUAL(2u, merged->count());
        // getResource() returns a linked (ref-counted) reference per CSecurityResourceList's
        // implementation ("return LINK(r);") -- the caller owns it and must release it.
        Owned<ISecResource> res1 = merged->getResource("res1");
        CPPUNIT_ASSERT(res1 != nullptr);
        Owned<ISecResource> res2 = merged->getResource("res2");
        CPPUNIT_ASSERT(res2 != nullptr);
    }

    // getResourceList() (unlike queryResourceList()) is documented to return a clone, so the
    // caller can't mutate CAuthMap's internal state through the returned pointer.
    void testGetResourceListClonesRatherThanAliases()
    {
        CAuthMap authMap;
        CSecurityResourceList *list1 = new CSecurityResourceList("list1");
        list1->addResource("res1");
        authMap.add("/a", list1);

        Owned<ISecResourceList> cloned = authMap.getResourceList("/a");
        CPPUNIT_ASSERT(cloned != nullptr);
        CPPUNIT_ASSERT(cloned.get() != static_cast<ISecResourceList*>(list1));

        cloned->addResource("res2");
        ISecResourceList *stillInternal = authMap.queryResourceList("/a");
        CPPUNIT_ASSERT_EQUAL(1u, stillInternal->count());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(LdapAuthMapTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(LdapAuthMapTest, "LdapAuthMapTest");

#endif // _USE_CPPUNIT
