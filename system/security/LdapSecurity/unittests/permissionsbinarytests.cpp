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

#include "permissions.ipp"
#include "unittests.hpp"

#include <vector>

#ifndef _WIN32

// The following overloads are only used internally within permissions.cpp
// (called before they are declared further down in the same translation
// unit) and are therefore not forward-declared in permissions.ipp/hpp.
// They still have external linkage, so they can be exercised directly here.
bool AddAccessAllowedAce(PACL pNewAcl, DWORD* pNewLength, PACL pAcl, DWORD dwAceRevision, DWORD AccessMask, PSID pSid);
bool AddAccessDeniedAce(PACL pNewAcl, DWORD* pNewLength, PACL pAcl, DWORD dwAceRevision, DWORD AccessMask, PSID pSid);
bool DeleteAce(PACL pAcl, DWORD dwAceIndex);

class PermissionsBinaryTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(PermissionsBinaryTest);
    CPPUNIT_TEST(testEqualSid);
    CPPUNIT_TEST(testGetAceBoundary);
    CPPUNIT_TEST(testAddAccessAllowedAceInPlaceBoundary);
    CPPUNIT_TEST(testAddAccessAllowedAceGrowBoundary);
    CPPUNIT_TEST(testAddAccessDeniedAceGrowBoundary);
    CPPUNIT_TEST(testDeleteAceBoundary);
    CPPUNIT_TEST(testGetSecurityDescriptorLength);
    CPPUNIT_TEST(testMakeSelfRelativeAbsoluteRoundTripNoAces);
    CPPUNIT_TEST(testMakeSelfRelativeAbsoluteRoundTripOneAce);
    CPPUNIT_TEST(testMakeSelfRelativeAbsoluteRoundTripMultipleAces);
    CPPUNIT_TEST_SUITE_END();

public:
    // ---- Synthetic SID/ACL construction helpers ----

    // Builds a SID with a single sub-authority (a single DWORD RID) into the
    // supplied buffer, which must be at least sizeofSid() == sizeof(SID) bytes.
    static void buildSid(BYTE* buf, DWORD rid, BYTE authorityId = 5)
    {
        PISID sid = (PISID)buf;
        sid->Revision = 1;
        sid->SubAuthorityCount = 1;
        memset(sid->IdentifierAuthority.Value, 0, sizeof(sid->IdentifierAuthority.Value));
        sid->IdentifierAuthority.Value[5] = authorityId;
        sid->SubAuthority[0] = rid;
    }

    // constexpr so array declarations sized via these helpers (e.g.
    // "BYTE aclBuf[sizeof(ACL) + 3 * aceSize()]") are compile-time constant
    // expressions rather than non-standard variable-length arrays.
    static constexpr unsigned sidSize()
    {
        // Matches sizeofSid()'s formula for a single sub-authority SID:
        // sizeof(SID) already accounts for one DWORD via ANYSIZE_ARRAY.
        return sizeof(SID);
    }

    static constexpr unsigned aceSize()
    {
        return sizeof(ACE_HEADER) + sizeof(ACCESS_MASK) + sidSize();
    }

    // Initializes pAcl (sized to hold numAces) and adds numAces ACCESS_ALLOWED
    // aces via AddAccessAllowedAce(), each carrying a distinct SID RID/mask so
    // ordering and content can be verified after later operations.
    static void buildAcl(BYTE* buf, unsigned aclBufSize, unsigned numAces, DWORD baseRid, DWORD baseMask)
    {
        PACL pAcl = (PACL)buf;
        CPPUNIT_ASSERT(InitializeAcl(pAcl, aclBufSize, ACL_REVISION));
        for (unsigned i = 0; i < numAces; i++)
        {
            BYTE sidBuf[sizeof(SID)];
            buildSid(sidBuf, baseRid + i);
            CPPUNIT_ASSERT(AddAccessAllowedAce(pAcl, ACL_REVISION, baseMask + i, (PSID)sidBuf));
        }
    }

    static void assertAceAt(PACL pAcl, DWORD index, BYTE expectedType, DWORD expectedMask, DWORD expectedRid)
    {
        void* pAce = nullptr;
        CPPUNIT_ASSERT(GetAce(pAcl, index, &pAce));
        PACCESS_ALLOWED_ACE ace = (PACCESS_ALLOWED_ACE)pAce;
        CPPUNIT_ASSERT_EQUAL((int)expectedType, (int)ace->Header.AceType);
        CPPUNIT_ASSERT_EQUAL((unsigned)expectedMask, (unsigned)ace->Mask);
        PISID sid = (PISID)&ace->SidStart;
        CPPUNIT_ASSERT_EQUAL((unsigned)expectedRid, (unsigned)sid->SubAuthority[0]);
    }

    // ---- EqualSid() ----

    void testEqualSid()
    {
        BYTE sidA[sizeof(SID)];
        BYTE sidB[sizeof(SID)];
        BYTE sidDiffRid[sizeof(SID)];
        buildSid(sidA, 1001);
        buildSid(sidB, 1001);
        buildSid(sidDiffRid, 1002);

        // Identical SIDs.
        CPPUNIT_ASSERT(EqualSid((PSID)sidA, (PSID)sidB));

        // SIDs differing only in the last (only) sub-authority.
        CPPUNIT_ASSERT(!EqualSid((PSID)sidA, (PSID)sidDiffRid));

        // SIDs with differing sub-authority counts (differing length).
        BYTE sidTwoSub[sizeof(SID) + sizeof(DWORD)];
        PISID twoSub = (PISID)sidTwoSub;
        twoSub->Revision = 1;
        twoSub->SubAuthorityCount = 2;
        memset(twoSub->IdentifierAuthority.Value, 0, sizeof(twoSub->IdentifierAuthority.Value));
        twoSub->IdentifierAuthority.Value[5] = 5;
        twoSub->SubAuthority[0] = 1001;
        twoSub->SubAuthority[1] = 500;
        CPPUNIT_ASSERT(!EqualSid((PSID)sidA, (PSID)twoSub));

        // NULL SIDs never compare equal.
        CPPUNIT_ASSERT(!EqualSid(nullptr, (PSID)sidA));
        CPPUNIT_ASSERT(!EqualSid((PSID)sidA, nullptr));
    }

    // ---- GetAce() boundary cases ----

    void testGetAceBoundary()
    {
        BYTE aclBuf[sizeof(ACL) + 3 * aceSize()];
        buildAcl(aclBuf, sizeof(aclBuf), 3, 100, 0x1);
        PACL pAcl = (PACL)aclBuf;

        // Index 0 and last valid index (2) succeed and return the expected ACE.
        assertAceAt(pAcl, 0, ACCESS_ALLOWED_ACE_TYPE, 0x1, 100);
        assertAceAt(pAcl, 2, ACCESS_ALLOWED_ACE_TYPE, 0x3, 102);

        // Out-of-range index fails.
        void* pAce = nullptr;
        CPPUNIT_ASSERT(!GetAce(pAcl, 3, &pAce));
        CPPUNIT_ASSERT(!GetAce(pAcl, 100, &pAce));

        // NULL ACL fails.
        CPPUNIT_ASSERT(!GetAce(nullptr, 0, &pAce));
    }

    // ---- AddAccessAllowedAce() (in-place, non-growing overload) ----

    void testAddAccessAllowedAceInPlaceBoundary()
    {
        // ACL buffer sized to hold exactly one ace.
        BYTE aclBuf[sizeof(ACL) + aceSize()];
        PACL pAcl = (PACL)aclBuf;
        CPPUNIT_ASSERT(InitializeAcl(pAcl, sizeof(aclBuf), ACL_REVISION));

        BYTE sidBuf[sizeof(SID)];
        buildSid(sidBuf, 200);

        // First ace fits exactly.
        CPPUNIT_ASSERT(AddAccessAllowedAce(pAcl, ACL_REVISION, 0x1, (PSID)sidBuf));
        CPPUNIT_ASSERT_EQUAL((unsigned)1, (unsigned)pAcl->AceCount);

        // A second ace does not fit in the remaining (zero) space.
        BYTE sidBuf2[sizeof(SID)];
        buildSid(sidBuf2, 201);
        CPPUNIT_ASSERT(!AddAccessAllowedAce(pAcl, ACL_REVISION, 0x2, (PSID)sidBuf2));
        CPPUNIT_ASSERT_EQUAL((unsigned)1, (unsigned)pAcl->AceCount);

        // NULL ACL and empty SID both rejected.
        CPPUNIT_ASSERT(!AddAccessAllowedAce(nullptr, ACL_REVISION, 0x1, (PSID)sidBuf));
        CPPUNIT_ASSERT(!AddAccessAllowedAce(pAcl, ACL_REVISION, 0x1, nullptr));
    }

    // ---- AddAccessAllowedAce() (growing overload) ----

    void testAddAccessAllowedAceGrowBoundary()
    {
        BYTE srcAclBuf[sizeof(ACL) + aceSize()];
        buildAcl(srcAclBuf, sizeof(srcAclBuf), 1, 300, 0x1);
        PACL pSrcAcl = (PACL)srcAclBuf;

        // Destination buffer too small: call fails and reports the required size.
        BYTE tooSmallBuf[sizeof(ACL) + aceSize()];
        PACL pTooSmall = (PACL)tooSmallBuf;
        DWORD requiredLen = sizeof(tooSmallBuf); // deliberately just short of what's needed
        CPPUNIT_ASSERT(!AddAccessAllowedAce(pTooSmall, &requiredLen, pSrcAcl, ACL_REVISION, 0x2, nullptr));
        // pSid == nullptr => sizeofSid()==0 => rejected as "empty sid" before the size check.
        BYTE sidBuf[sizeof(SID)];
        buildSid(sidBuf, 301);
        requiredLen = sizeof(tooSmallBuf); // still only room for the original single ace, no growth
        CPPUNIT_ASSERT(!AddAccessAllowedAce(pTooSmall, &requiredLen, pSrcAcl, ACL_REVISION, 0x2, (PSID)sidBuf));
        CPPUNIT_ASSERT_EQUAL((unsigned)(sizeof(ACL) + 2 * aceSize()), (unsigned)requiredLen);

        // Growing into a correctly sized destination buffer succeeds and appends the new ace.
        BYTE dstAclBuf[sizeof(ACL) + 2 * aceSize()];
        PACL pDstAcl = (PACL)dstAclBuf;
        DWORD dstLen = sizeof(dstAclBuf);
        CPPUNIT_ASSERT(AddAccessAllowedAce(pDstAcl, &dstLen, pSrcAcl, ACL_REVISION, 0x2, (PSID)sidBuf));
        CPPUNIT_ASSERT_EQUAL((unsigned)2, (unsigned)pDstAcl->AceCount);
        assertAceAt(pDstAcl, 0, ACCESS_ALLOWED_ACE_TYPE, 0x1, 300);
        assertAceAt(pDstAcl, 1, ACCESS_ALLOWED_ACE_TYPE, 0x2, 301);
    }

    // ---- AddAccessDeniedAce() (growing overload; there is no in-place overload) ----

    void testAddAccessDeniedAceGrowBoundary()
    {
        BYTE srcAclBuf[sizeof(ACL) + aceSize()];
        buildAcl(srcAclBuf, sizeof(srcAclBuf), 1, 400, 0x1);
        PACL pSrcAcl = (PACL)srcAclBuf;

        BYTE sidBuf[sizeof(SID)];
        buildSid(sidBuf, 401);

        // Destination buffer too small: fails and reports the required size.
        BYTE tooSmallBuf[sizeof(ACL) + aceSize()];
        PACL pTooSmall = (PACL)tooSmallBuf;
        DWORD requiredLen = sizeof(tooSmallBuf);
        CPPUNIT_ASSERT(!AddAccessDeniedAce(pTooSmall, &requiredLen, pSrcAcl, ACL_REVISION, 0x2, (PSID)sidBuf));
        CPPUNIT_ASSERT_EQUAL((unsigned)(sizeof(ACL) + 2 * aceSize()), (unsigned)requiredLen);

        // NULL source ACL / NULL destination buffer / empty SID rejected.
        DWORD len = sizeof(tooSmallBuf);
        CPPUNIT_ASSERT(!AddAccessDeniedAce(pTooSmall, &len, nullptr, ACL_REVISION, 0x2, (PSID)sidBuf));
        CPPUNIT_ASSERT(!AddAccessDeniedAce(nullptr, &len, pSrcAcl, ACL_REVISION, 0x2, (PSID)sidBuf));
        CPPUNIT_ASSERT(!AddAccessDeniedAce(pTooSmall, &len, pSrcAcl, ACL_REVISION, 0x2, nullptr));

        // Growing into a correctly sized buffer succeeds and appends an ACCESS_DENIED_ACE_TYPE ace.
        BYTE dstAclBuf[sizeof(ACL) + 2 * aceSize()];
        PACL pDstAcl = (PACL)dstAclBuf;
        DWORD dstLen = sizeof(dstAclBuf);
        CPPUNIT_ASSERT(AddAccessDeniedAce(pDstAcl, &dstLen, pSrcAcl, ACL_REVISION, 0x2, (PSID)sidBuf));
        CPPUNIT_ASSERT_EQUAL((unsigned)2, (unsigned)pDstAcl->AceCount);
        assertAceAt(pDstAcl, 0, ACCESS_ALLOWED_ACE_TYPE, 0x1, 400);
        assertAceAt(pDstAcl, 1, ACCESS_DENIED_ACE_TYPE, 0x2, 401);
    }

    // ---- DeleteAce() boundary cases ----

    void testDeleteAceBoundary()
    {
        // Deleting the sole ACE from a single-ACE list empties it.
        {
            BYTE aclBuf[sizeof(ACL) + aceSize()];
            buildAcl(aclBuf, sizeof(aclBuf), 1, 500, 0x1);
            PACL pAcl = (PACL)aclBuf;
            CPPUNIT_ASSERT(DeleteAce(pAcl, 0));
            CPPUNIT_ASSERT_EQUAL((unsigned)0, (unsigned)pAcl->AceCount);
        }

        // Deleting index 0 from a multi-ace list shifts remaining aces down,
        // preserving their relative order.
        {
            BYTE aclBuf[sizeof(ACL) + 3 * aceSize()];
            buildAcl(aclBuf, sizeof(aclBuf), 3, 600, 0x1);
            PACL pAcl = (PACL)aclBuf;
            CPPUNIT_ASSERT(DeleteAce(pAcl, 0));
            CPPUNIT_ASSERT_EQUAL((unsigned)2, (unsigned)pAcl->AceCount);
            assertAceAt(pAcl, 0, ACCESS_ALLOWED_ACE_TYPE, 0x2, 601);
            assertAceAt(pAcl, 1, ACCESS_ALLOWED_ACE_TYPE, 0x3, 602);
        }

        // Deleting the last index leaves the preceding aces untouched.
        {
            BYTE aclBuf[sizeof(ACL) + 3 * aceSize()];
            buildAcl(aclBuf, sizeof(aclBuf), 3, 700, 0x1);
            PACL pAcl = (PACL)aclBuf;
            CPPUNIT_ASSERT(DeleteAce(pAcl, 2));
            CPPUNIT_ASSERT_EQUAL((unsigned)2, (unsigned)pAcl->AceCount);
            assertAceAt(pAcl, 0, ACCESS_ALLOWED_ACE_TYPE, 0x1, 700);
            assertAceAt(pAcl, 1, ACCESS_ALLOWED_ACE_TYPE, 0x2, 701);
        }

        // NULL ACL is rejected.
        CPPUNIT_ASSERT(!DeleteAce(nullptr, 0));

        // Out-of-range index: per the documented Win32 contract, DeleteAce()
        // must fail and leave the ACL unmodified (matching GetAce()'s
        // out-of-range behavior tested above).
        {
            BYTE aclBuf[sizeof(ACL) + aceSize()];
            buildAcl(aclBuf, sizeof(aclBuf), 1, 800, 0x1);
            PACL pAcl = (PACL)aclBuf;
            CPPUNIT_ASSERT(!DeleteAce(pAcl, 5));
            CPPUNIT_ASSERT_EQUAL((unsigned)1, (unsigned)pAcl->AceCount);
            assertAceAt(pAcl, 0, ACCESS_ALLOWED_ACE_TYPE, 0x1, 800);
        }
    }

    // ---- GetSecurityDescriptorLength() ----

    void testGetSecurityDescriptorLength()
    {
        BYTE ownerSid[sizeof(SID)];
        BYTE groupSid[sizeof(SID)];
        buildSid(ownerSid, 900);
        buildSid(groupSid, 901);

        for (unsigned numAces : { 0u, 1u, 3u })
        {
            std::vector<BYTE> aclBuf(sizeof(ACL) + numAces * aceSize());
            buildAcl(aclBuf.data(), aclBuf.size(), numAces, 1000, 0x1);

            SECURITY_DESCRIPTOR sd;
            memset(&sd, 0, sizeof(sd));
            sd.Revision = SECURITY_DESCRIPTOR_REVISION;
            sd.Control = SE_DACL_PRESENT;
            sd.Owner = (PSID)ownerSid;
            sd.Group = (PSID)groupSid;
            sd.Sacl = nullptr;
            sd.Dacl = (PACL)aclBuf.data();

            // Expected length per the documented self-relative SD layout: a
            // 20-byte SECURITY_DESCRIPTOR_RELATIVE header, plus the owner SID,
            // group SID, SACL (absent here) and full (in-use + free) DACL size.
            unsigned expectedLen = sizeof(SECURITY_DESCRIPTOR_RELATIVE) + sidSize() + sidSize() + (unsigned)aclBuf.size();
            unsigned long actualLen = GetSecurityDescriptorLength(&sd);
            CPPUNIT_ASSERT_EQUAL((unsigned long)expectedLen, actualLen);

            // Confirm the computed length is sufficient to actually perform the conversion.
            std::vector<BYTE> selfRelBuf(expectedLen);
            DWORD bufLen = (DWORD)selfRelBuf.size();
            CPPUNIT_ASSERT(MakeSelfRelativeSD(&sd, selfRelBuf.data(), &bufLen));
        }

        // A self-relative descriptor is explicitly unsupported by this
        // reimplementation and reports a length of 0 rather than a real size.
        {
            std::vector<BYTE> aclBuf(sizeof(ACL));
            buildAcl(aclBuf.data(), aclBuf.size(), 0, 0, 0);
            SECURITY_DESCRIPTOR sd;
            memset(&sd, 0, sizeof(sd));
            sd.Revision = SECURITY_DESCRIPTOR_REVISION;
            sd.Control = SE_DACL_PRESENT;
            sd.Owner = (PSID)ownerSid;
            sd.Group = (PSID)groupSid;
            sd.Dacl = (PACL)aclBuf.data();
            unsigned len = sizeof(SECURITY_DESCRIPTOR_RELATIVE) + sidSize() * 2 + (unsigned)aclBuf.size();
            std::vector<BYTE> selfRelBuf(len);
            DWORD bufLen = (DWORD)selfRelBuf.size();
            CPPUNIT_ASSERT(MakeSelfRelativeSD(&sd, selfRelBuf.data(), &bufLen));

            CPPUNIT_ASSERT_EQUAL((unsigned long)0, GetSecurityDescriptorLength(selfRelBuf.data()));
        }
    }

    // ---- MakeSelfRelativeSD() / MakeAbsoluteSD() round-trip ----

    void assertRoundTrip(unsigned numAces)
    {
        BYTE ownerSid[sizeof(SID)];
        BYTE groupSid[sizeof(SID)];
        buildSid(ownerSid, 2000);
        buildSid(groupSid, 2001);

        std::vector<BYTE> aclBuf(sizeof(ACL) + numAces * aceSize());
        buildAcl(aclBuf.data(), aclBuf.size(), numAces, 3000, 0x10);

        SECURITY_DESCRIPTOR absoluteSd;
        memset(&absoluteSd, 0, sizeof(absoluteSd));
        absoluteSd.Revision = SECURITY_DESCRIPTOR_REVISION;
        absoluteSd.Control = SE_DACL_PRESENT;
        absoluteSd.Owner = (PSID)ownerSid;
        absoluteSd.Group = (PSID)groupSid;
        absoluteSd.Sacl = nullptr;
        absoluteSd.Dacl = (PACL)aclBuf.data();

        // Convert to self-relative form.
        unsigned long selfRelLen = GetSecurityDescriptorLength(&absoluteSd);
        std::vector<BYTE> selfRelBuf(selfRelLen);
        DWORD selfRelBufLen = (DWORD)selfRelBuf.size();
        CPPUNIT_ASSERT(MakeSelfRelativeSD(&absoluteSd, selfRelBuf.data(), &selfRelBufLen));

        PISECURITY_DESCRIPTOR_RELATIVE relSd = (PISECURITY_DESCRIPTOR_RELATIVE)selfRelBuf.data();
        CPPUNIT_ASSERT(0 != (relSd->Control & SE_SELF_RELATIVE));

        // ... and back to absolute form.
        SECURITY_DESCRIPTOR roundTrippedSd;
        DWORD sdSize = sizeof(SECURITY_DESCRIPTOR);
        std::vector<BYTE> daclOut(aclBuf.size());
        DWORD daclOutLen = (DWORD)daclOut.size();
        BYTE ownerOut[sizeof(SID)];
        DWORD ownerOutLen = sizeof(ownerOut);
        BYTE groupOut[sizeof(SID)];
        DWORD groupOutLen = sizeof(groupOut);
        DWORD saclOutLen = 0;

        CPPUNIT_ASSERT(MakeAbsoluteSD(selfRelBuf.data(), &roundTrippedSd, &sdSize,
                                       (PACL)daclOut.data(), &daclOutLen,
                                       nullptr, &saclOutLen,
                                       ownerOut, &ownerOutLen,
                                       groupOut, &groupOutLen));

        // Byte-for-byte equality of owner/group SIDs with the originals.
        CPPUNIT_ASSERT_EQUAL(0, memcmp(ownerOut, ownerSid, sidSize()));
        CPPUNIT_ASSERT_EQUAL(0, memcmp(groupOut, groupSid, sidSize()));
        CPPUNIT_ASSERT(!(roundTrippedSd.Control & SE_SELF_RELATIVE));

        // DACL: same ace count, and the same content (mask/type/sid) in the
        // same order, per-field via assertAceAt()...
        PACL pRoundTrippedDacl = (PACL)daclOut.data();
        CPPUNIT_ASSERT_EQUAL((unsigned)numAces, (unsigned)pRoundTrippedDacl->AceCount);
        for (unsigned i = 0; i < numAces; i++)
            assertAceAt(pRoundTrippedDacl, i, ACCESS_ALLOWED_ACE_TYPE, 0x10 + i, 3000 + i);

        // ...and, to substantiate the byte-for-byte round-trip claim (which
        // assertAceAt() alone can't: it only inspects AceType/Mask/first
        // SID sub-authority, so corruption of AceSize, AceFlags, SID
        // Revision/IdentifierAuthority/SubAuthorityCount, or any other ACE
        // byte would otherwise go undetected), compare every in-use byte of
        // the original and round-tripped DACL, including the ACL header
        // itself.
        ACL_SIZE_INFORMATION srcAclSizeInfo;
        CPPUNIT_ASSERT(GetAclInformation((PACL)aclBuf.data(), &srcAclSizeInfo, sizeof(srcAclSizeInfo), AclSizeInformation));
        ACL_SIZE_INFORMATION roundTrippedAclSizeInfo;
        CPPUNIT_ASSERT(GetAclInformation(pRoundTrippedDacl, &roundTrippedAclSizeInfo, sizeof(roundTrippedAclSizeInfo), AclSizeInformation));
        CPPUNIT_ASSERT_EQUAL(srcAclSizeInfo.AclBytesInUse, roundTrippedAclSizeInfo.AclBytesInUse);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(aclBuf.data(), daclOut.data(), srcAclSizeInfo.AclBytesInUse));
    }

    void testMakeSelfRelativeAbsoluteRoundTripNoAces()
    {
        assertRoundTrip(0);
    }

    void testMakeSelfRelativeAbsoluteRoundTripOneAce()
    {
        assertRoundTrip(1);
    }

    void testMakeSelfRelativeAbsoluteRoundTripMultipleAces()
    {
        assertRoundTrip(4);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(PermissionsBinaryTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(PermissionsBinaryTest, "PermissionsBinaryTest");

#endif // _WIN32

#endif // _USE_CPPUNIT
