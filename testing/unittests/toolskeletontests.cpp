/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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
#include <sstream>
#include "jfile.hpp"
#include "jscm.hpp"
#include "unittests.hpp"
#include "espapi.hpp"
#include "jfile.hpp"
#include "toolcommand.hpp"
#include <iostream>
#include <set>

using namespace std;

class ToolSkeletonTests : public CppUnit::TestFixture
{
    /**
     * @brief A test environment for capturing output streams.
     */
    struct Environment
    {
        stringstream out;
        stringstream err;
        Environment()
        {
            IToolCommand::setOutputStream(out);
            IToolCommand::setErrorStream(err);
        }
        ~Environment()
        {
            IToolCommand::setOutputStream(cout);
            IToolCommand::setErrorStream(cerr);
        }
    };

    /**
     * @brief A mock atomic action implementation.
     *
     * Allows differentiation between different test actions.
     */
    class MockAtomic1 : public CAtomicToolCommand
    {
    public:
        using CAtomicToolCommand::CAtomicToolCommand;
    };

    /**
     * @brief A mock atomic action implementation.
     *
     * Allows differentiation between different test actions.
     */
    class MockAtomic2 : public CAtomicToolCommand
    {
    public:
        using CAtomicToolCommand::CAtomicToolCommand;
    };

public:
    CPPUNIT_TEST_SUITE(ToolSkeletonTests);
        CPPUNIT_TEST(testAtomicActionToken);
        CPPUNIT_TEST(testGroupOfNone);
        CPPUNIT_TEST(testGroupOfOne);
        CPPUNIT_TEST(testGroupOfTwo);
        CPPUNIT_TEST(testGroupOfGroup);
    CPPUNIT_TEST_SUITE_END();


protected:
#define CPPUNIT_EXPECT_EMPTY(s) CPPUNIT_ASSERT_EQUAL(string(""), s.str())
#if 1
#define CPPUNIT_EXPECT_EQUAL(cstr, stream) CPPUNIT_ASSERT_EQUAL(string(cstr), stream.str())
#else
#define CPPUNIT_EXPECT_EQUAL(cstr, stream) \
    { \
        const string expect(cstr); \
        const string actual(stream.str()); \
        for (size_t idx = 0; idx < expect.size() && idx < actual.size(); ++idx) \
        { \
            if (expect.at(idx) != actual.at(idx)) \
                cerr << idx << ":" << actual.at(idx) << " != " << expect.at(idx) << endl; \
        } \
        CPPUNIT_ASSERT_EQUAL(expect, actual); \
    }
#endif

    void testAtomicActionToken()
    {
        static const char* expectCmdNotImplemented = R"!!!(cmd : command not implemented

)!!!";
        static const char* expectUnknownCmd = R"!!!(internal error: expected 'cmd' but got 'unknown'

)!!!";
        static const char* expectCmdUsage = R"!!!(abstract

Usage:
    cmd [ options ]

Options:
    -h, -?, --help : Display this help

)!!!";
        Owned<IToolCommand> tool = new CAtomicToolCommand("cmd", "abstract");
        {
            Environment env;
            const char* argv[] = { "cmd" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectCmdNotImplemented, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "unknown" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectUnknownCmd, env.err);
        }
        {
            Environment env;
            const char* argv[] = { "cmd", "-h" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectCmdUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "cmd", "-?" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectCmdUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "cmd", "--help" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectCmdUsage, env.out);
        }
    }

    void testGroupOfNone()
    {
        static const char* expectGroupUsage = R"!!!(abstract

Usage:
    tool [ ( options | command ) ]

Options:
    -h, -?, --help : Display this help

Commands:

)!!!";
        static const char* expectUnknownTool = R"!!!(internal error: expected 'tool' but got 'unknown'

)!!!";
        static const char* expectGroupUnknownCmd = R"!!!(Unknown command 'cmd'

abstract

Usage:
    tool [ ( options | command ) ]

Options:
    -h, -?, --help : Display this help

Commands:

)!!!";
        Owned<IToolCommand> tool = new CToolCommandGroup("tool", "abstract");
        {
            Environment env;
            const char* argv[] = { "tool" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.err);
        }
        {
            Environment env;
            const char* argv[] = { "unknown" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectUnknownTool, env.err);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "-h" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "-?" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "--help" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "cmd" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectGroupUnknownCmd, env.err);
        }
    }

    void testGroupOfOne()
    {
        static const char* expectGroupUsage = R"!!!(abstract

Usage:
    tool [ ( options | command ) ]

Options:
    -h, -?, --help : Display this help

Commands:
    cmd1 : abstract1

)!!!";
        static const char* expectUnknownTool = R"!!!(internal error: expected 'tool' but got 'unknown'

)!!!";
        static const char* expectCmd1NotImplemented = R"!!!(tool cmd1 : command not implemented

)!!!";
        static const char* expectGroupUnknownCmd = R"!!!(Unknown command 'cmd2'

abstract

Usage:
    tool [ ( options | command ) ]

Options:
    -h, -?, --help : Display this help

Commands:
    cmd1 : abstract1

)!!!";
        Owned<IToolCommand> tool = new CToolCommandGroup("tool", "abstract",
            new CAtomicToolCommand("cmd1", "abstract1"));
        {
            Environment env;
            const char* argv[] = { "tool" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.err);
        }
        {
            Environment env;
            const char* argv[] = { "unknown" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectUnknownTool, env.err);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "-h" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "-?" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "--help" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "cmd1" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectCmd1NotImplemented, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "cmd2" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectGroupUnknownCmd, env.err);
        }
    }

    void testGroupOfTwo()
    {
        static const char* expectGroupUsage = R"!!!(abstract

Usage:
    tool [ ( options | command ) ]

Options:
    -h, -?, --help : Display this help

Commands:
    cmd1 : abstract1
    cmd2 : abstract2

)!!!";
        static const char* expectUnknownTool = R"!!!(internal error: expected 'tool' but got 'unknown'

)!!!";
        static const char* expectCmd1NotImplemented = R"!!!(tool cmd1 : command not implemented

)!!!";
        static const char* expectCmd2NotImplemented = R"!!!(tool cmd2 : command not implemented

)!!!";
        static const char* expectGroupUnknownCmd = R"!!!(Unknown command 'cmd3'

abstract

Usage:
    tool [ ( options | command ) ]

Options:
    -h, -?, --help : Display this help

Commands:
    cmd1 : abstract1
    cmd2 : abstract2

)!!!";
        Owned<IToolCommand> tool = new CToolCommandGroup("tool", "abstract",
            new MockAtomic1("cmd1", "abstract1"),
            new MockAtomic2("cmd2", "abstract2"));
        {
            Environment env;
            const char* argv[] = { "tool" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.err);
        }
        {
            Environment env;
            const char* argv[] = { "unknown" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectUnknownTool, env.err);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "-h" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "-?" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "--help" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "cmd1" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectCmd1NotImplemented, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "cmd2" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectCmd2NotImplemented, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "cmd3" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectGroupUnknownCmd, env.err);
        }
    }

    void testGroupOfGroup()
    {
        static const char* expectGroupUsage = R"!!!(abstract

Usage:
    tool [ ( options | command ) ]

Options:
    -h, -?, --help : Display this help

Commands:
    group1 : abstract1

)!!!";
        static const char* expectUnknownTool = R"!!!(internal error: expected 'tool' but got 'unknown'

)!!!";
        static const char* expectGroup1Usage = R"!!!(abstract1

Usage:
    tool group1 [ ( options | command ) ]

Options:
    -h, -?, --help : Display this help

Commands:

)!!!";
        static const char* expectGroupUnknownCmd = R"!!!(Unknown command 'cmd1'

abstract

Usage:
    tool [ ( options | command ) ]

Options:
    -h, -?, --help : Display this help

Commands:
    group1 : abstract1

)!!!";
        Owned<IToolCommand> tool = new CToolCommandGroup("tool", "abstract",
            new CToolCommandGroup("group1", "abstract1"));
        {
            Environment env;
            const char* argv[] = { "tool" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.err);
        }
        {
            Environment env;
            const char* argv[] = { "unknown" };
            CPPUNIT_ASSERT(tool->dispatch(1, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectUnknownTool, env.err);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "-h" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "-?" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "--help" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroupUsage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "group1", "-h" };
            CPPUNIT_ASSERT(tool->dispatch(3, argv) == 0);
            CPPUNIT_EXPECT_EMPTY(env.err);
            CPPUNIT_EXPECT_EQUAL(expectGroup1Usage, env.out);
        }
        {
            Environment env;
            const char* argv[] = { "tool", "cmd1" };
            CPPUNIT_ASSERT(tool->dispatch(2, argv) == 1);
            CPPUNIT_EXPECT_EMPTY(env.out);
            CPPUNIT_EXPECT_EQUAL(expectGroupUnknownCmd, env.err);
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( ToolSkeletonTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( ToolSkeletonTests, "toolskeletontests" );

#endif
