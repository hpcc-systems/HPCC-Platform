/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

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

#pragma once

#include "jiface.hpp"
#include "jstring.hpp"
#include <map>
#include <ostream>
#include <string>

/**
 * @brief Abstraction for acting upon command line arguments.
 *
 * A command line is assumed to consist of one or more tokens selecting an action to be taken
 * followed by zero or more options and parameters for that action. A command represents one
 * action token. The program name is the first action token and is represented by an interface
 * instance.
 *
 * A command line with one action token, i.e., `app [options] [parameters]`, uses a single isntance
 * to perform the action. A command line with `N` action tokens, i.e., `app t2 ... tN [options]
 * [parameters]`, uses 'N' sequenced instances to perform the action. Each of the first `N-1`
 * instances delegates to the next instance. The last instance performs the action.
 *
 * Given a tool with hypothetical command syntax, showing only the action tokens:
 * - hpcc events transform ...
 * - hpcc events summarize ...
 * - hpcc events model ...
 *
 * The command representations would be:
 * - `CToolCommandGroup` with name `hpcc`, dispatching to:
 *   - `CToolCommandGroup` with name `events`, dispatching to:
 *     - `CAtomicToolCommand`-derived class with name `transform`
 *     - `CAtomicToolCommand`-derived class with name `summarize`
 *     - `CAtomicToolCommand`-derived class with name `model`
 *
 * The 'hpcc' group is included for illustrative purposes. It would only be meaninful if one tool
 * was intended to support disparate sets of commands only related by being part of the HPCC
 * Platform.
 *
 * A tool implementation might look similar to:
 *
 *     int main(int argc, const char* argv[])
 *     {
 *        Owned<IToolCommand> tool = new CToolCommandGroup(argv[0], "HPCC command line tool",
 *           new CToolCommandGroup("events", "Event data commands",
 *              new CTransformEventsCommand("transform", "Convert binary event data into alternate represenations."),
 *              new CSummarizeEventsCommand("summarize", "Summarize event data."),
 *              new CModelEventsCommand("model", "Analyze event data."));
 *        return tool->dispatch(argc, argv);
 *     }
 *
 * Observe that the name of the top level group is set to the program name. This is necessary for
 * the dispatch request to match the group name to how the program is invoked.
 *
 * @see CAtomicToolCommand
 * @see CToolCommandGroup
 */
interface IToolCommand : extends IInterface
{
    /**
     * @brief Receive and process command line arguments.
     *
     * @param argc The number of arguments passed to `main`.
     * @param argv The arguments passed to `main`.
     * @param pos The position of the command's action token in the argument array.
     * @return The program's exit status.
     */
    virtual int dispatch(int argc, const char* argv[], int pos) = 0;

    /**
     * @brief Receive and process command line arguments.
     *
     * @param argc The number of arguments passed to `main`.
     * @param argv The arguments passed to `main`.
     * @return The program's exit status.
     */
    inline int dispatch(int argc, const char* argv[]) { return dispatch(argc, argv, 0); }

    /**
     * @brief Return the action token for this command.
     * @return The action token for this command.
     */
    virtual const char* queryName() const = 0;

    /**
     * @brief Return a brief description of this command.
     * @return A brief description of this command.
     */
    virtual const char* queryAbstract() const = 0;

    /**
     * @brief Set the output stream for all commands.
     *
     * The default output stream is `std::cout`. To faciliate testing, the default can be replaced
     * with a different stream. This method is not thread-safe.
     *
     * @param output The output stream.
     */
    static void setOutputStream(std::ostream& output);

    /**
     * @brief Set the error stream for all commands.
     *
     * The default error stream is `std::cerr`. To faciliate testing, the default can be replaced
     */
    static void setErrorStream(std::ostream& error);

protected:
    static std::ostream* out;
    static std::ostream* err;
};

/**
 * @brief Abstract implementation of `IToolCommand` providing functionality common to all commands.
 */
class CCommonToolCommand : public CInterfaceOf<IToolCommand>
{
public: // overridden methods
    virtual const char* queryName() const override { return name; }
    virtual const char* queryAbstract() const override { return abstract; }

public: // abstract method declarations
    /**
     * @brief Display help text.
     * @param target The target stream.
     * @param argc The number of arguments.
     * @param argv The arguments.
     * @param pos The position of this command in `argv`.
     */
    virtual void usage(std::ostream& target, int argc, const char* argv[], int pos) = 0;

private:
    const char* name = nullptr;
    const char* abstract = nullptr;

public:
    /**
     * @brief Construct a command with a name and a brief description.
     *
     * The name must be empty. The abstract must not be NULL.
     *
     * @param _name The name of the command.
     * @param _abstract A brief description of the command.
     */
    CCommonToolCommand(const char* _name, const char* _abstract) : name(_name), abstract(_abstract)
    {
        assertex(!isEmptyString(name));
        assertex(abstract != nullptr);
    }

    /**
     * @brief Add argv values, separated by space, between possitions 0 and pos to the target
     *        stream.
     * @param target The target stream.
     * @param argc The number of arguments.
     * @param argv The arguments.
     * @param pos The position of the last argument to be added.
     */
    void usagePrefix(std::ostream& target, int argc, const char* argv[], int pos);

    /**
     * @brief Display help text when no more tokens are present.
     *
     * Help text is output to the current error stream.
     *
     * @param argc The number of arguments.
     * @param argv The arguments.
     * @param pos The position of the first argument to check.
     * @return
     */
    bool doImplicitHelp(int argc, const char* argv[], int pos);

    /**
     * @brief Check if help is requested. Display help text if it is.
     *
     * Help text is output to the current output stream.
     *
     * @param argc The number of arguments.
     * @param argv The arguments.
     * @param pos The position of the first argument to check.
     * @return True if help text has been displayed, false otherwise.
     */
    bool doExplicitHelp(int argc, const char* argv[], int pos);
};

/**
 * @bried Concrete extension of `CCommonToolCommand` intended as a base for individual actions.
 */
class CAtomicToolCommand : public CCommonToolCommand
{
public: // overridden methods
    virtual int dispatch(int argc, const char* argv[], int pos) override;
    virtual void usage(std::ostream& target, int argc, const char* argv[], int pos) override;

public: // methods to be overridden
    /**
     * @brief Perform the action of this command.
     * @param argc The number of arguments.
     * @param argv The arguments.
     * @param pos The position of the action token in `argv`.
     */
    virtual int doCommand(int argc, const char* argv[], int pos);

public:
    using CCommonToolCommand::CCommonToolCommand;
};

/**
 * @brief Concrete extension of `CCommonToolCommand` for delegating requests to the next action
 *        token processor.
 */
class CToolCommandGroup : public CCommonToolCommand
{
public:
    virtual int dispatch(int argc, const char* argv[], int pos) override;
    virtual void usage(std::ostream& target, int argc, const char* argv[], int pos) override;
protected:
    using Group = std::map<std::string, Owned<IToolCommand>>;
    Group group;
    size_t maxNameLength = 0;

public:
    /**
     * @brief Construct a command group.
     *
     * A command group is constructed with a variable number of `IToolCommand` instances. The group
     * assumes ownership of each instance reference passed to it.
     *
     * @param name The name of the command group.
     * @param abstract A brief description of the command group.
     * @param commands A parameter pack of group commands.
     */
    template <typename... Commands>
    CToolCommandGroup(const char* name, const char* abstract, Commands... commands) : CCommonToolCommand(name, abstract)
    {
        (addCommand(commands), ...);
    }
private:
    /**
     * @brief Add a command to the group.
     * @param command The non-NULL command to add.
     */
    void addCommand(IToolCommand* command)
    {
        assertex(command != nullptr);
        std::string key(command->queryName());
        if (key.length() > maxNameLength)
            maxNameLength = key.length();
        group[key].setown(command);
    }
};