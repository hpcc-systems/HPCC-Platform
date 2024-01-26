/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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
#include "esdlcmd_core.hpp"
#include "build-config.h"
#include "esdlcmdutils.hpp"
#include <xpp/XmlPullParser.h>
#include <memory>
#include <unordered_set>
#include <vector>
#include <stack>

// #define _MANIFEST_DEBUG_

#ifdef _MANIFEST_DEBUG_
#include <ostream>
#endif

#define ESDL_MANIFEST_ERR   -1
#define INDENT_SIZE          2

#define MANIFEST_URI                "urn:hpcc:esdl:manifest"

#define MANIFEST_TAG_BINDING         "Binding"
#define MANIFEST_TAG_DEFINITION      "Definition"
#define MANIFEST_TAG_DEFINITIONS     "Definitions"
#define MANIFEST_TAG_BUNDLE          "EsdlBundle"
#define MANIFEST_TAG_INCLUDE         "Include"
#define MANIFEST_TAG_MANIFEST        "Manifest"
#define MANIFEST_TAG_SCRIPTS         "Scripts"
#define MANIFEST_TAG_SVC_BINDING     "ServiceBinding"
#define MANIFEST_TAG_SVC_DEFINITION  "EsdlDefinition"
#define MANIFEST_TAG_BIND_DEFINITION "BindingDefinition"
#define MANIFEST_TAG_TRANSFORM       "Transform"

#define CDATA_START                 "<![CDATA["
#define CDATA_END                   "]]>"
#define ENCODED_CDATA_END           "]]]]><![CDATA[>"

class EsdlManifestCmd : public EsdlCmdCommon
{
    protected:
        // Command-line options
        StringAttr optManifestPath;
        StringAttr optOutputPath;
        StringAttr optOutputType;
        std::vector<std::string> includeSearchPaths;
        // Delimited string needed for ecm->esxdl compilation
        StringBuffer includePathsString;

        // Input
        StringBuffer manifest;
        // Holds serialized output
        StringBuffer output;
        // To compile ecm files to esxdl
        EsdlCmdHelper cmdHelper;
        // Type of output we're generating
        enum class ManifestType { binding, bundle };
        // Track if we are generating binding or bundle
        ManifestType outputType = ManifestType::bundle;
        // Used to trigger warnings and exceptions when parsing
        bool foundEsdlDefn = false;
        bool foundServiceBinding = false;
        // Used to build output EsdlBinding with needed attributes
        StringBuffer service;
        // Tracks indentation for lifetime of parsing
        int indent = 0;
        // Type of previous node found during parsing
        int lastType = XmlPullParser::END_TAG;
        // Gives context to file loader to treat different
        // types of includes correctly
        enum class IncludeType { script, esdl, xslt };

        // Set of attributes output on the Binding element. All
        // others are copied through to Binding/Definition.
        std::set<std::string> bindingAttributes {"created",
                                                "espbinding",
                                                "espprocess",
                                                "id",
                                                "port",
                                                "publishedBy"
                                                };

        // Set of namespace definitions valid in some scope
        using NamespaceFrame = std::unordered_set<std::string>;

        // Stack of namespaces defined with each start node.
        // Because there isn't a 1-1 mapping between parsed
        // manifest nodes and output nodes, there is some special
        // handling regarding when namespaces go in and out of scope
        std::stack<NamespaceFrame> newNamespaces;

        // Namespace definitions that apply to the current scope.
        NamespaceFrame activeNamespaces;

    public:

        EsdlManifestCmd() {}

        bool setOutputType(const char* type)
        {
            if(!isEmptyString(type))
            {
                if(stricmp("binding", type) == 0)
                    outputType = ManifestType::binding;
                else if(stricmp("bundle", type) == 0)
                    outputType = ManifestType::bundle;
                else
                    return false;

                return true;
            }
            return false;
        }

        virtual int processCMD() override
        {
            try
            {
                if (!processManifest())
                {
                    UERRLOG("Error: Failed processsing manifest file. Exiting.");
                    return 1;
                }

                if (!outputResult())
                {
                    UERRLOG("Error: Failed writing output file. Exiting.");
                    return 1;
                }

                PROGLOG("Success");

            } catch (IException* e) {
                StringBuffer msg;
                e->errorMessage(msg);
                UERRLOG("Error running manifest: %s. \nExiting.\n", msg.str());
                e->Release();
                return 1;
            } catch (XmlPullParserException& xppex) {
                UERRLOG("Error running manifest: %s.\nExiting.\n", xppex.what());
                return 1;
            } catch (...) {
                UERRLOG("Unknown error running manifest. Exiting\n");
                return 1;
            }

            #ifdef _MANIFEST_DEBUG_
            printf("Output:\n%s", output.str());
            #endif

            return 0;
        }

        bool parseCommandLineOptions(ArgvIterator &iter) override
        {
            if (iter.done())
            {
                usage();
            }
            else
            {

                //First parameter's order is fixed.
                const char *arg = iter.query();
                if (*arg != '-')
                {
                    optManifestPath.set(arg);
                }
                else
                {
                    UERRLOG("Option detected before required parameters: %s\n", arg);
                    usage();
                    return false;
                }

                for (; !iter.done(); iter.next())
                {
                    StringAttr oneOption;
                    if (iter.matchOption(oneOption, ESDLOPT_INCLUDE_PATH) || iter.matchOption(oneOption, ESDLOPT_INCLUDE_PATH_S))
                        includeSearchPaths.push_back(oneOption.get());

                    iter.matchOption(optOutputPath, ESDLOPT_MANIFEST_OUTFILE);
                    iter.matchOption(optOutputType, ESDLOPT_MANIFEST_FORMAT);
                }
            }
            return true;
        }

        void usage() override
        {
            //   >-------------------------- 80 columns ------------------------------------------<
            puts("Usage:\n");
            puts("esdl manifest <manifest-file> [options]\n");
            puts("Options:");
            puts("    -I | --include-path <path>");
            puts("                        Search path for external files included in the manifest.");
            puts("                        Use once for each path.");
            puts("    --outfile <filename>");
            puts("                        Path and name of the output file");
            puts("    --output-type <format>");
            puts("                        This option overrides both the default output type if");
            puts("                        nothing is specified (bundle) and any explicit type");
            puts("                        provided in the manifest file @outputType attribute. This");
            puts("                        allows you to create one manifest with all the elements");
            puts("                        needed for a bundle, but output only the binding subset.");

            EsdlCmdCommon::usage();
        }

        bool finalizeOptions(IProperties* globals) override
        {
            // TODO: Investigate esdlcmd_shell.cpp because returning false doesn't exit with an error
            if (optManifestPath.isEmpty())
                throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Missing Manifest file path");

            if (!optOutputType.isEmpty())
            {
                if (!setOutputType(optOutputType.get()))
                    throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Unknown value '%s' provided for --output-type option.", optOutputType.get());
            }

            for (size_t i=0; i<includeSearchPaths.size(); i++)
            {
                if (i!=0)
                    includePathsString.append(ENVSEPCHAR);
                includePathsString.append(includeSearchPaths[i].c_str());
            }

            return true;
        }

    private:

        bool locateFile(const char* filePath, std::vector<std::string>& includes, Owned<IFile>& foundFile) const
        {
            foundFile.setown(createIFile(filePath));

            if (foundFile->isFile() == fileBool::foundYes)
                return true;
            if (isAbsolutePath(filePath))
                return false;

            for (std::string& include : includes)
            {
                StringBuffer tryPath(include.c_str());
                addNonEmptyPathSepChar(tryPath);
                tryPath.append(filePath);
                foundFile.setown(createIFile(tryPath));
                if (foundFile->isFile() == fileBool::foundYes)
                    return true;
            }

            foundFile.clear();
            return false;
        }

        int skipNextEndTag(IXmlPullParser* xpp)
        {
            int type;
            do
            {
                type = xpp->next();
            }
            while(type != XmlPullParser::END_TAG && type != XmlPullParser::END_DOCUMENT);

            if (XmlPullParser::END_TAG == type)
            {
                EndTag etag;
                xpp->readEndTag(etag);
            }

            return type;
        }

        // To correctly enclose CDATA sections, any occurrence of the CDATA_END
        // string must be replaced with the ENCODED_CDATA_END string
        void encodeCdataEnd(const char* in, StringBuffer& out)
        {
            const char *str = in;
            int offset = 0;

            while(1)
            {
                const char *cDataEnd = strstr(str, CDATA_END);

                if (cDataEnd != 0)
                {
                    out.append(in, offset, unsigned(cDataEnd - (in + offset)));
                    // encode the found ]]>
                    out.append(ENCODED_CDATA_END);
                    // move past the end of the found ]]>
                    offset = unsigned(cDataEnd - in) + 3;
                    str = cDataEnd + 1;
                    continue;
                }
                else
                {
                    out.append(in + offset);
                    break;
                }
            }
        }

        int processInclude(IXmlPullParser* xpp, StartTag& root, IncludeType includeType)
        {
            const char* filename = nullptr;
            StringBuffer content;
            Owned<IFile> file;

            // Do not copy out the <em:Include ... /> node itself
            filename = root.getValue("file");

            if (nullptr == filename)
                throw makeStringExceptionV(ESDL_MANIFEST_ERR, "File attribute missing from Include statement on line #%d column #%d", xpp->getLineNumber(), xpp->getColumnNumber());

            if (!locateFile(filename, includeSearchPaths, file))
                throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Unable to locate include file %s on line #%d column #%d", filename, xpp->getLineNumber(), xpp->getColumnNumber());

            if (IncludeType::script == includeType)
            {
                StringBuffer rawFile;
                rawFile.loadFile(file);
                // Trim to ensure correct indentation
                rawFile.trim();
                if (!rawFile.length())
                    UWARNLOG("Unable to load <Include>, file='%s' on line #%d column #%d", filename, xpp->getLineNumber(), xpp->getColumnNumber());

                encodeCdataEnd(rawFile.str(), content);
            }
            else if (IncludeType::esdl == includeType)
            {
                cmdHelper.getServiceESXDL(filename, service.str(), content, 0, NULL, (DEPFLAG_INCLUDE_TYPES & ~DEPFLAG_INCLUDE_METHOD), includePathsString.str(), optTraceFlags());

                if (!content.length())
                    throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Unable to load <Include>, file='%s' on line #%d column #%d", filename, xpp->getLineNumber(), xpp->getColumnNumber());
            }
            else if (IncludeType::xslt == includeType)
            {
                StringBuffer rawFile;
                rawFile.loadFile(file);
                // Trim to ensure correct indentation
                rawFile.trim();
                if (!rawFile.length())
                    UWARNLOG("Unable to load <Include>, file='%s' on line #%d column #%d", filename, xpp->getLineNumber(), xpp->getColumnNumber());

                encodeCdataEnd(rawFile.str(), content);
            }
            else
                throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Unknown resource type for <Include>, file='%s' on line #%d column #%d", filename, xpp->getLineNumber(), xpp->getColumnNumber());

            if ('\n' != output.charAt(output.length()-1))
                output.append('\n');

            // Indent each line of the included file by the current indent level as appending
            const char* finger = content.str();
            const char* newline = strchr(content.str(), '\n');
            while (newline != nullptr)
            {
                output.appendN(indent*INDENT_SIZE, ' ').append(newline-finger+1, finger);
                finger = newline + 1;
                newline = strchr(finger, '\n');
            }

            if (!isEmptyString(finger))
                output.appendN(indent*INDENT_SIZE, ' ').append(finger);
            if ('\n' == output.charAt(output.length()-1))
                output.setLength(output.length()-1);

            // Skip the end tag from the <em:Include/> node
            return skipNextEndTag(xpp);
        }

        bool isManifestNodeNamed(StartTag& stag, const char* name)
        {
            if (streq(stag.getLocalName(), name))
            {
                const char* uri = stag.getUri();
                if (uri && streq(uri, MANIFEST_URI))
                    return true;
                else
                    UWARNLOG("Found node named '%s' with namespace '%s', but the namespace '%s' may have been intended.", stag.getLocalName(), uri, MANIFEST_URI);
            }
            return false;
        }

        /**
         * @brief Predicate indicating if a namespace should be tracked and output.
         *
         * Avoid tracking and therefore outputting namespaces we don't want in the result.
         * This includes the manifest ns, the empty default ns and the XML ns.
         *
         * @param uri uri of the namespace in question
         * @returns true if it should be tracked and false otherwise
        */
        bool shouldTrackNamespace(const char* uri)
        {
            if (isEmptyString(uri))
                return false;
            if (strncmp(MANIFEST_URI, uri, strlen(MANIFEST_URI)) == 0 )
                return false;
            if (strncmp("http://www.w3.org/XML/1998/namespace", uri, strlen("http://www.w3.org/XML/1998/namespace")) == 0 )
                return false;
            return true;
        }

        inline void appendXmlns(std::string& ns, const std::map<std::string, const SXT_CHAR*>::const_iterator it)
        {
            ns = ns + "xmlns" + (it->first.empty() ? "" : (std::string(":") + it->first)) + "=\"" + it->second + "\"";
        }

        /**
         * @brief Track namespaces newly defined in the start tag
         *
         * Push a frame onto the stack containing any namespaces newly
         * defined in this start tag. This includes any defaults seen
         * used for the first time. Maintain an additional frame of all
         * namespaces currently active.
         *
         * Constraints of the parser mean that we don't get an explicit
         * notice of a namespace definition, only a list of namespaces
         * 'in use' or 'in scope' for the current element. We deduce that
         * an element defines the namespace when it is seen in use for
         * the first time in the current scope.
         *
         * This means that the output may not be lexically identical to
         * the input, but it will be semantically correct. Two places
         * you'll see this difference are in duplicate and default
         * namespace definitions:
         *
         * Child elements that duplicate an ancestor's definition won't
         * won't be output with that duplicate xmlns attribute. Default
         * ns may not be output on the same element it was defined on in
         * input. Instead it will be redefined for each direct child of
         * the element that defined it in the input.
         *
         * @param xpp the xml pull parser
         * @param stag the current start tag
        */
        void pushNamespaces(IXmlPullParser* xpp, StartTag* stag)
        {
            NamespaceFrame newThisNode;
            std::map<std::string, const SXT_CHAR*>::const_iterator it = xpp->getNsBegin();
            while (it != xpp->getNsEnd())
            {
                if (shouldTrackNamespace(it->second) )
                {
                    std::string nsDefn;
                    appendXmlns(nsDefn, it);
                    if (activeNamespaces.insert(nsDefn).second)
                        newThisNode.insert(nsDefn);
                }
                it++;
            }
            if (stag)
            {
                const char* uri = stag->getUri();
                const char* qname = stag->getQName();
                // If the qname has no colon (no prefix) its using a default namespace
                if (!strchr(qname, ':') && shouldTrackNamespace(uri) )
                {
                    std::string defaultNs = std::string("xmlns=\"") + uri + "\"";
                    if (activeNamespaces.insert(defaultNs).second)
                        newThisNode.insert(defaultNs);
                }
            }
            #ifdef _MANIFEST_DEBUG_
            if(stag)
                std::cout << "push(" << stag->getQName() << ")\n";
            else
                std::cout << "push()\n";

            std::cout << "  active:\n";
            for (auto ns : activeNamespaces)
                std::cout << "    " << ns << "\n";
            std::cout << "  new:\n";
            for (auto ns : newThisNode)
                std::cout << "    " << ns << "\n";
            #endif
            newNamespaces.push(newThisNode);
        }

        /**
         * @brief Pop the top namespace frame off the stack as it is going out of scope and
         *  remove those namespaces from the active namespaces.
        */
        inline void popNamespaces()
        {
            NamespaceFrame newNsForPairedStartTag = newNamespaces.top();
            for (const std::string& ns : newNsForPairedStartTag)
                activeNamespaces.erase(ns);

            newNamespaces.pop();
        }

        /**
         * @brief Append to the output namespaces newly defined- on the top of the stack.
        */
        inline void appendNewNamespaces()
        {
            NamespaceFrame newThisNode = newNamespaces.top();
            for (const std::string& nsDefn : newThisNode)
                output.appendf(" %s", nsDefn.c_str());
        }

        /**
         * @brief Append StartTag to output, tracking namespaces.
         *
         * Use the original tag name, output namespace definitions and attributes.
        */
        inline void appendStartTag(IXmlPullParser* xpp, StartTag& stag)
        {
            appendStartTag(xpp, stag, true, true);
        }

        /**
         * @brief Append StartTag to output, tracking namespaces.
         *
         * Use the supplied name, output namespace definitions and attributes.
         *
         * @param xpp Xml pull parser
         * @param stag StartTag to output
         * @param newName tag name to use on output
        */
        inline void appendStartTagWithName(IXmlPullParser* xpp, StartTag& stag, const char* newName)
        {
            appendStartTag(xpp, stag, false, true, newName);
        }

        /**
         * @brief Append StartTag to output, tracking namespaces.
         *
         * Output the local or qname, choose if attributes are output. Namespace definitions
         * are always output.
         *
         * @param xpp Xml pull parser
         * @param stag StartTag to output
         * @param useQname flag to indicate if qname should be output, otherwise local name
         * @param withAttributes flag to indicate if attributes should be output
        */
        inline void appendStartTag(IXmlPullParser* xpp, StartTag& stag, bool useQname, bool withAttributes)
        {
            appendStartTag(xpp, stag, useQname, withAttributes, nullptr);
        }

        /**
         * @brief Append StartTag to output, tracking namespaces.
         *
         * Output the local name, qname or supplied new name. Choose if attributes are output.
         * Namespace definitions are always output.
         *
         * @param xpp Xml pull parser
         * @param stag StartTag to output
         * @param useQname flag to indicate if qname should be output, otherwise local name
         * @param withAttributes flag to indicate if attributes should be output
        */
        inline void appendStartTag(IXmlPullParser* xpp, StartTag& stag, bool useQname, bool withAttributes, const char* newName)
        {
            if( output.length() > 0 )
                output.append('\n').appendN(indent*INDENT_SIZE, ' ');
            if( !isEmptyString(newName))
                output.appendf("<%s", newName);
            else
            {
                if (useQname)
                    output.appendf("<%s", stag.getQName());
                else
                    output.appendf("<%s", stag.getLocalName());
            }

            if (withAttributes)
                for (int i=0; ; i++)
                {
                    const char* attr = stag.getRawName(i);
                    if (!attr)
                        break;
                    appendXMLAttr(output, attr, stag.getValue(i), nullptr, true);
                }

            pushNamespaces(xpp, &stag);
            appendNewNamespaces();
            output.append('>');
            lastType = XmlPullParser::START_TAG;
            indent++;
        }

        /**
         * @brief Append EndTag to output, tracking namespaces
         *
         * @param etag EndTag to append
        */
        inline void appendEndTag(EndTag& etag)
        {
            appendEndTag(etag, true);
        }

        /**
         * @brief Append EndTag to output, tracking namespaces
         *
         * Parameter indicates if the qname or localname is used.
         *
         * @param etag EndTag to append
         * @param useQname if true append qname, otherwise localname
        */
        inline void appendEndTag(EndTag& etag, bool useQname)
        {
            --indent;
            // If we have a StartTag with no children or content then close it out like <this/>
            if (XmlPullParser::START_TAG == lastType)
                output.insert(output.length()-1,'/');
            else
            {
                // If the last thing output was a Tag, then place the EndTag on a new line and indent.
                // Otherwise close the node on the same line so we aren't adding extra lines for small
                // content. This simple heuristic should work for most basic cases.
                if (output.charAt(output.length()-1) == '>')
                    output.append('\n').appendN(indent*INDENT_SIZE, ' ');

                if (useQname)
                    appendXMLCloseTag(output, etag.getQName());
                else
                    appendXMLCloseTag(output, etag.getLocalName());
            }
            popNamespaces();
        }

        /**
         * @brief Append start tag to output using supplied text name, not tracking namespaces.
         *
         * Used when a synthesized tag is needed, not one copied from the manifest.
         *
         * @param text tag name
        */
        inline void appendCompleteStartTag(const char* text)
        {
            appendStartTag(text, true);
        }

        /**
         * @brief Append start tag to output using supplied text name, not tracking namespaces.
         *
         * Used when a synthesized tag is needed, not one copied from the manifest.
         *
         * @param text tag name
         * @param complete when true the tag is closed, otherwise it is left open
        */
        inline void appendStartTag(const char* text, bool complete)
        {
            if (output.length()>0)
                output.append('\n');
            output.appendN(indent*INDENT_SIZE, ' ');
            appendXMLOpenTag(output, text, nullptr, complete);
            indent++;
        }

        /**
         * @brief Append end tag to output using supplied text name, not tracking namespaces.
         *
         * Used when a synthesized tag is needed, not one copied from the manifest.
         *
         * @param text tag name
        */
        inline void appendEndTag(const char* text)
        {
            --indent;
            // If the last thing output was a Tag, then place the EndTag on a new line and indent.
            // Otherwise close the node on the same line so we aren't adding extra lines for small
            // content. This simple heuristic should work for most basic cases.
            if (output.charAt(output.length()-1) == '>')
                output.append('\n').appendN(indent*INDENT_SIZE, ' ');
            appendXMLCloseTag(output, text);
        }

        /**
         * @brief Append CDATA start markup.
        */
        inline void appendStartCDATA()
        {
            if (output.length()>0)
                output.append('\n');
            output.appendN(indent*INDENT_SIZE, ' ').append("<![CDATA[");
            indent++;
        }

        /**
         * @brief Append CDATA end markup.
        */
        inline void appendEndCDATA()
        {
            --indent;
            // If the last thing output was a Tag, then place the EndTag on a new line and indent.
            // Otherwise close the node on the same line so we aren't adding extra lines for small
            // content. This simple heuristic should work for most basic cases.
            if (output.charAt(output.length()-1) == '>')
                output.append('\n').appendN(indent*INDENT_SIZE, ' ');
            output.append("]]>");
        }

        /**
         * @brief Append xml-encoded non-whitespace content.
         *
         * @param xpp Xml pull parser
        */
        inline void appendNonWhitespaceContent(IXmlPullParser* xpp)
        {
            if (!xpp->whitespaceContent())
            {
                StringBuffer content(xpp->readContent());
                StringBuffer encodedContent;
                encodeXML(content.str(), encodedContent);
                output.append(encodedContent.str());
            }
        }

        /**
         * @brief Process the 'Scripts' node and children.
         *
         * The 'Scripts' node contents are enclosed with CDATA and another 'Scripts' node.
         * Any namespaces defined in the root 'Scripts' node of an included file are
         * copied to the CDATA-enclosed 'Scripts' node of output.
         *
         * @param xpp pointer to the pull parser
         * @param root reference to the 'Scripts' node read
         * @return int type of the last node read
         */
        int processScripts(IXmlPullParser* xpp, StartTag& root)
        {
            int type;
            StartTag stag;
            EndTag etag;
            // Track the node level nesting of nodes handled in this function only
            int level = 1;

            // To preserve order, the expected format for scripts is:
            // <Scripts>
            //     <![CDATA[<Scripts>...actual scripts...</Scripts>]]>
            // </Scripts>

            appendCompleteStartTag(MANIFEST_TAG_SCRIPTS);
            appendStartCDATA();
            // Any namespaces defined on the em:Scripts element are output in the
            // CDATA-wrapped Scripts element here
            appendStartTagWithName(xpp, root, MANIFEST_TAG_SCRIPTS);

            do
            {
                type = xpp->next();
                switch (type)
                {
                    case XmlPullParser::START_TAG:
                        xpp->readStartTag(stag);
                        if (isManifestNodeNamed(stag, MANIFEST_TAG_INCLUDE))
                            processInclude(xpp, stag, IncludeType::script);
                        else
                        {
                            appendStartTag(xpp, stag);
                            level++;
                        }
                        break;

                    case XmlPullParser::CONTENT:
                        appendNonWhitespaceContent(xpp);
                        break;

                    case XmlPullParser::END_TAG:
                        xpp->readEndTag(etag);

                        if (isRootEndTagNamed(etag, MANIFEST_TAG_SCRIPTS, level))
                        {
                            appendEndTag(MANIFEST_TAG_SCRIPTS);
                            appendEndCDATA();
                            appendEndTag(MANIFEST_TAG_SCRIPTS);
                            popNamespaces();
                        }
                        else
                            appendEndTag(etag);

                        level--;
                        break;
                }
                lastType = type;
            }
            while (level > 0);

            return type;
        }

        /**
         * @brief Process the 'Transform' node and children.
         *
         * The 'Transform' node contents are enclosed with CDATA.
         *
         * @param xpp pointer to the pull parser
         * @param root reference to the 'Transform' node read
         * @return int type of the last node read
         */
        int processTransform(IXmlPullParser* xpp, StartTag& root)
        {
            int type;
            StartTag stag;
            EndTag etag;
            // Track the node level nesting of nodes handled in this function only
            int level = 1;

            // To preserve order, the expected format for transforms is:
            // <Transform>
            //     <![CDATA[...XSLT...]]>
            // </Transform>
            appendStartTag(xpp, root, false, true);
            appendStartCDATA();

            do
            {
                type = xpp->next();
                switch (type)
                {
                    case XmlPullParser::START_TAG:
                        xpp->readStartTag(stag);
                        if (isManifestNodeNamed(stag, MANIFEST_TAG_INCLUDE))
                            processInclude(xpp, stag, IncludeType::xslt);
                        else
                        {
                            appendStartTag(xpp, stag);
                            level++;
                        }
                        break;

                    case XmlPullParser::CONTENT:
                        appendNonWhitespaceContent(xpp);
                        break;

                    case XmlPullParser::END_TAG:
                        xpp->readEndTag(etag);

                        // We're about to encounter the root <Transform> node, so close
                        // out the CDATA section we injected first
                        if (isRootEndTagNamed(etag, "Transform", level))
                        {
                            appendEndCDATA();
                            appendEndTag(etag, false);
                        }
                        else
                            appendEndTag(etag);

                        level--;
                        break;
                }
                lastType = type;
            }
            while (level > 0);

            return type;
        }

        /**
         * @brief Process the 'EsdlDefinition' node and children.
         *
         * The 'EsdlDefinition' node is renamed on output to 'Definitions'. Its contents are enclosed
         * with CDATA.
         *
         * @param xpp pointer to the pull parser
         * @param root reference to the 'EsdlDefinition' node read
         * @return int type of the last node read
         */
        int processEsdlDefinition(IXmlPullParser* xpp, StartTag& root)
        {
            int type;
            StartTag stag;
            EndTag etag;
            int level = 1;

            if (foundEsdlDefn)
                throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Only one EsdlDefinition per manifest is supported. Found a second at line #%d, column #%d", xpp->getLineNumber(), xpp->getColumnNumber());
            foundEsdlDefn = true;

            if (outputType != ManifestType::bundle)
            {
                UWARNLOG("The requested binding output will not include the ESDL Definition found in the Manifest.");
                lastType = xpp->skipSubTree();
                return lastType;
            }

            // Eat the root start/end tag and output in the expected bundle format
            // Push root tag namespaces since its getting eaten here
            pushNamespaces(xpp, &root);
            appendCompleteStartTag(MANIFEST_TAG_DEFINITIONS);
            appendStartCDATA();
            lastType = XmlPullParser::START_TAG;

            do
            {
                type = xpp->next();
                switch (type)
                {
                    case XmlPullParser::START_TAG:
                    {
                        xpp->readStartTag(stag);
                        if (isManifestNodeNamed(stag, MANIFEST_TAG_INCLUDE))
                            processInclude(xpp, stag, IncludeType::esdl);
                        else
                        {
                            appendStartTag(xpp, stag);
                            level++;
                        }
                        break;
                    }

                    case XmlPullParser::CONTENT:
                        appendNonWhitespaceContent(xpp);
                        break;

                    case XmlPullParser::END_TAG:
                        xpp->readEndTag(etag);
                        if (!isRootEndTagNamed(etag, MANIFEST_TAG_SVC_DEFINITION, level))
                            appendEndTag(etag);
                        level--;
                        break;
                }
                lastType = type;
            }
            while (level > 0);

            appendEndCDATA();
            appendEndTag(MANIFEST_TAG_DEFINITIONS);
            popNamespaces();
            return type;
        }

        /**
         * @brief Determine if an end tag is root level and matches the given name.
         *
         * Some nodes in the manifest input are renamed on output or skipped altogether.
         * The root start tag is always the stag passed in to each process function.
         * This predicate is used to determine if the candidate end tag is the root level
         * end tag matching the start tag.
         *
         * @param etag candidate end tag to possibly skip
         * @param match local name of the end tag to skip
         * @param level current level of parsing
         * @return true if the candidate end tag should be skipped
        */
        inline bool isRootEndTagNamed(EndTag& etag, const char* match, int level)
        {
            return (1 == level && streq(etag.getLocalName(), match));
        }

        /**
         * @brief Append the 'Binding' tag with namespaces and any allowed attributes provided.
         *
         * Append only those attributes that are assigned to be part of the Binding node. If
         * @id isn't provided, then auto-generate one.
         *
         * @param xpp pointer to the pull parser
         * @param bindTag reference to the 'ServiceBinding' node read
         */
        void appendBindingTag(IXmlPullParser* xpp, StartTag& bindTag)
        {
            appendStartTag(MANIFEST_TAG_BINDING, false);
            for (int i=0; ; i++)
            {
                const char* attr = bindTag.getRawName(i);
                if (!attr)
                    break;
                auto search = bindingAttributes.find(attr);
                if (search != bindingAttributes.end())
                    appendXMLAttr(output, attr, bindTag.getValue(i), nullptr, true);
            }

            StringBuffer idAttr(bindTag.getValue("id"));
            if( isEmptyString(idAttr.str()))
            {
                VStringBuffer generatedId("%s_desdl_binding", service.str());
                appendXMLAttr(output, "id", generatedId.str(), nullptr, true);
            }

            pushNamespaces(xpp, &bindTag);
            appendNewNamespaces();

            output.append(">");
        }

        /**
         * @brief Append the 'Definition' tag with any allowed attributes provided.
         *
         * Append only those attributes defined on the 'ServiceBinding' that aren't
         * assigned to be part of the Binding node. If @id isn't provided, then
         * auto-generate one if we aren't outputting a binding. This is because for
         * a binding, the Definition/@id must match the ESDLDefinitionId provided
         * when running 'esdl publish-service' and that is likely to vary widely.
         *
         * @param xpp pointer to the pull parser
         * @param bindTag reference to the 'ServiceBinding' node read
         */
        void appendDefinitionTag(IXmlPullParser* xpp, StartTag& bindTag)
        {
            appendStartTag(MANIFEST_TAG_DEFINITION, false);
            for (int i=0; ; i++)
            {
                const char* attr = bindTag.getRawName(i);
                if (!attr)
                    break;
                auto search = bindingAttributes.find(attr);
                if (search == bindingAttributes.end())
                    appendXMLAttr(output, attr, bindTag.getValue(i), nullptr, true);
            }

            if (ManifestType::bundle == outputType)
            {
                VStringBuffer generatedId("%s.1", service.str());
                appendXMLAttr(output, "id", generatedId.str(), nullptr, true);
            }

            output.append(">");
        }

        /**
         * @brief Process the 'ServiceBinding' node and children found in the manifest.
         *
         * The 'ServiceBinding' node and its attributes are copied to output with the name 'Binding'.
         * If 'ServiceBinding' does not contain a 'BindingDefinition' node as its first child, then
         * output a 'Definition' child including the required 'esdlservice' and 'id' attributes
         * populated with default values. Process remaining child nodes.
         *
         * @param xpp pointer to the pull parser
         * @param root reference to the 'ServiceBinding' node read
         * @return int type of the last node read
         */
        int processServiceBinding(IXmlPullParser* xpp, StartTag& root)
        {
            int type;
            StartTag stag;
            EndTag etag;
            // track the node level nesting of nodes handled in this function only
            int level = 1;

            if (foundServiceBinding)
                throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Only one ServiceBinding per manifest is supported. Found a second at line #%d, column #%d", xpp->getLineNumber(), xpp->getColumnNumber());

            foundServiceBinding = true;
            service = root.getValue("esdlservice");

            if (isEmptyString(service.str()))
                throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Missing required attribute ServiceBinding/@esdlservice at line #%d, column #%d", xpp->getLineNumber(), xpp->getColumnNumber());

            appendBindingTag(xpp, root);
            appendDefinitionTag(xpp, root);

            do
            {
                type = xpp->next();

                switch (type)
                {
                    case XmlPullParser::START_TAG:
                    {
                        xpp->readStartTag(stag);
                        if (isManifestNodeNamed(stag, MANIFEST_TAG_SCRIPTS))
                            type = processScripts(xpp, stag);
                        else if (isManifestNodeNamed(stag, MANIFEST_TAG_TRANSFORM))
                            type = processTransform(xpp, stag);
                        else
                        {
                            appendStartTag(xpp, stag);
                            level++;
                        }

                        break;
                    }

                    case XmlPullParser::CONTENT:
                        appendNonWhitespaceContent(xpp);
                        break;

                    case XmlPullParser::END_TAG:
                        xpp->readEndTag(etag);
                        if (!isRootEndTagNamed(etag, MANIFEST_TAG_SVC_BINDING, level))
                            appendEndTag(etag);
                        level--;
                        break;
                }

                lastType = type;
            }
            while (level > 0);

            appendEndTag(MANIFEST_TAG_DEFINITION);
            appendEndTag(MANIFEST_TAG_BINDING);
            // to match appendBindingTag(xpp, root);
            popNamespaces();
            return type;
        }

#ifdef _MANIFEST_DEBUG_
        void printNs(IXmlPullParser* xpp, const char* msg = nullptr)
        {
            printf("%s\n", msg ? msg : "namespaces:");
            std::map<std::string, const SXT_CHAR*>::const_iterator it = xpp->getNsBegin();
            while (it != xpp->getNsEnd())
            {
                std::string nsDefn;
                appendXmlns(nsDefn, it);
                printf(">%s|%s<\n", it->first.c_str(), it->second);
                it++;
            }
        }
#else
        void printNs(IXmlPullParser*xpp, const char* msg = nullptr){}
#endif

        /**
         * @brief Process the manifest file beginning at the root.
         *
         * @return bool false if there were problems reading or parsing the file
         */
        bool processManifest()
        {
            bool result = true;
            try
            {
                manifest.loadFile(optManifestPath.str());
                int size = manifest.length();

                if (size>0)
                {
                    std::unique_ptr<XmlPullParser> xpp(new XmlPullParser());
                    xpp->setSupportNamespaces(true);
                    xpp->setInput(manifest.str(), size);

                    int type;
                    StartTag stag;
                    EndTag etag;
                    // track the node level nesting of nodes handled in this function only
                    int level = 1;

                    do { type = xpp->next(); }
                    while (type != XmlPullParser::END_DOCUMENT && type != XmlPullParser::START_TAG);

                    if (XmlPullParser::END_DOCUMENT == type)
                        throw makeStringException(ESDL_MANIFEST_ERR, "Manifest file missing root <Manifest> tag");

                    // Eat the start/end tags for <em:Manifest>
                    lastType = type;
                    xpp->readStartTag(stag);
                    #ifdef _MANIFEST_DEBUG_
                    printNs(xpp.get(), "after readStartTag");
                    #endif

                    const char* localName = stag.getLocalName();
                    const char* uri = stag.getUri();

                    if (isEmptyString(localName) || !streq(localName, MANIFEST_TAG_MANIFEST))
                        throw makeStringException(ESDL_MANIFEST_ERR, "Manifest file root tag must be <Manifest>");

                    if (isEmptyString(uri))
                        throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Manifest file not using expected namespace '%s'", MANIFEST_URI);

                    if (!streq(uri, MANIFEST_URI))
                        throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Manifest file using incorrect namespace URI '%s', must be '%s'", uri, MANIFEST_URI);

                    // Command line --output-type overrides manifest attribute
                    if (!isEmptyString(optOutputType.get()))
                        setOutputType(optOutputType.get());
                    else
                    {
                        const char* bundleAttr = stag.getValue("outputType");
                        if (bundleAttr)
                        {
                            if (!setOutputType(bundleAttr))
                                throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Unknown value '%s' provided for attribute Manifest/@outputType", bundleAttr);
                        }
                    }
                    if (ManifestType::bundle == outputType)
                        appendCompleteStartTag(MANIFEST_TAG_BUNDLE);

                    do
                    {
                        type=xpp->next();
                        switch (type)
                        {
                            case XmlPullParser::START_TAG:
                            {
                                xpp->readStartTag(stag);
                                if (isManifestNodeNamed(stag, MANIFEST_TAG_SVC_BINDING))
                                    type = processServiceBinding(xpp.get(), stag);
                                else if (isManifestNodeNamed(stag, MANIFEST_TAG_SVC_DEFINITION))
                                    type = processEsdlDefinition(xpp.get(), stag);
                                else
                                {
                                    appendStartTag(xpp.get(), stag);
                                    level++;
                                }
                                break;
                            }

                            case XmlPullParser::CONTENT:
                                appendNonWhitespaceContent(xpp.get());
                                break;

                            case XmlPullParser::END_TAG:
                                xpp->readEndTag(etag);
                                if(!isRootEndTagNamed(etag, MANIFEST_TAG_MANIFEST, level))
                                    appendEndTag(etag);
                                level--;
                                break;

                            case XmlPullParser::END_DOCUMENT:
                                level = 0;
                                break;
                        }

                        lastType = type;

                        #ifdef _MANIFEST_DEBUG_
                        printf("processBinding while:(...%s)", output.length()<100 ? output.str() : output.str()+output.length()-100);
                        #endif
                    }
                    while (level > 0);

                    if (ManifestType::bundle == outputType)
                        appendEndTag(MANIFEST_TAG_BUNDLE);
                    if (ManifestType::bundle == outputType && !foundEsdlDefn)
                        throw makeStringExceptionV(ESDL_MANIFEST_ERR, "Generating EsdlBundle output without ESDL Definition is not supported.");

                    #ifdef _MANIFEST_DEBUG_
                    printf("processBinding final:(%s)", output.str());
                    #endif
                }
                else
                {
                    UERRLOG("Error reading manifest file (%s): empty or does not exist.", optManifestPath.str());
                    result = false;
                }
            }
            catch (IException* e)
            {
                StringBuffer msg;
                e->errorMessage(msg);
                UERRLOG("Error reading manifest file (%s): %s", optManifestPath.str(), msg.str());
                result = false;
                e->Release();
            }
            catch (XmlPullParserException& xe)
            {
                const char* msg = xe.what();
                UERRLOG("Error reading manifest file (%s) at line #%d, column #%d: %s", optManifestPath.str(), xe.getLineNumber(), xe.getColumnNumber(), msg ? msg : "Unknown");
                result = false;
            }
            catch (...)
            {
                UERRLOG("Unknown error reading manifest file (%s)", optManifestPath.str());
                result = false;
            }

            return result;
        }

        bool outputResult()
        {
            bool result = true;
            try {
                if (optOutputPath.isEmpty())
                {
                    printf("%s", output.str());
                }
                else
                {
                    Owned<IFile> bindingFile = createIFile(optOutputPath.str());
                    if (bindingFile)
                    {
                        Owned<IFileIO> bindingFileIO = bindingFile->open(IFOcreate);
                        if (bindingFileIO)
                        {
                            bindingFileIO->write(0, output.length(), output.str());
                        } else {
                            UERRLOG("Error writing to output file (%s).", optOutputPath.str());
                            result = false;
                        }
                    } else {
                        UERRLOG("Error creating output file (%s).", optOutputPath.str());
                        result = false;
                    }
                }
            } catch (IException* e) {
                StringBuffer msg;
                e->errorMessage(msg);
                UERRLOG("Error writing output file (%s): %s", optOutputPath.str(), msg.str());
                result = false;
                e->Release();
            } catch (...) {
                UERRLOG("Unknown error writing output file (%s)", optOutputPath.str());
                result = false;
            }
            return result;
        }
};
