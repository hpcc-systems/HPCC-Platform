/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#ifndef __HQLCACHE_HPP_
#define __HQLCACHE_HPP_

interface IHqlExpression;
/*
 * This interface represents cached information about an ECL definition.  If it is up to date then
 * the stored information can be used to optimize creating an archive, and parsing source code.
 */
interface IEclCachedDefinition : public IInterface
{
public:
    virtual timestamp_type getTimeStamp() const = 0;
    virtual IEclSource * queryOriginal() const = 0;
    virtual bool isUpToDate(hash64_t optionHash) const = 0;
    virtual IFileContents * querySimplifiedEcl() const = 0;
    virtual void queryDependencies(StringArray & values) const = 0;
    virtual bool hasKnownDependents() const = 0;
};

/*
 * This interface is used to locate a cached definition for a scoped reference.  There are at least two
 * implementations - a directory tree and a compound cache file (useful for regression testing)
 */
interface IEclCachedDefinitionCollection : public IInterface
{
    virtual IEclCachedDefinition * getDefinition(const char * path) = 0;
};


/*
 * Create a cached definition collection from a single xml file.  The xml structure matches the structure of the source files.
 *
 * @param repository    The repository that contains the source being compiled.
 * @param root          The root node of the property tree.
 */
/* Create a cached definition collection from a single xml file */
extern HQL_API IEclCachedDefinitionCollection * createEclXmlCachedDefinitionCollection(IEclRepository * repository, IPropertyTree * root);

/*
 * Create a cached definition collection from a directory.  There is one cache file corresponding to each source file.
 *
 * @param repository    The repository that contains the source being compiled.
 * @param root          The root directory containing the hierarchy of cache files.
 */
extern HQL_API IEclCachedDefinitionCollection * createEclFileCachedDefinitionCollection(IEclRepository * repository, const char * root);

/*
 * Convert an ecl path of the form a.b.c.d.e to the path used to find a cache entry - e.g., a/b/c/d/e
 *
 * @param filename      The filename to append the path to.
 * @param eclPath       The path to expand
 */
extern HQL_API void convertSelectsToPath(StringBuffer & filename, const char * eclPath);


/*
 * Attempt to produce a simple expression which can be parsed quickly (because it has few dependencies), and can be
 * used for syntax checking in place of the original expression.
 *
 * @param definition    The expression to simplify.
 * @return              A simplified expression, or nullptr if no simplified value can be created
 */
extern HQL_API IHqlExpression * createSimplifiedDefinition(IHqlExpression * definition);

/*
 * Create an archive directly from the cached information about the attributes (and the source files).
 *
 * @param collection    The cached information about the attributes.
 * @param root          The main cached information about the attributes.
 * @return              The archive.
 * @see updateArchiveFromCache
 */
extern HQL_API IPropertyTree * createArchiveFromCache(IEclCachedDefinitionCollection * collection, const char * root);

/*
 * Update an existing archive directly from the cached information about the attributes (and the source files).
 *
 * @param archive       The archive to cached information about the attributes.
 * @param collection    The cached information about the attributes.
 * @param root          The cached information about the attributes.
 * @see createArchiveFromCache
 */
extern HQL_API void updateArchiveFromCache(IPropertyTree * archive, IEclCachedDefinitionCollection * collection, const char * root);

/*
 * Expand an archive as a collection of files with a directory.  Effectively the inverse of creating an archive.
 *
 * @param path          The root directory to create the file structure in.
 * @param archive       The archive to expand.
 * @param includePlugins Set to true if modules that are defined within a single source file should be expanded.
 */
extern HQL_API void expandArchive(const char * path, IPropertyTree * archive, bool includePlugins);

//Shared functions
void setDefinitionText(IPropertyTree * target, const char * prop, IFileContents * contents, bool checkDirty);

//Call to enable tracing cache access
extern HQL_API void setTraceCache(bool value);


#endif
