/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include <stdio.h>
#include "jlog.hpp"
#include "jfile.hpp"
#include "jargv.hpp"
#include "jregexp.hpp"

#include "rmtfile.hpp"

#include "build-config.h"

#include "eclcmd.hpp"
#include "eclcmd_common.hpp"
#include "eclcmd_core.hpp"

#define ECLCC_ECLBUNDLE_PATH "ECLCC_ECLBUNDLE_PATH"
#define HPCC_FILEHOOKS_PATH "HPCC_FILEHOOKS_PATH"
#define VERSION_SUBDIR "_versions"

#define ECLOPT_DETAILS "--details"
#define ECLOPT_DRYRUN "--dryrun"
#define ECLOPT_FORCE "--force"
#define ECLOPT_KEEPPRIOR "--keepprior"
#define ECLOPT_RECURSE "--recurse"
#define ECLOPT_UPDATE "--update"


static bool optVerbose;


/*
 * Version comparison code.
 *
 * Version strings are of the form X.Y.Z
 *
 * Each component X, Y, Z etc should be numeric, with optional trailing alphanumeric.
 * Comparison is done numerically on the leading digits, and alphabetically on any remainder
 *
 * When comparing versions, 1.5.2 is considered to match 1.5 (but not vice versa), unless strict is specified
 */

static int versionCompareSingleComponent(const char *v1, const char *v2)
{
    char *ev1;
    char *ev2;
    long l1 = strtoul(v1, &ev1, 10);
    long l2 = strtoul(v2, &ev2, 10);
    if (l1 != l2)
        return l1 < l2 ? -1 : 1;
    return stricmp(ev1, ev2);
}

static int versionCompare(const char *v1, const char *v2, bool strict)
{
    StringArray a1, a2;
    a1.appendList(v1, ".");
    a2.appendList(v2, ".");
    int i = 0;
    loop
    {
        if (a1.isItem(i) && a2.isItem(i))
        {
            int diff = versionCompareSingleComponent(a1.item(i), a2.item(i));
            if (diff)
                return diff;
            i++;
        }
        else if (a2.isItem(i))  // second is longer, so first is < second
            return -1;
        else if (strict && a1.isItem(i))  // Note: trailing info in v1 is ignored if not strict
            return 1;
        else
            return 0;
    }
}

static bool versionOk(const char *versionPresent, const char *minOk, const char *maxOk = NULL)
{
    if (minOk && versionCompare(versionPresent, minOk, false) < 0)
        return false;
    if (maxOk && versionCompare(versionPresent, maxOk, false) > 0)
        return false;
    return true;
}

//--------------------------------------------------------------------------------------------------------------

unsigned doPipeCommand(StringBuffer &output, const char *cmd, const char *args, const char *input)
{
    VStringBuffer runcmd("%s %s", cmd, args);
    if (optVerbose)
    {
        printf("Running %s\n", runcmd.str());
        if (input)
            printf("with input %s\n", input);
    }
    unsigned ret = runExternalCommand(output, runcmd, input);
    if (optVerbose && (ret > 0))
        printf("%s return code was %d", cmd, ret);
    return ret;
}

static bool platformVersionDone = false;
static StringBuffer platformVersion;

static const char *queryPlatformVersion()
{
    if (!platformVersionDone)
    {
        StringBuffer output;
        doPipeCommand(output, "eclcc", "--nologfile --version", NULL);
        RegExpr re("_[0-9]+[.][0-9]+[.][0-9]+");
        const char *found = re.find(output);
        if (!found)
            throw MakeStringException(0, "Unexpected response from eclcc\n");
        platformVersion.append(re.findlen()-1, found+1);  // Skip the leading _
    }
    return platformVersion.str();
}

static void extractValueFromEnvOutput(StringBuffer &path, const char *specs, const char *name)
{
    StringBuffer search(name);
    search.append('=');
    const char *found = strstr(specs, search);
    if (found)
    {
        found += search.length();
        const char *eol = strchr(found, '\n');
        if (eol)
        {
            while (eol >= found)
            {
                if (!isspace(*eol))
                    break;
                eol--;
            }
            path.append(eol - found + 1, found);
        }
        else
            path.append(found);
    }
    else
    {
        found = getenv(name);
        if (!found)
            throw MakeStringException(0, "%s could not be located", name);
        path.append(found);
    }
}

bool directoryContainsBundleFile(IFile *dir)
{
    Owned<IDirectoryIterator> files = dir->directoryFiles(NULL, false, false);
    ForEach(*files)
    {
        IFile *thisFile = &files->query();
        StringBuffer fileName;
        splitFilename(thisFile->queryFilename(), NULL, NULL, &fileName, &fileName);
        if (stricmp(fileName.str(), "bundle.ecl")==0)
            return true;
    }
    return false;
}

void recursiveRemoveDirectory(IFile *dir, bool isDryRun)
{
    Owned<IDirectoryIterator> files = dir->directoryFiles(NULL, false, true);
    ForEach(*files)
    {
        IFile *thisFile = &files->query();
        if (thisFile->isDirectory()==foundYes)
            recursiveRemoveDirectory(thisFile, isDryRun);
        if (isDryRun || optVerbose)
            printf("rm %s\n", thisFile->queryFilename());
        if (!isDryRun)
            thisFile->remove();
    }
    if (isDryRun || optVerbose)
        printf("rmdir %s\n", dir->queryFilename());
    if (!isDryRun)
        dir->remove();
}

static bool isUrl(const char *str)
{
    return strstr(str, ":/") != NULL;
}

//--------------------------------------------------------------------------------------------------------------

interface IBundleCollection;

interface IBundleInfo : extends IInterface
{
    virtual bool isValid() const = 0;
    virtual const char *queryBundleName() const = 0;
    virtual const char *queryCleanName() const = 0;
    virtual const char *queryVersion() const = 0;
    virtual const char *queryCleanVersion() const = 0;
    virtual const char *queryDescription() const = 0;
    virtual unsigned numDependencies() const = 0;
    virtual const char *queryDependency(unsigned idx) const = 0;
    virtual void printError() const = 0;
    virtual void printFullInfo() const = 0;
    virtual void printShortInfo() const = 0;
    virtual void checkValid() const = 0;
    virtual bool checkPlatformVersion() const = 0;
    virtual bool checkVersion(const char *required) const = 0;
    virtual bool checkDependencies(const IBundleCollection &allBundles, const IBundleInfo *bundle, bool going) const = 0;

    virtual const char *queryInstalledPath() const = 0;
    virtual bool selftest() const = 0;
    virtual void setInstalledPath(const char *path) = 0;
    virtual void setActive(bool active) = 0;
};

interface IBundleInfoSet : extends IInterface
{
    virtual const char *queryName() const = 0;
    virtual IBundleInfo *queryActive() = 0;
    virtual unsigned numVersions() = 0;
    virtual IBundleInfo *queryVersion(const char *version) = 0;
    virtual IBundleInfo *queryVersion(unsigned idx) = 0;
    virtual void deleteVersion(const char *version, bool isDryRun) = 0;
    virtual void deleteAllVersions(bool isDryRun) = 0;
    virtual void deleteRedirectFile(bool isDryRun) = 0;
    virtual void setActive(const char *version) = 0;
    virtual void addBundle(IBundleInfo *bundle) = 0;
};

interface IBundleCollection : extends IInterface
{
    virtual unsigned numBundles() const = 0;
    virtual IBundleInfoSet *queryBundleSet(const char *name) const = 0;
    virtual IBundleInfoSet *queryBundleSet(unsigned idx) const = 0;
    virtual IBundleInfo *queryBundle(const char *name, const char *version) const = 0;  // returns active if version==NULL
    virtual bool checkDependencies(const IBundleInfo *bundle, bool going) const = 0;
};

StringArray deleteOnCloseDown;

void doDeleteOnCloseDown()
{
    ForEachItemIn(idx, deleteOnCloseDown)
    {
        const char *goer = deleteOnCloseDown.item(idx);
        try
        {
            Owned<IFile> goFile = createIFile(goer);
            recursiveRemoveDirectory(goFile, false);
        }
        catch (IException *E)
        {
            StringBuffer m;
            E->errorMessage(m);
            printf("Error: %s\n", m.str());
            E->Release();
        }
    }
}

StringBuffer & fetchURL(const char *bundleName, StringBuffer &fetchedLocation)
{
    // If the bundle name looks like a url, fetch it somewhere temporary first...
    if (isUrl(bundleName))
    {
        //Put it into a temp directory - we need the filename to be right
        //I don't think there is any way to disable the following warning....
        const char *tmp = tmpnam(NULL);
        recursiveCreateDirectory(tmp);
        deleteOnCloseDown.append(tmp);
        if (optVerbose)
            printf("mkdir %s\n", tmp);

        const char *ext = pathExtension(bundleName);
        if (ext && strcmp(ext, ".git")==0)
        {
            fetchedLocation.append(tmp).append(PATHSEPCHAR);
            splitFilename(bundleName, NULL, NULL, &fetchedLocation, NULL);
            StringBuffer output;
            VStringBuffer params("clone --depth=1 %s %s", bundleName, fetchedLocation.str());
            unsigned retCode = doPipeCommand(output, "git", params, NULL);
            if (optVerbose)
                printf("%s", output.str());
            if (retCode == START_FAILURE)
                throw makeStringExceptionV(0, "Could not retrieve repository %s: git executable missing?", bundleName);
        }
        else
        {
            // Use curl executable
            fetchedLocation.append(tmp).append(PATHSEPCHAR);
            splitFilename(bundleName, NULL, NULL, &fetchedLocation,  &fetchedLocation);
            StringBuffer output;
            VStringBuffer params("-o %s %s", fetchedLocation.str(), bundleName);
            unsigned retCode = doPipeCommand(output, "curl", params, NULL);
            if (optVerbose)
                printf("%s", output.str());
            if (retCode == START_FAILURE)
                throw makeStringExceptionV(0, "Could not retrieve url %s: curl executable missing?", bundleName);
        }
    }
    else
        fetchedLocation.append(bundleName); // Assume local
    return fetchedLocation;
}

class CBundleInfo : public CInterfaceOf<IBundleInfo>
{
public:
    CBundleInfo(const char *bundle)
    {
        active = false;
        try
        {
            StringBuffer output;
            StringBuffer eclOpts("- --nologfile --nostdinc -Me --nobundles");
            StringBuffer bundleName;
            Owned<IFile> bundleFile = createIFile(bundle);
            if (bundleFile->exists())
            {
                StringBuffer cleanedParam(bundle);
                removeTrailingPathSepChar(cleanedParam);
                StringBuffer path;
                splitFilename(cleanedParam, &path, &path, &bundleName, NULL, true);
                if (!path.length())
                    path.append(".");
                if (bundleFile->isDirectory() && !directoryContainsBundleFile(bundleFile))
                    includeOpt.appendf(" -I%s", bundle);
                else
                    includeOpt.appendf(" -I%s", path.str());
            }
            else
                throw MakeStringException(0, "File not found");
            eclOpts.append(includeOpt);
            VStringBuffer bundleCmd("IMPORT %s.Bundle as B;"
                                    " [ (UTF8) B.name, (UTF8) B.version, B.description, B.license, B.copyright ] +"
                                    " [ (UTF8) COUNT(b.authors) ] + B.authors + "
                                    " [ (UTF8) COUNT(B.dependsOn) ] + B.dependsOn + "
                                    " [ (UTF8) #IFDEFINED(B.platformVersion, '')]", bundleName.str());
            if (doPipeCommand(output, "eclcc", eclOpts.str(), bundleCmd) > 0)
                throw MakeStringException(0, "%s cannot be parsed as a bundle\n", bundle);
            // output should contain [ 'name', 'version', etc ... ]
            if (optVerbose)
                printf("Bundle info from ECL compiler: %s\n", output.str());
            if (!output.length())
                throw MakeStringException(0, "%s cannot be parsed as a bundle\n", bundle);
            RegExpr re("'{[^'\r\n\\\\]|\\\\[^\r\n]}*'");
            extractAttr(re, name, output.str());
            if (!strieq(name, bundleName))
                throw MakeStringException(0, "Bundle name %s does not match file name %s", name.get(), bundleName.str());
            extractAttr(re, version);
            extractAttr(re, description);
            extractAttr(re, license);
            extractAttr(re, copyright);
            extractArray(re, authors);
            extractArray(re, depends);
            extractAttr(re, platformVersion);
            // version must contain nothing but alphanumeric + . (no underscores, as they can clash with the ones we put in for .)
            RegExpr validVersions("^[A-Za-z0-9.]*$");
            if (!validVersions.find(version))
                throw MakeStringException(0, "Illegal character in version string %s in bundle %s", version.get(), bundleName.str());
            cleanVersion.append(version).replace('.', '_');
            if (isdigit(cleanVersion.charAt(0)))
                cleanVersion.insert(0, "V");
            cleanName.set(name);
            cleanName.setCharAt(0, toupper(cleanName.charAt(0)));
        }
        catch (IException *E)
        {
            exception.setown(E);
        }
    }
    virtual void checkValid() const { if (exception) throw exception.getLink(); }
    virtual bool isValid() const { return exception == NULL; }
    virtual const char *queryBundleName() const { return name.get(); }
    virtual const char *queryCleanName() const { return cleanName.str(); }
    virtual const char *queryVersion() const { return version.get(); }
    virtual const char *queryCleanVersion() const { return cleanVersion.str(); }
    virtual const char *queryDescription() const { return description.get(); }
    virtual unsigned numDependencies() const { return depends.ordinality(); }
    virtual const char *queryDependency(unsigned idx) const { return depends.item(idx); }
    virtual const char *queryInstalledPath() const { return installedPath.get(); }
    virtual void setInstalledPath(const char *path) { installedPath.set(path); }
    virtual void setActive(bool isActive) { active = isActive; }
    virtual void printError() const
    {
        if (exception)
        {
            StringBuffer s;
            exception->errorMessage(s);
            printf("%s", s.str());
        }
    }
    virtual void printFullInfo() const
    {
        printAttr("Name:", name);
        printAttr("Version:", version);
        printAttr("Description:", description);
        printAttr("License:", license);
        printAttr("Copyright:", copyright);
        printArray("Authors:", authors);
        printArray("DependsOn:", depends);
        printAttr("Platform:", platformVersion);
    }
    virtual void printShortInfo() const
    {
        if (!active)
            printf("(");
        printf("%-13s %-10s %s", name.get(), version.get(), description.get());
        if (!active)
            printf(")");
        printf("\n");
    }
    virtual bool checkVersion(const char *required) const
    {
        StringArray requiredVersions;
        requiredVersions.appendList(required, "-");
        const char *minOk, *maxOk;
        minOk = requiredVersions.item(0);
        if (requiredVersions.isItem(1))
            maxOk = requiredVersions.item(1);
        else
            maxOk = NULL;
        return versionOk(version, minOk, maxOk);
    }
    virtual bool checkPlatformVersion() const
    {
        bool ok = true;
        if (platformVersion.length())
        {
            const char *platformFound = queryPlatformVersion();
            StringArray requiredVersions;
            requiredVersions.appendList(platformVersion, "-");
            const char *minOk, *maxOk;
            minOk = requiredVersions.item(0);
            if (requiredVersions.isItem(1))
                maxOk = requiredVersions.item(1);
            else
                maxOk = NULL;
            if (!versionOk(platformFound, minOk, maxOk))
            {
                printf("%s requires platform version %s, version %s found\n", name.get(), platformVersion.get(), platformFound);
                ok = false;
            }
        }
        return ok;
    }
    virtual bool checkDependencies(const IBundleCollection &allBundles, const IBundleInfo *bundle, bool going) const
    {
        bool ok = true;
        for (unsigned i = 0; i < depends.length(); i++)
        {
            if (!checkDependency(allBundles, depends.item(i), bundle, going))
                ok = false;
        }
        return ok;
    }

    bool selftest() const
    {
        VStringBuffer exeFileName(".%c_%s-bundle-selftest", PATHSEPCHAR, cleanName.str());
        VStringBuffer eclOpts("-   --nologfile -o%s", exeFileName.str());
        VStringBuffer bundleCmd("IMPORT %s as B;\n"
                                "#IF (#ISDEFINED(B.__selftest))\n"
                                "  EVALUATE(B.__selftest);\n"
                                "#ELSE\n"
                                "  FAIL(253, 'No selftests exported');\n"
                                "#END\n"
                , cleanName.str());
        StringBuffer output;
        if (doPipeCommand(output, "eclcc", eclOpts.str(), bundleCmd) > 0)
        {
            printf("%s\n", output.str());
            printf("%s selftests cannot be compiled\n", cleanName.str());
        }
        int retcode = doPipeCommand(output, exeFileName, "", NULL);
        printf("%s\n", output.str());
        if (retcode > 0)
        {
            if (retcode != 253)
                printf("%s selftests returned non-zero\n", cleanName.str());
            else
                printf("%s has no selftests\n", cleanName.str());
            return false;
        }
        else
            printf("%s selftests succeeded\n", cleanName.str());
        return true;
    }
private:
    bool checkDependency(const IBundleCollection &allBundles, const char *dep, const IBundleInfo *bundle, bool going) const
    {
        // MORE - this is really doing two separate jobs, and should be split
        // 1. check if I have a dependency on bundle
        // 2. Check if all my dependencies are met
        if (!dep || !*dep)
            return true;  // Bit of a silly bundle...
        StringArray depVersions;
        depVersions.appendList(dep, " ");
        const char *dependentName = depVersions.item(0);
        if (bundle && !strieq(dependentName, bundle->queryBundleName()))
            return true;
        const IBundleInfo *dependentBundle = bundle;
        if (!dependentBundle)
        {
            dependentBundle = allBundles.queryBundle(dependentName, NULL);
            if (!dependentBundle || !dependentBundle->isValid())
            {
                printf("%s requires %s, which cannot be loaded\n", name.get(), dependentName);
                return false;
            }
        }
        if (going)
        {
            printf("%s is required by %s\n", dependentName,  name.get());
            return false;
        }
        else if (depVersions.length() > 1)
        {
            bool ok = false;
            for (unsigned i = 1; i < depVersions.length(); i++)
                if (dependentBundle->checkVersion(depVersions.item(i)))
                    ok = true;
            if (!ok)
                printf("%s requires %s, version %s found\n", name.get(), dep, dependentBundle->queryVersion());
            return ok;
        }
        return true;
    }

    static void printAttr(const char *prompt, const StringAttr &attr)
    {
        printf("%-13s%s\n", prompt, attr.get());
    }
    static void printArray(const char *prompt, const StringArray &array)
    {
        if (array.length())
        {
            printf("%-13s", prompt);
            ForEachItemIn(idx, array)
            {
                if (idx)
                    printf(", ");
                printf("%s", array.item(idx));
            }
            printf("\n");
        }
    }
    void extractAttr(RegExpr &re, StringAttr &dest, const char *searchIn = NULL)
    {
        const char *found = re.find(searchIn);
        if (!found)
            throw MakeStringException(0, "Unexpected response from eclcc\n");
        StringBuffer cleaned;
        found++;  // skip the '
        unsigned foundLen = re.findlen()-2; // and the trailing ''
        for (unsigned i = 0; i < foundLen; i++)
        {
            unsigned char c = found[i];
            if (c=='\\')
            {
                i++;
                c = found[i];
                switch (c)
                {
                case 't': c = '\t'; break;
                case 'r': c = '\r'; break;
                case 'n': c = '\n'; break;
                default:
                    if (isdigit(c))
                    {
                        unsigned value = c - '0';
                        value = value * 8 + (found[++i] - '0');
                        value = value * 8 + (found[++i] - '0');
                        c = value;
                    }
                    break;
                }
            }
            cleaned.append(c);
        }
        dest.set(cleaned);
    }
    void extractArray(RegExpr &re, StringArray &dest)
    {
        StringAttr temp;
        extractAttr(re, temp);
        unsigned count = atoi(temp);
        while (count--)
        {
            extractAttr(re, temp);
            dest.append(temp);
        }
    }

    StringBuffer includeOpt;    // The -I option to pass to eclcc to get this bundle included
    StringAttr installedPath;
    StringAttr name;
    StringBuffer cleanName;
    StringAttr version;
    StringBuffer cleanVersion;
    StringAttr description;
    StringAttr license;
    StringAttr copyright;
    StringArray authors;
    StringArray depends;
    StringAttr platformVersion;
    Owned<IException> exception;
    bool active;
};

class CBundleInfoSet : public CInterfaceOf<IBundleInfoSet>
{
public:
    CBundleInfoSet(const char *_path, const char *_bundlePath)
        : path(_path)
    {
        loaded = false;
        active = false;
        init(_bundlePath);
    }
    CBundleInfoSet(const char *_path, const char *_bundlePath, IBundleInfo *_bundle)
        : path(_path)
    {
        loaded = true;
        active = false;
        bundles.append(*LINK(_bundle));
        init(_bundlePath);
    }
    virtual const char *queryName() const
    {
        return name.str();
    }
    virtual IBundleInfo *queryActive()
    {
        checkLoaded();
        if (active)
            return &bundles.item(0);
        else
            return NULL;
    }
    virtual unsigned numVersions()
    {
        checkLoaded();
        return bundles.length();
    }
    virtual IBundleInfo *queryVersion(const char *version)
    {
        checkLoaded();
        unsigned idx = findVersion(version);
        return idx==NotFound ? NULL : &bundles.item(idx);
    }
    virtual IBundleInfo *queryVersion(unsigned idx)
    {
        checkLoaded();
        return &bundles.item(idx);
    }
    virtual void deleteVersion(const char *version, bool isDryRun)
    {
        checkLoaded();
        unsigned idx = findVersion(version);
        if (idx==NotFound)
            throw MakeStringException(0, "No version %s found for bundle %s", version, name.str());
        deleteBundle(idx, isDryRun);
    }
    virtual void deleteAllVersions(bool isDryRun)
    {
        checkLoaded();
        ForEachItemInRev(idx, bundles)
        {
            deleteBundle(idx, isDryRun);
        }
        if (isDryRun && bundles.length() > 1)
            printf("rmdir %s\n", path.get());
    }
    virtual void deleteRedirectFile(bool isDryRun)
    {
        Owned<IFile> redirector = createIFile(redirectFileName);
        if (redirector->exists())
        {
            if (isDryRun || optVerbose)
                printf("rm %s\n", redirector->queryFilename());
            if (!isDryRun)
                redirector->remove();
        }
    }
    virtual void createRedirectFile(IBundleInfo *bundle)
    {
        Owned<IFile> redirector = createIFile(redirectFileName);
        const char *name = bundle->queryCleanName();
        const char *version = bundle->queryCleanVersion();
        VStringBuffer redirect("IMPORT %s.%s.%s.%s as _%s; EXPORT %s := _%s;", VERSION_SUBDIR, name, version, name, name, name, name);
        Owned<IFileIO> rfile = redirector->open(IFOcreaterw);
        rfile->write(0, redirect.length(), redirect.str());
        bundle->setActive(true);
    }
    virtual void setActive(const char *version)
    {
        checkLoaded();
        if (strieq(version, "none"))
        {
            if (active)
            {
                bundles.item(0).setActive(false);
                deleteRedirectFile(false);
            }
            active = false;
        }
        else
        {
            unsigned newidx = findVersion(version);
            if (newidx==NotFound)
                throw MakeStringException(0, "No version %s found for bundle %s", version, name.str());
            IBundleInfo *newActive = &bundles.item(newidx);
            if (active && (newidx==0))
                printf("%s version %s is already active\n", newActive->queryBundleName(), version);
            else
            {
                bundles.item(0).setActive(false);
                bundles.swap(newidx, 0);
                createRedirectFile(newActive);
                active = true;
            }
        }
    }
    virtual void addBundle(IBundleInfo *_bundle)
    {
        bundles.append(*LINK(_bundle));
    }
private:
    void init(const char *_bundlePath)
    {
        splitFilename(path, NULL, NULL, &name, NULL);
        redirectFileName.append(_bundlePath);
        addPathSepChar(redirectFileName).append(name).append(".ecl");
    }
    unsigned findVersion(const char *version)
    {
        assertex(version && *version);
        ForEachItemIn(idx, bundles)
        {
            if (bundles.item(idx).queryVersion() && strieq(version, bundles.item(idx).queryVersion()))
                return idx;
        }
        return NotFound;
    }
    void deleteBundle(unsigned idx, bool isDryRun)
    {
        const IBundleInfo &goer = bundles.item(idx);
        const char *installedDir = goer.queryInstalledPath();
        if (installedDir)
        {
            Owned<IFile> versionDir = createIFile(installedDir);
            recursiveRemoveDirectory(versionDir, isDryRun);
        }
        if (!idx)
            active = false;
        if (!isDryRun)
            bundles.remove(idx);
        if (installedDir && bundles.length() == (isDryRun ? 1 : 0))
        {
            Owned<IFile> versionsDir = createIFile(path);
            recursiveRemoveDirectory(versionsDir, isDryRun);
        }
    }
    void checkLoaded()
    {
        if (!loaded)
        {
            StringBuffer activeBundlePath(path);
            Owned<IBundleInfo> activeBundle =  new CBundleInfo(redirectFileName);
            if (activeBundle->isValid())
            {
                addPathSepChar(activeBundlePath).append(activeBundle->queryCleanVersion());
                activeBundle->setInstalledPath(activeBundlePath);
                activeBundle->setActive(true);
                bundles.append(*activeBundle.getClear());
                active = true;
            }
            Owned<IFile> versionsDir = createIFile(path);
            Owned<IDirectoryIterator> versions = versionsDir->directoryFiles(NULL, false, true);
            ForEach (*versions)
            {
                IFile &f = versions->query();
                const char *vname = f.queryFilename();
                if (f.isDirectory() && vname && vname[0] != '.')
                {
                    if (!streq(activeBundlePath, vname))
                    {
                        Owned<IDirectoryIterator> bundleFiles = f.directoryFiles(NULL, false, true);  // Should contain a single file - either a .ecl file or a directory
                        ForEach (*bundleFiles)
                        {
                            Owned<IBundleInfo> bundle = new CBundleInfo(bundleFiles->query().queryFilename());
                            bundle->setInstalledPath(vname);
                            bundles.append(*bundle.getClear());
                            break;
                        }
                    }
                }
            }
            loaded = true;
        }
    }
    StringAttr path;
    StringBuffer redirectFileName;
    StringBuffer name;
    IArrayOf<IBundleInfo> bundles;
    bool loaded;
    bool active;
};

class CBundleCollection : public CInterfaceOf<IBundleCollection>
{
public:
    CBundleCollection(const char *_bundlePath, const char *filter = NULL) : bundlePath(_bundlePath)
    {
        if (optVerbose)
            printf("Using bundle path %s\n", bundlePath.get());
        Owned<IFile> bundleDir = createIFile(bundlePath);
        if (bundleDir->exists())
        {
            if (!bundleDir->isDirectory())
                throw MakeStringException(0, "Bundle path %s does not specify a directory", bundlePath.get());
            StringBuffer versionsPath(bundlePath);
            addPathSepChar(versionsPath).append(VERSION_SUBDIR);
            Owned<IFile> versionsDir = createIFile(versionsPath);
            Owned<IDirectoryIterator> versions = versionsDir->directoryFiles(NULL, false, true);
            ForEach (*versions)
            {
                IFile &f = versions->query();
                const char *name = f.queryFilename();
                if (f.isDirectory() && name && name[0] != '.')
                {
                    if (filter)
                    {
                        StringBuffer tail;
                        splitFilename(name, NULL, NULL, &tail, NULL);
                        if (!WildMatch(tail, filter, true))
                            continue;
                    }
                    bundleSets.append(*new CBundleInfoSet(name, bundlePath));
                }
            }
        }
        bundleSets.sort(compareBundleSets);
    }
    virtual unsigned numBundles() const
    {
        return bundleSets.length();
    }
    virtual IBundleInfoSet *queryBundleSet(const char *name) const
    {
        // MORE - at some point a hash table might be called for?
        assertex(name && *name);
        ForEachItemIn(idx, bundleSets)
        {
            if (strieq(name, bundleSets.item(idx).queryName()))
                return &bundleSets.item(idx);
        }
        return NULL;
    }
    virtual IBundleInfoSet *queryBundleSet(unsigned idx) const
    {
        return &bundleSets.item(idx);
    }
    virtual IBundleInfo *queryBundle(const char *name, const char *version) const
    {
        IBundleInfoSet *bundleSet = queryBundleSet(name);
        if (!bundleSet)
            return NULL;
        IBundleInfo *bundle = version ? bundleSet->queryVersion(version) : bundleSet->queryActive();
        if (!bundle || !bundle->isValid())
            return NULL;
        return bundle;
    }
    virtual bool checkDependencies(const IBundleInfo *bundle, bool going) const
    {
        bool ok = true;
        ForEachItemIn(idx, bundleSets)
        {
            const IBundleInfo *active = bundleSets.item(idx).queryActive();
            if (active && active->isValid() && !active->checkDependencies(*this, bundle, going))
                ok = false;
        }
        return ok;
    }

private:
    static int compareBundleSets(IInterface * const *a, IInterface * const *b)
    {
        IBundleInfoSet *aa = (IBundleInfoSet *) *a;
        IBundleInfoSet *bb = (IBundleInfoSet *) *b;
        return stricmp(aa->queryName(), bb->queryName());
    }
    StringAttr bundlePath;
    IArrayOf<IBundleInfoSet> bundleSets;
};


//-------------------------------------------------------------------------------------------------

class EclCmdBundleBase : public EclCmdCommon
{
public:
    EclCmdBundleBase(bool _bundleCompulsory = true)
      : EclCmdCommon(false), bundleCompulsory(_bundleCompulsory)
    {
    }
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg != '-')
            {
                if (optBundle.isEmpty())
                    optBundle.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return false;
                }
                continue;
            }
            if (matchCommandLineOption(iter, true) != EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optBundle.isEmpty() && bundleCompulsory)
        {
            printf("Missing bundle name\n");
            return false;
        }
        if (!EclCmdCommon::finalizeOptions(globals))
            return false;
        ::optVerbose = optVerbose;
        getCompilerPaths();
        installFileHooks(hooksPath.str());
        return true;
    }
protected:
    void getCompilerPaths()
    {
        StringBuffer output;
        doPipeCommand(output, "eclcc", "--nologfile -showpaths", NULL);
        extractValueFromEnvOutput(bundlePath, output, ECLCC_ECLBUNDLE_PATH);
        extractValueFromEnvOutput(hooksPath, output, HPCC_FILEHOOKS_PATH);
    }
    bool isFromFile() const
    {
        // If a supplied bundle id contains pathsep or ., assume a filename or directory is being supplied
        return strchr(optBundle, PATHSEPCHAR) != NULL || strchr(optBundle, '.') != NULL || isUrl(optBundle);
    }
    StringAttr optBundle;
    StringBuffer bundlePath;
    StringBuffer hooksPath;
    bool bundleCompulsory;
};

//-------------------------------------------------------------------------------------------------

class EclCmdBundleBaseWithVersion : public EclCmdBundleBase
{
public:
    virtual bool parseCommandLineOptions(ArgvIterator &iter)
    {
        for (; !iter.done(); iter.next())
        {
            const char *arg = iter.query();
            if (*arg != '-')
            {
                if (optBundle.isEmpty())
                    optBundle.set(arg);
                else
                {
                    fprintf(stderr, "\nunrecognized argument %s\n", arg);
                    return false;
                }
                continue;
            }
            if (matchCommandLineOption(iter, true) != EclCmdOptionMatch)
                return false;
        }
        return true;
    }
    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
    {
        if (iter.matchOption(optVersion, ECLOPT_VERSION))
            return EclCmdOptionMatch;
        return EclCmdBundleBase::matchCommandLineOption(iter, finalAttempt);
    }
    virtual void usage()
    {
        printf("   --version <version>    Specify a version of the bundle\n");
        EclCmdBundleBase::usage();
    }
protected:
    IBundleInfo *loadBundle(const CBundleCollection &allBundles, bool fileOk)
    {
        Owned<IBundleInfo> bundle;
        if (isFromFile())
        {
            if (!fileOk)
                throw MakeStringException(0, "Please specify the name of an installed bundle (not a file)");
            bundle.setown(new CBundleInfo(optBundle));
        }
        else
            bundle.set(allBundles.queryBundle(optBundle, optVersion));
        if (!bundle || !bundle->isValid())
        {
            if (optVersion.length())
                throw MakeStringException(0, "Bundle %s version %s could not be loaded", optBundle.get(), optVersion.get());
            else
                throw MakeStringException(0, "Bundle %s could not be loaded", optBundle.get());
        }
        return bundle.getClear();
    }
    StringAttr optVersion;
};

//-------------------------------------------------------------------------------------------------

class EclCmdBundleDepends : public EclCmdBundleBaseWithVersion
{
public:
    virtual int processCMD()
    {
        CBundleCollection allBundles(bundlePath);
        ConstPointerArray active;
        Owned<const IBundleInfo> bundle(loadBundle(allBundles, true));
        return printDependency(allBundles, bundle, 0, active) ? 0 : 1;
    }

    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
    {
        if (iter.matchFlag(optRecurse, ECLOPT_RECURSE))
            return EclCmdOptionMatch;
        return EclCmdBundleBaseWithVersion::matchCommandLineOption(iter, finalAttempt);
    }

    virtual void usage()
    {
        printf("\nUsage:\n"
                "\n"
                "The 'depends' command will show the dependencies of a bundle\n"
                "\n"
                "ecl bundle depends <bundle> \n"
                " Options:\n"
                "   <bundle>               The name of an installed bundle, or of a bundle file\n"
                "   --recurse              Display indirect dependencies\n"
               );
        EclCmdBundleBaseWithVersion::usage();
    }
private:
    bool printDependency(const IBundleCollection &allBundles, const IBundleInfo *bundle, int level, ConstPointerArray &active)
    {
        if (active.find(bundle) != NotFound)
            throw MakeStringException(0, "Circular dependency detected");
        bool ok = true;
        active.append(bundle);
        unsigned numDependencies = bundle->numDependencies();
        for (unsigned i = 0; i < numDependencies; i++)
        {
            for (int l = 0; l < level; l++)
                printf(" ");
            const char *dependency = bundle->queryDependency(i);
            StringArray depVersions;
            depVersions.appendList(dependency, " ");
            const char *dependentName = depVersions.item(0);
            const IBundleInfo *dependentBundle = allBundles.queryBundle(dependentName, NULL);
            if (dependentBundle)
            {
                bool matching = true;
                if (depVersions.length() > 1)
                {
                    matching = false;
                    for (unsigned i = 1; i < depVersions.length(); i++)
                        if (dependentBundle->checkVersion(depVersions.item(i)))
                            matching = true;
                    if (!matching)
                        ok = false;
                }
                printf("%s (%s version %s found)\n", dependency, matching ? "matching" : "non-matching", dependentBundle->queryVersion());
                if (matching && optRecurse)
                {
                    ok = printDependency(allBundles, dependentBundle, level + 1, active) && ok;
                }
            }
            else
                printf("%s (not found)\n", dependency);
        }
        active.pop();
        return ok;
    }

    bool optRecurse;
};

//-------------------------------------------------------------------------------------------------

class EclCmdBundleInfo : public EclCmdBundleBaseWithVersion
{
public:
    virtual int processCMD()
    {
        Owned<const IBundleInfo> bundle;
        if (isFromFile())
        {
            StringBuffer useName;
            fetchURL(optBundle, useName);
            bundle.setown(new CBundleInfo(useName));
        }
        else
        {
            CBundleCollection allBundles(bundlePath, optBundle);
            bundle.setown(loadBundle(allBundles, true));
        }
        bundle->checkValid();
        bundle->printFullInfo();
        return 0;
    }

    virtual void usage()
    {
        printf("\nUsage:\n"
                "\n"
                "The 'info' command will print information about a bundle\n"
                "\n"
                "ecl bundle info <bundle> \n"
                " Options:\n"
                "   <bundle>               The name or URL of a bundle file, or installed bundle\n"
               );
        EclCmdBundleBaseWithVersion::usage();
    }
};

//-------------------------------------------------------------------------------------------------

class EclCmdBundleInstall : public EclCmdBundleBase
{
public:
    EclCmdBundleInstall()
    {
        optForce = false;
        optUpdate = false;
        optDryRun = false;
        optKeepPrior = false;
    }

    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
    {
        if (iter.matchFlag(optDryRun, ECLOPT_DRYRUN))
            return EclCmdOptionMatch;
        if (iter.matchFlag(optForce, ECLOPT_FORCE))
            return EclCmdOptionMatch;
        if (iter.matchFlag(optKeepPrior, ECLOPT_KEEPPRIOR))
            return EclCmdOptionMatch;
        if (iter.matchFlag(optUpdate, ECLOPT_UPDATE))
            return EclCmdOptionMatch;
        return EclCmdBundleBase::matchCommandLineOption(iter, finalAttempt);
    }

    virtual int processCMD()
    {
        StringBuffer useName;
        fetchURL(optBundle, useName);
        Owned<IFile> bundleFile = createIFile(useName);
        if (bundleFile->exists())
        {
            bool ok = true;
            Owned<IBundleInfo> bundle = new CBundleInfo(useName);
            bundle->checkValid();
            const char *version = bundle->queryVersion();
            printf("Installing bundle %s version %s\n", bundle->queryBundleName(), version);

            StringBuffer thisBundlePath(bundlePath);
            addPathSepChar(thisBundlePath).append(VERSION_SUBDIR).append(PATHSEPCHAR).append(bundle->queryCleanName());
            CBundleCollection allBundles(bundlePath);
            IBundleInfoSet *bundleSet = allBundles.queryBundleSet(bundle->queryCleanName());
            if (!bundleSet)
                bundleSet = new CBundleInfoSet(thisBundlePath, bundlePath, bundle);
            else
            {
                const IBundleInfo *active = bundleSet->queryVersion(version);
                if (!active)
                    active = bundleSet->queryActive();
                if (active && active->isValid())
                {
                    if (!optUpdate)
                    {
                        printf("A bundle %s version %s is already installed\n", active->queryBundleName(), active->queryVersion());
                        printf("Specify --update to install a replacement version of this bundle\n");
                        return 1;
                    }
                    int diff = versionCompare(bundle->queryVersion(), active->queryVersion(), true);
                    if (diff <= 0)
                    {
                        printf("Existing active version %s is newer or same version\n", active->queryVersion());
                        ok = false;
                    }
                    else
                    {
                        printf("Updating previously installed version %s\n", active->queryVersion());
                    }
                }
            }
            // Don't combine these using ||  - they have side effects of reporting what the reasons are for not installing
            if (!bundle->checkPlatformVersion())
                ok = false;
            if (!bundle->checkDependencies(allBundles, NULL, false))
                ok = false;
            if (!allBundles.checkDependencies(bundle, false))
                ok = false;
            if (!ok)
            {
                if (!optForce)
                {
                    printf("Specify --force to force installation of this bundle\n");
                    return 1;
                }
                else
                    printf("--force specified - updating anyway\n");
            }
            if (!optKeepPrior)
                bundleSet->deleteAllVersions(optDryRun);
            else if (bundleSet->queryVersion(version))
                bundleSet->deleteVersion(version, optDryRun);  // if reinstalling a currently installed version, you want to delete old copy

            if (!optDryRun && !recursiveCreateDirectory(bundlePath))
                throw MakeStringException(0, "Cannot create bundle directory %s", bundlePath.str());

            // Copy the bundle contents
            StringBuffer versionPath(thisBundlePath);
            versionPath.append(PATHSEPCHAR).append(bundle->queryCleanVersion());
            if (!optDryRun && !recursiveCreateDirectory(versionPath))
                throw MakeStringException(0, "Cannot create bundle version directory %s", versionPath.str());
            if (bundleFile->isDirectory() == foundYes) // could also be an archive, acting as a directory
            {
                if (directoryContainsBundleFile(bundleFile))
                {
                    versionPath.append(PATHSEPCHAR).append(bundle->queryCleanName());
                    if (!optDryRun && !recursiveCreateDirectory(versionPath))
                        throw MakeStringException(0, "Cannot create bundle version directory %s", versionPath.str());
                }
                copyDirectory(bundleFile, versionPath);
            }
            else
            {
                StringBuffer tail;
                splitFilename(bundleFile->queryFilename(), NULL, NULL, &tail, &tail);
                versionPath.append(PATHSEPCHAR).append(tail);
                Owned<IFile> targetFile = createIFile(versionPath.str());
                if (optDryRun || optVerbose)
                    printf("cp %s %s\n", bundleFile->queryFilename(), targetFile->queryFilename());
                if (!optDryRun)
                {
                    if (targetFile->exists())
                        throw MakeStringException(0, "A bundle file %s is already installed", versionPath.str());
                    doCopyFile(targetFile, bundleFile, 1024 * 1024, NULL, NULL, false);
                }
            }
            if (!optDryRun)
            {
                bundleSet->addBundle(bundle);
                bundleSet->setActive(bundle->queryVersion());
                bundle->printShortInfo();
                printf("Installation complete\n");
            }
        }
        else
            throw MakeStringException(0, "%s cannot be resolved as a bundle\n", optBundle.get());
        return 0;
    }

    virtual void usage()
    {
        printf("\nUsage:\n"
                "\n"
                "The 'install' command will install a bundle\n"
                "\n"
                "ecl bundle install <bundle> \n"
                " Options:\n"
                "   <bundle>               The name or URL of a bundle file\n"
                "   --dryrun               Print what would be installed, but do not copy\n"
                "   --force                Install even if required dependencies missing\n"
                "   --keepprior            Do not remove an previous versions of the bundle\n"
                "   --update               Update an existing installed bundle\n"
               );
        EclCmdBundleBase::usage();
    }
private:
    void copyDirectory(IFile *sourceDir, const char *destdir)
    {
        Owned<IDirectoryIterator> files = sourceDir->directoryFiles(NULL, false, true);
        ForEach(*files)
        {
            IFile *thisFile = &files->query();
            StringBuffer tail;
            splitFilename(thisFile->queryFilename(), NULL, NULL, &tail, &tail);
            StringBuffer destname(destdir);
            destname.append(PATHSEPCHAR).append(tail);
            Owned<IFile> targetFile = createIFile(destname);
            if (thisFile->isDirectory()==foundYes)
            {
                if (!optDryRun)
                    targetFile->createDirectory();
                copyDirectory(thisFile, destname);
            }
            else
            {
                if (optDryRun || optVerbose)
                    printf("cp %s %s\n", thisFile->queryFilename(), targetFile->queryFilename());
                if (!optDryRun)
                    doCopyFile(targetFile, thisFile, 1024*1024, NULL, NULL, false);
            }
        }
    }
    bool optUpdate;
    bool optForce;
    bool optDryRun;
    bool optKeepPrior;
};

//-------------------------------------------------------------------------------------------------

class EclCmdBundleList : public EclCmdBundleBase
{
public:
    EclCmdBundleList() : EclCmdBundleBase(false)
    {
        optDetails = false;
    }

    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
    {
        if (iter.matchFlag(optDetails, ECLOPT_DETAILS))
            return EclCmdOptionMatch;
        return EclCmdCommon::matchCommandLineOption(iter, finalAttempt);
    }
    virtual int processCMD()
    {
        CBundleCollection allBundles(bundlePath, optBundle.get());
        for (unsigned i = 0; i < allBundles.numBundles(); i++)
        {
            IBundleInfoSet *bundleSet = allBundles.queryBundleSet(i);
            if (optDetails)
            {
                for (unsigned j = 0; j < bundleSet->numVersions(); j++)
                {
                    const IBundleInfo *bundle = bundleSet->queryVersion(j);
                    if (bundle->isValid())
                       bundle->printShortInfo();
                    else
                       printf("Bundle %s[%d] at %s could not be loaded\n", bundleSet->queryName(), j, bundle->queryInstalledPath());
                }
            }
            else
            {
                printf("%s\n", bundleSet->queryName());
            }
        }
        return 0;
    }
    virtual void usage()
    {
        printf("\nUsage:\n"
                "\n"
                "The 'list' command will list installed bundles\n"
                "\n"
                "ecl bundle list [pattern]\n"
                " Options:\n"
                "   <pattern>              A pattern specifying what bundles to list\n"
                "                          If omitted, all bundles are listed\n"
                "   --details              Report details of each installed bundle\n"
               );
        EclCmdBundleBase::usage();
    }
protected:
    bool optDetails;
};

//-------------------------------------------------------------------------------------------------

class EclCmdBundleSelfTest : public EclCmdBundleBaseWithVersion
{
public:
    virtual int processCMD()
    {
        Owned<const IBundleInfo> bundle;
        if (isFromFile())
        {
            StringBuffer useName;
            fetchURL(optBundle, useName);
            bundle.setown(new CBundleInfo(useName));
        }
        else
        {
            CBundleCollection allBundles(bundlePath, optBundle);
            bundle.setown(loadBundle(allBundles, true));
        }
        bundle->checkValid();
        bundle->selftest();
        return 0;
    }

    virtual void usage()
    {
        printf("\nUsage:\n"
                "\n"
                "The 'selftest' command will run a bundle's selftests\n"
                "\n"
                "ecl bundle info <bundle> \n"
                " Options:\n"
                "   <bundle>               The name or URL of a bundle file, or installed bundle\n"
               );
        EclCmdBundleBaseWithVersion::usage();
    }
};

//-------------------------------------------------------------------------------------------------

class EclCmdBundleUninstall : public EclCmdBundleBaseWithVersion
{
public:
    EclCmdBundleUninstall()
    {
        optDryRun = false;
        optForce = false;
    }
    virtual int processCMD()
    {
        bool ok = true;
        CBundleCollection allBundles(bundlePath);
        Owned<IBundleInfo> goer = loadBundle(allBundles, false);
        IBundleInfoSet *bundleSet = allBundles.queryBundleSet(optBundle);
        assertex(bundleSet); // or loadBundle would fail
        bool wasActive = false;
        if (goer == bundleSet->queryActive())
        {
            ok = allBundles.checkDependencies(goer, true);
            wasActive = true;
        }
        if (!ok)
        {
            if (!optForce)
            {
                printf("Specify --force to force uninstallation\n");
                return 1;
            }
            printf("--force specified - uninstalling anyway\n");
        }
        if (optVersion.length())
            bundleSet->deleteVersion(optVersion, optDryRun);
        else
            bundleSet->deleteAllVersions(optDryRun);
        if (wasActive)  // this will always be true if version not specified
            bundleSet->deleteRedirectFile(optDryRun);
        return 0;
    }

    virtual eclCmdOptionMatchIndicator matchCommandLineOption(ArgvIterator &iter, bool finalAttempt)
    {
        if (iter.matchFlag(optDryRun, ECLOPT_DRYRUN))
            return EclCmdOptionMatch;
        if (iter.matchFlag(optForce, ECLOPT_FORCE))
            return EclCmdOptionMatch;
        return EclCmdBundleBaseWithVersion::matchCommandLineOption(iter, finalAttempt);
    }

    virtual void usage()
    {
        printf("\nUsage:\n"
                "\n"
                "The 'uninstall' command will remove a bundle\n"
                "\n"
                "ecl bundle uninstall <bundle> \n"
                " Options:\n"
                "   <bundle>               The name of an installed bundle\n"
                "   --dryrun               Print files that would be removed, but do not remove them\n"
                "   --force                Uninstall even if other bundles are dependent on this\n"
               );
        EclCmdBundleBaseWithVersion::usage();
    }
private:
    bool optDryRun;
    bool optForce;
};

//-------------------------------------------------------------------------------------------------

class EclCmdBundleUse : public EclCmdBundleBaseWithVersion
{
public:
    virtual int processCMD()
    {
        CBundleCollection allBundles(bundlePath);
        Owned<const IBundleInfo> bundle(loadBundle(allBundles, false));
        IBundleInfoSet *bundleSet = allBundles.queryBundleSet(optBundle);
        assertex(bundleSet);  // or loadBundle should have failed
        bundleSet->setActive(optVersion);
        bundle->printShortInfo();
        return 0;
    }
    virtual bool finalizeOptions(IProperties *globals)
    {
        if (optVersion.isEmpty())
        {
            printf("Version must be specified\n");
            return false;
        }
        return EclCmdBundleBaseWithVersion::finalizeOptions(globals);
    }

    virtual void usage()
    {
        printf("\nUsage:\n"
              "\n"
              "The 'use' command makes a specified version of a bundle active\n"
              "\n"
              "ecl bundle use <bundle> <version>\n"
              " Options:\n"
              "   <bundle>               The name of an installed bundle\n"
              "   --version <version>    The version of the bundle to make active, or \"none\"\n"
             );
        EclCmdBundleBase::usage();
    }
};

//-------------------------------------------------------------------------------------------------

IEclCommand *createBundleSubCommand(const char *cmdname)
{
    if (!cmdname || !*cmdname)
        return NULL;
    else if (strieq(cmdname, "depends"))
        return new EclCmdBundleDepends();
    else if (strieq(cmdname, "info"))
        return new EclCmdBundleInfo();
    else if (strieq(cmdname, "install"))
        return new EclCmdBundleInstall();
    else if (strieq(cmdname, "list"))
        return new EclCmdBundleList();
    else if (strieq(cmdname, "selftest"))
        return new EclCmdBundleSelfTest();
    else if (strieq(cmdname, "uninstall"))
        return new EclCmdBundleUninstall();
    else if (strieq(cmdname, "use"))
        return new EclCmdBundleUse();
    return NULL;
}

//=========================================================================================

class BundleCMDShell : public EclCMDShell
{
public:
    BundleCMDShell(int argc, const char *argv[], EclCommandFactory _factory, const char *_version)
        : EclCMDShell(argc, argv, _factory, _version)
    {
    }

    virtual void usage()
    {
        printf("\nUsage:\n\n"
                "ecl bundle <command> [command options]\n\n"
                "   bundle commands:\n"
                "      depends      Show bundle dependencies\n"
                "      info         Show bundle information\n"
                "      install      Install a bundle\n"
                "      list         List installed bundles\n"
                "      selftest     Run bundle selftests\n"
                "      uninstall    Uninstall a bundle\n"
                "      use          Specify which version of a bundle to use\n"
                );
    }
};

static int doMain(int argc, const char *argv[])
{
    BundleCMDShell processor(argc, argv, createBundleSubCommand, BUILD_TAG);
    return processor.run();
}

int main(int argc, const char *argv[])
{
    assert(versionCompareSingleComponent("1", "2") < 0);
    assert(versionCompareSingleComponent("1a", "1b") < 0);
    assert(versionCompareSingleComponent("1", "1a") < 0);
    assert(versionCompare("1.2.3", "1.2", false) == 0);
    assert(versionCompare("1.2.3", "1.2", true) > 0);
    assert(versionCompare("1.2.3", "1.2.4", false) < 0);
    assert(versionCompare("1.2.3", "1.2.4", true) < 0);
    assert(versionOk("1.4.2", "1.4"));
    assert(versionOk("1.4.2", "1.4"));
    assert(!versionOk("1.4.2", "1.5"));
    assert(versionOk("1.4.2", "1.4", "1.5"));
    assert(versionOk("1.5.2", "1.4", "1.5"));
    assert(!versionOk("1.6.2", "1.4", "1.5"));
    InitModuleObjects();
    removeLog();
    unsigned exitCode;
    try
    {
        exitCode = doMain(argc, argv);
    }
    catch (IException *E)
    {
        StringBuffer m;
        E->errorMessage(m);
        printf("Error: %s\n", m.str());
        E->Release();
        exitCode = 2;
    }
    doDeleteOnCloseDown();
    removeFileHooks();
    releaseAtoms();
    exit(exitCode);
}
