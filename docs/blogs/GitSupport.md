# HPCC Git support

For many years hpcc has had support for eclccserver to compile ecl code from git repositories.  The repositories (and optional branches/users) are configured using environment variables on the server.  With this "githook" mechanism configured you can submit a query from a branch, and eclccserver will pull the source from a github or gitlab repository and compiled it.  This allows you to deploy a specific version of a query to production, without needing to perform any work on the client.

Recent versions of that platform have significantly improved the support for compiling from git repositories.  Some of the changes have been included in older support releases, and others are only in the current release.

## 7.12.x (and following) git improvements

Some significant changes have recently been back-ported to older versions of the platform.

* Speed improvements\
  The code for interfacing with git repositories has been significantly improved.  On large queries this can reduce the compile time by 10s of minutes.  (Previously it ran the `git` command as a child process, now it uses a library to read the repositories directly.)

* Resources and manifests from git\
  This is a significant bug fix.  Previously manifests and resources were only associated with a query if they were contained in source files, which meant they were not included when compiling using the "githook" mechanism.

* Support for git-lfs\
  git-lfs is an extension to git that improves support for large files which is supported by github (https://git-lfs.github.com/) and gitlab. If the git lfs extension is installed then eclccserver will retrieve files from git-lfs.  This is particularly useful for large resources e.g. java packages included as part of the manifest.

Note: You need to update to a recent point release to gain these improvements - the speed improvements alone are a compelling reason to ensure you are up to date.

## Problem of large projects
  
The "githook" mechanism works well when the source code is all contained within a single repository.  This is not an  issue for small to medium projects, but a single repository doesn't scale so well for large projects.  For example you might have one team providing common utility code shared between many teams, another responsible for data ingest and index generation and a third team creating queries using that data.  There are also likely to be other teams, for example those developing external tools like SALT/KEL, and even bundles.  How do you manage these independent source streams?

Previously you had a couple of options:

* Single repository\
  All teams work on a single repository.  Each of the teams is likely to be working on different products with different schedules.  Each team needs to maintain a separate set of branches for the different versions of their code (within the same repository), and changes need to be periodically merged between branches owned by the different teams.  Managing multiple semi-independent branches in the same repo is a recipe for confusion.  Also, some code from external tools is guaranteed to be managed in a different repo.
* Merging repositories\
  Each team works on their own repository and merges changes in from other repositories as they are needed.  The advantage is that the branch structure within a repository is simpler.  Integrating changes from other teams is probably worse.  How do you ensure all the changes from the other team are taken?  How do you avoid problems with incompatible changes or dependencies?  It is likely to lead to multiple copies of the same source not really kept in sync.

This is the problem that the new git support aims to solve.

# New git features in 8.6

The following describes the changes in the most recent version of 8.6.x.  (Some of the following features are in 8.4 but the latest point release of version 8.6.x is recommended.)

## Compiling from a git repository

To compile a query directly from a repository the --main syntax has been extended.  You can now use the command

```
ecl run thor --main demo.main@https://github.com/ghalliday/gch-ecldemo-d#version1 --server=...
```

This submits a query to Thor via esp.  It will retrieve ecl code from the 'version1' branch in the repository https://hithub.com/ghalliday/gch-demo-d, compile the code in file demo/main.ecl and run the query on thor.  Similar to the old "githhook" mechanism the checkout will be done on the remote eclccserver, rather than the client machine.

The reference to the repository location has the following form:

`<protocol:>//<urn>/<user>/<repository>#version`

If the protocol and urn are omitted a default will be used - so this example command can be simplified to the following:
```
ecl run thor --main demo.main@ghalliday/gch-ecldemo-d#version1 --server=...
```

The version-text that follows the # can take one of the following forms:
- The name of a branch
- The name of a tag
- A SHA of a commit.

The branch 'version1' in ghalliday/gch-ecldemo-d has the SHA 3c23ca0, so the following command would be equivalent.
```
ecl run thor --main demo.main@ghalliday/gch-ecldemo-d#3c23ca0 --server=...
```
If the version1 branch changed, the 1st command would compile the updated branch, while the second form would always compile the same code.


This feature is also supported for local compiles using eclcc.  So the following command would locally syntax check the same code:

```
eclcc --main demo.main@ghalliday/gch-ecldemo-d#3c23ca0 -syntax
```

But this time the source would be downloaded and processed on the client machine.

## Packages and dependencies

The other changes in 8.6 aim to solve a couple of problems:

 1. To allow queries to be compiled from multiple repositories, avoiding problems with clashing symbols.\
    This is problem whenever you try and merge source code from multiple teams.  You can get clashes if the teams have modules with the same name, and other errors if you are using the legacy import semantics.
 2. To ensure that the dependencies between repositories are versioned.

The platform improvements allow each git repository to be treated as a separate independent package.  Dependencies between the repositories are specified in a package file which is checked into the repository and versioned along with the ecl code.  The package file indicates what the dependencies are and which versions should be used.  Many of the ideas are borrowed/stolen from npm.

It is best illustrated   with an example:

`package.json:`
```
{
  "name": "demoRepoC",
  "version": "1.0.0",
  "dependencies": {
    "demoRepoD": "ghalliday/gch-ecldemo-d#version1"
  }
}
```

At its simplest the package file gives a name to the package and defines the dependencies.  The `dependencies` property is a list of key-value pairs.  The key provides the name of the ecl module that is used to access the external repository.  The value is a repository reference which uses the same format as a repository in the --main syntax.

To use definitions from the external repository you add an import definition to your ecl code.  For example you might have the following `format/personAsText.ecl`:
```
IMPORT layout;
IMPORT demoRepoD AS demoD;

EXPORT personAsText(layout.person input) :=
    input.name + ': ' + demoD.format.maskPassword(input.password);
```

The name demoRepoD in the second import matches the key value in the `package.json` file.  This code uses the attribute format.maskPassword from the repository ghalliday/gch-ecldemo-d .

Each package is processed independently of any others.  The only connection is through explicit imports of the external packages.  This means that packages can have modules or attributes with the same name and they will not clash with each other.

It is even possible (although not encouraged) to use multiple versions of an external package.  E.g.

`package.json:`
```
{
  "name": "demoRepoC",
  "version": "1.0.0",
  "dependencies": {
    "demoRepoD_V1": "ghalliday/gch-ecldemo-d#version1"
    "demoRepoD_V2": "ghalliday/gch-ecldemo-d#version2"
  }
}
```
`query.ecl:`
```
IMPORT layout;
IMPORT demoRepoD_V1 AS demo1;
IMPORT demoRepoD_V2 AS demo2;

EXPORT personAsText(layout.person input) :=
    'Was: ' + demo1.format.maskPassword(input.password) +
    ' Now: ' + demo2.format.maskPassword(input.password);
```

This is useful if different teams have dependencies on different versions of a 3rd party repository.  It is possible to use code from both teams without having to resolve their dependencies first.  (You are likely to get smaller queries if only single version of a repository is being used.)

## Local development aids

There are a couple of options added to the `eclcc` and `ecl` commands to aid ecl developers.  One common situation is where the developer is working on multiple repositories at the same time.  The `-R` option indicates that instead of using source from an external repository the compiler should use it from a local directory.  The syntax is

```
-R<repo>[#version]=path
```
For example
```
ecl run examples/main.ecl -Rghalliday/gch-ecldemo-d=/home/myuser/source/demod
```

This will then use ecl code for DemoRepoD from `/home/myuser/source/DemoD` rather than `https://github.com/ghalliday/gch-ecldemo-d#version1`.

If you have any issues not resolving repositories it can be helpful to use the `-v` option to enable verbose logging output, including details of the git requests.

## Advanced options

The following options can be used to configure the use of git within helm charts for the cloud version:

- eclccserver.gitUsername
- secrets.git\
  See the authentication section below for more details.
- eclccserver.gitPlane\
  The eclccserver instances are ephemeral, but the same repositories tend to be shared by queries - so it makes sense to be able to cache and share the cloned packages between instances.  This options allows the user to define the storage plane that external packages are cloned to.\
  If the option is not supplied, the default is the first storage plane with a category of git - otherwise eclccserver uses the first storage plane with a category of dll.

Options of ecl and eclcc:

 * --defaultgitprefix\
   This command line option changes the default prefix that is added to relative packages references.  The default can also be configured using the environment variable `ECLCC_DEFAULT_GITPREFIX`.  It defaults to "https://github.com/".
 * --fetchrepos\
   Should external repos that have not been cloned locally be fetched?  This defaults to true in 8.6.  It may be useful to set to false if all external repos are mapped to local directories to double check they are being redirected correctly.
 * --updaterepos\
   Should external repos that have previously been fetched locally be updated?  This defaults to true.  It is useful to set to false if you are working in a situation with no access to the external repositories, or to avoid the overhead of checking for changes if you know there are none.
 * ECLCC_ECLREPO_PATH
   Which directory are the external repos cloned to?  On a client machine this defaults to \<home>/.HPCCSystems/repos (or \<APPDATA>\HPCCSystems\repos on windows).  You can delete the contents of this directory to force a clean download of all repositories.

### npm and package-lock.json

For most developers using the `package.json` files to define the dependencies will be sufficient.  However, it is possible to use the npm package manager to ensure that labels or branches are tied down to a specific SHA. E.g.

```
npm install --package-lock-only
```

This command will create a `package-lock.json` file in the same location as `package.json`.  The npm program will resolve the references to branches and resolve them to the corresponding SHAs.  (The --package-lock-only option indicates that npm should not clone the associated versions of the code to node_modules directories.)  The generated `package-lock.json` file will contain something similar to the following:

```
{
  "packages": {
    "node_modules/demoRepoD": {
      "resolved": "https://github.com/ghalliday/gch-ecldemo-d.git#644c4585221f4dd80ca1e8f05974983455a244e5",
    }
  }
}
```

If a `package-lock.json` file is present it will take precedence over the `package.json` file.  The ecl is compiled in the same way, with eclcc automatically downloading the dependencies.

What is the advantage of using package-lock.json over package.json?   It allows you to use npm's semantic versioning syntax (#semver) which is not currently supported by eclcc.  It also allows you to use a branch in your package.json file as a logical dependency, but resolved to an actual dependency on specific SHA, so that if the branch is updated the query will not change.

Finally npm install can also be used without the --package-lock-only option. This will check out the appropriate version of the code into the node_modules subdirectory of the current project.  eclcc supports the node_module structure as a way of providing the source for external packages - so this is an alternative way to compile the code using eclcc completely independently from the source control system.

## Migrating existing code

If you have an existing single repository and you want to split it up into multiple repositories, what is the best way to go about it?  Take the example of extracting common utilities (currently in a ut module) in to a separate repository.

The original `example.ecl` code that uses it could be:

```
IMPORT ut;
EXPORT example(string request, string user) := ut.normalizeRequest(user + ':' + request);
```

The aim is to extract all of the ut module into another repository, and modify the example code to use it.

### Creating the package.json

First you would define a package file indicating your example code is now dependent on a new extrenal repository:

```
{
  "name": "myExampleRepo",
  "version": "1.0.0",
  "dependencies": {
    "utilities": "myorg/shared-utilities#version1"
  }
}
```

You would also move all the utility code into the 'myorg/shared-utilities' repository, and check it in as the version1 branch.  Delete the utility code from the original location.

### Imports

Then change the remaining code to use that imported external package:

```
IMPORT utilities.ut;
EXPORT example(string request, string user) := ut.normalizeRequest(user + ':' + request);
```
or
```
IMPORT utilities;
EXPORT example(string request, string user) := utilities.ut.normalizeRequest(user + ':' + request);
```

If you are starting from scratch that would probably be the clearest way to express the imports.  However it can be painful to have go through all the existing source replacing `'IMPORT ut;'` with `'IMPORT utilities.ut;'`.  One alternative is to define a attribute in the example repository to define an alias for a module from an external repository.  In this case create a ut.ecl within the root of the main repository as follows:

`ut.ecl:`
```
IMPORT utilities;
EXPORT ut := utilities.ut;
```

With this definition all exiting imports of ut will continue to work and will now refer to the code in the external repository - your existing source code can remain the same.  One consequence of using this approach is it prevents you having modules with the same name in different repositories.

### Macros

Macros differ from other ecl attribute definitions because they are expanded as source in the context they are called from.  This means any identifiers are interpreted in the **calling** context, instead of the context the macro is defined in.  This can cause problems for imports within macros.

For example if you have an attribute for processing all fields in a file:

`ut.processing.processFile.ecl`:
```
EXPORT processFile(infile) := FUNCTION MACRO
   IMPORT ut;
   ...
       ut.processing.function1();
   ...
       ut.other.function2();
ENDMACRO;
```

If this macro is moved into a separate repository, then when it is called the "`IMPORT ut;`" will be processed in the context of the caller's repository.  That will work if that repository contains an alias for ut, but otherwise the import will fail.  The solution is to use the #\$ symbol in the macro.  This symbol expands to the module or folder that contains the definition of the macro (in this case ut.processing) including the context of the repository it was defined in.  The following definition will work wherever it is called from:

`ut.processing.processFile.ecl`:
```
EXPORT processFile(infile) := FUNCTION MACRO
   IMPORT #$.^ AS ut;
   ...
       ut.processing.function1();
   ...
       ut.other.function2();
ENDMACRO;
```

Where #\$ corresponds to ut.processing, so #\$.^ corresponds to ut.

### Bundles

Some of these features overlap with the idea of ecl bundles - which are packages of ecl code that can be installed and used from your ecl code.  However there some significant differences:

- Imports within bundles need to be relative.
- Bundles need to be installed before they can be used.
- There is no way to create a dependency on a particular version of a bundle.  It depends on what is installed.

The new git package mechanism can be used to included existing bundles.  So instead of

```
ecl bundle install DataPatterns
```

you could use the following `package.json` file:

```
{
  ...
  "dependencies": {
    "DataPatterns": "hpcc-systems/DataPatterns#v1.8.2"
  }
}
```

This has the advantage that there is a clear dependency on the version, and the bundle does not need to be separately installed on the eclccserver machine.

### Other notes

One minor implication of the new features is that the archive format has been extended.  Each external package will be included as a nested archive with a package tag to indicate which package it corresponds to.  (All external archives are contained within the root archive, and external packages will only be included once.)


## Authentication

If the external repositories are public (e.g. bundles) then there are no futher requirements.  Private repositories have the additional complication of requiring authentication information - either on the client on eclccserver depending on where the source is gathered.  Git provides various methods for providing the credentials...

### Credentials for client machines or local development

The following are the recommended approaches configuring the credentials on a local development system interacting with github:

1) ssh key.

In this scenario, the ssh key associated with the local developer machine is registered with the github account.  For more details see https://docs.github.com/en/authentication/connecting-to-github-with-ssh/about-ssh

This is used when the github reference is of the form ssh://github.com.  The sshkey can be protected with a passcode, and there are various options to avoid having to enter the passcode each time.

2) github authentication

Download the GitHub command line tool (https://github.com/cli/cli).  You can then use it to authenticate all git access with
```
gh auth login
```

Probably the simplest option if you are using github.  More details are found at https://cli.github.com/manual/gh_auth_login

3) Use a personal access token

These are similar to a password, but with additional restrictions on their lifetime and the resources that can be accessed.

Details on how to to create them are found : https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token

These can then be used with the various git credential caching options.  E.g. see https://git-scm.com/book/en/v2/Git-Tools-Credential-Storage

Note: It is preferrable to use the https:// protocol instead of ssh:// for links in `package-lock.json` files.  If ssh:// is used it requires any machine that processes the dependency to have access to a registered ssh key.

### Credentials for eclccserver

All of the options above are likely to involve some user interaction - passphrases for ssh keys, web interaction with github authentication, and initial entry for cached access tokens.  This is problematic for eclccserver - which cannot support user interaction, and it is preferrable not to pass credentials around.

The solution is to use a personal access token securely stored as a secret.  (This token would generally be associated with a special service account.)  The secret avoids the need to pass credentials and allows the keys to be rotated.

The following describes the support in the different versions:

### Kubernetes

In Kubernetes you need to take the following steps:

a) add the gitUsername property to the eclccserver component in the value.yaml file:

```
eclccserver:
- name: myeclccserver
  gitUsername: ghalliday
```

b) add a secret to the values.yaml file, with a key that matches the username:

```
secrets:
  git:
    ghalliday: my-git-secret
```

note: this secret cannot currently be stored in a vault - see future work.

c1) add a secret to Kubernetes containing the personal access token:

```
apiVersion: v1
kind: Secret
metadata:
  name: my-git-secret
type: Opaque
stringData:
  password: ghp_eZLHeuoHxxxxxxxxxxxxxxxxxxxxol3986sS=
```

```
kubectl apply -f ~/dev/hpcc/helm/secrets/my-git-secret
```

When a query is submitted to eclccserver, any git repositories are accessed using the user name and password configured above.

c2) Store the secret in a vault.

You can also store the PAT inside a vault.  See the documentation and values.yaml file for details of how to configure vaults in more detail.

### Bare-metal

Bare-metal systems require some similar configuration steps:

a) Add the gitUsername property to the EclCCServerProcess entry in the environment file.

```
  <EclCCServerProcess daliServers="mydali"
                      ...
                      gitUsername="ghalliday"
                      ...
```

b1) Store the access token in /opt/HPCCSystems/secrets/git/<user-name>/password

E.g.

```
$cat /opt/HPCCSystems/secrets/git/ghalliday/password
ghp_eZLHeuoHxxxxxxxxxxxxxxxxxxxxol3986sS=
```

b2) Alternatively you can store the credentials in a vault.

You can now define a vaults section within within the Environment/Software section.  E.g.
```
<Environment>
 <Software>
   ...
   <vaults>
    <git name='my-storage-vault' url="http://127.0.0.1:8200/v1/secret/data/git/${secret}" kind="kv-v2" client-secret="myVaultSecret"/>
    ...
   </vaults>
   ...
```

The entries have the same content as the entries in the kubernetes value.yaml file.  The xml above corresponds to the yaml content:

```
   vaults:
     git:
     - name: 'my-storage-vault'
       url: "http://127.0.0.1:8200/v1/secret/data/git/${secret}"
       kind: "kv-v2"
       client-secret: "myVaultSecret"
```

## Future

There are a few features which are being considered for the future:

- Semantic versioning\
  npm supports a special tag of #semver to allow semantic versioning to be used.  Currently you can only use this if you use the `package-lock.json` files generated by npm install.  It is possible that this will be supported natively in the future.
- Various options to aid developers.
- https://track.hpccsystems.com/browse/HPCC-24449 can be used to track the progress of any other proposed features.
