# Tagging new versions


## General

The file tools/git/aliases.sh contains various git aliases which are useful when using git, and may be used by the merge scripts.

The file env.sh.example contains some example environment variable settings.  Copy that locally to env.sh and modify it to match your local setup.

Before running any of the other scripts, process the contents of that file as a source file

```
. env.sh
```

to initialize the common environment variables.

The following repositories should be checked out in a directory reserved for merging and tagging (default for scripts is ~/git):

```
git clone https://github.com/hpcc-systems/eclide.git
git clone https://github.com/hpcc-systems/HPCC-JAPIs.git
git clone https://github.com/hpcc-systems/Spark-HPCC.git
git clone https://github.com/hpcc-systems/LN.git ln
git clone https://github.com/hpcc-systems/HPCC-Platform.git hpcc
git clone https://github.com/hpcc-systems/helm-chart.git
```

The following are required for builds prior to 8.12.x
```
git clone https://github.com/hpcc-systems/nagios-monitoring.git
git clone https://github.com/hpcc-systems/ganglia-monitoring.git
```

The files git-fixversion and git-unupmerge can copied so they are on your default path, and then they will be available as git commands.

## Tagging new versions

The following process should be followed when tagging a new set of versions.  This ensures that real merge conflicts are dealt with separately from merge conflicts of version numbers in the helm charts.

1. Upmerge all changes between candidate branches for the different versions

```
./upmerge A.a.x candidate-A.b.x
./upmerge A.b.x candidate-A.c.x
./upmerge A.c.x master
```

2. Create new point release candidate branches:

```
./gorc.sh A.a.x
./gorc.sh A.b.x
./gorc.sh A.c.x
```

3. Upmerge again - this ensures any conflicts from version number changes are resolved independently of code changes

```
./upmerge A.a.x candidate-A.b.x
./upmerge A.b.x candidate-A.c.x
./upmerge A.c.x master
```


## Taking a build gold:

Go gold with each of the explicit versions

```
./gogold.sh 7.8.76
./gogold.sh 7.10.50
```


## Creating a new rc for an existing point release:

```
./gorc.sh A.a.<n>
```

## Create a new minor/major branch

A new minor branch is created from the current master...

```
./gorcminor.sh 8.12.x
```
