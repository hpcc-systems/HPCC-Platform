# HPCC git support

Version 8.4 of the HPCC platform allows package files to define dependencies between git repositories and also allows you to compile directly from a git repository.

E.g.

```
ecl run hthor --main demo.main@ghalliday/gch-ecldemo-d#version1 --server=...
```

There are no futher requirements if the repositories are public, but private repositories have the additional complication of supplying authentication information.  Git provides various methods for providing the credentials...

## Credentials for local development

The following are the recommended approaches configuring the credentials on a local development system interacting with github:

1) ssh key.

In this scenario, the ssh key associated with the local developer machine is registered with the github account.  For more details see https://docs.github.com/en/authentication/connecting-to-github-with-ssh/about-ssh

This is used when the github reference is of the form ssh://github.com.  The sshkey can be protected with a passcode, and there are various options to avoid having to enter the passcode each time.

It is preferrable to use the https:// protocol instead of ssh:// for links in package-lock.json files.  If ssh:// is used it requires any machine that processes the dependency to have access to a registered ssh key.

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


## Configuring eclccserver

All of the options above are likely to involve some user interaction - passphrases for ssh keys, web interaction with github authentication, and initial entry for cached access tokens.  This is problematic for eclccserver - which cannot support user interaction, and it is preferrable not to pass credentials around.

The solution is to use a personal access token securely stored as a secret.  (This would generally be associated with a special service account.)  This avoids the need to pass credentials and allows the keys to be rotated.

The following describes the support in the different versions:

## Kubernetes

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

note: this cannot currently use a vault - probably need to rethink that.  (Possibly extract from secret and supply as an optional environment variable to be picked up by the bash script.)

c) add a secret to Kubernetes containing the personal access token:

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

When a query is submitted to eclccserver, any git repositories are accessed using the user name and password.

## Bare-metal

Bare-metal require some similar configuration steps:

a) Define the environment variable HPCC_GIT_USERNAME

```
export HPCC_GIT_USERNAME=ghalliday
```

b) Store the access token in /opt/HPCCSystems/secrets/git/$HPCC_GIT_USERNAME/password

E.g.

```
$cat /opt/HPCCSystems/secrets/git/ghalliday/password
ghp_eZLHeuoHxxxxxxxxxxxxxxxxxxxxol3986sS=
```
