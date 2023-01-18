#The following file contains various useful git aliases.  Either use them all or copy and paster individual commands

# Details of the last commit
# $ git log1 [branch]
git config --global --replace-all alias.log1 "log -n 1"

# A graph of the commits from a particular branch
# $ git tree [branch]
git config --global --replace-all alias.tree "log --graph --oneline"

# Abbreviation for checkout
# $ git co master
git config --global --replace-all alias.co "checkout"

# Abbrieviation to create a new branch from the current HEAD
# $ git cb newbranch
git config --global --replace-all alias.cb "checkout -b"

# Recursively check out all submodules
# $ git su
git config --global --replace-all alias.su "submodule update --recursive --init"

# Fast forward merge the current branch so that it includes any changes in origin
# $ git ff
git config --global --replace-all alias.ff "!f1() { git merge origin/`git whoami` --ff-only; }; f1;"

# What is the name of the current branch?
# $ git whoami
git config --global --replace-all alias.whoami "symbolic-ref --short HEAD"

# Which are the most recent branches that have been modified?
# $ git recent [--count=n]
git config --global --replace-all alias.recent "for-each-ref --count=10 --sort=-committerdate refs/heads/ --format='%(refname:short) %(committerdate:relative): %(contents:subject)'"
