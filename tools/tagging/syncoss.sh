#!/bin/bash
# This script is used to synchronize the oss repository with the master repository for the candidate branches.
#
# <path>/syncoss.sh [--dry-run] [--originuser <origin_user>] [--ossuser <oss_user>]

print_help() {
   cat <<'EOF'
Usage: tools/tagging/syncoss.sh [OPTIONS]

Options:
  --dry-run, --dryrun    Do not perform pushes; show what would be done
  --originuser <user>    GitHub user to use for origin authentication
  --ossuser <user>       GitHub user to use for oss authentication
  -h, --help              Show this help message and exit

This launcher calls cmake_modules/syncoss.sh for each repo in the sync list.
EOF
   exit 0
}

DRY_RUN=0
ORIGIN_USER=""
OSS_USER=""

ARGS=("$@")
if [[ ${#ARGS[@]} -gt 0 ]]; then
   i=0
   while [[ $i -lt ${#ARGS[@]} ]]; do
      a="${ARGS[$i]}"
      case "$a" in
         --dry-run|--dryrun)
            DRY_RUN=1
            ;;
         --originuser)
            j=$((i+1))
            ORIGIN_USER="${ARGS[$j]:-}"
            i=$j
            ;;
         --ossuser)
            j=$((i+1))
            OSS_USER="${ARGS[$j]:-}"
            i=$j
            ;;
         -h|--help)
            print_help
            ;;
      esac
      i=$((i+1))
   done
fi

if [[ -z $all ]]; then
   echo "List of repos not configured (environment variable 'all')"
   exit 2
fi

if [[ -z $gitroot ]]; then
   echo "Root git directory not specified (environment variable 'gitroot')"
   exit 2
fi

scriptdir=$(dirname -- "$( readlink -f -- ""$0""; )")
hpccdir=$scriptdir/../..
gitroot="${gitroot/#\~/$HOME}"

sync="$all helm-chart"

echo "Effective args: ${ARGS[*]}"
echo "Settings:"
echo "  DRY_RUN=$DRY_RUN"
echo "  ORIGIN_USER=$ORIGIN_USER"
echo "  OSS_USER=$OSS_USER"
if [[ $DRY_RUN -eq 1 ]]; then
   echo "Note: running in dry-run mode; no pushes will be performed."
fi

echo Press any key to sync oss for "$sync"
read -n 1 -s

for f in $sync ; do
   if ! cd "$gitroot/$f"; then
      echo "Failed to change directory to $gitroot/$f" >&2
      exit 1
   fi
   echo "Process $f"
   $hpccdir/cmake_modules/syncoss.sh "$@"
done
