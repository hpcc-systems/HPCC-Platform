# This code is a modified version of the Debian based Ubuntu lsb-base-logging.sh
#
# This will provide functionality to the hooks provided in the modified version
# of the Debian based Ubuntu's  init-functions.
#
# This has been tested to function for shell output for init purposes on 
# Centos 5.5, Debian 5.0.3, Fedora 13, Linux Mint 9, Suse 11.1, and Ubuntu 
# 9.10/10.04
#
# Modified by LexisNexis Risk Solutions
#
# v 1.0 
# d June, 2010
##

# Default init script logging functions suitable for Ubuntu.
# See /lib/lsb/init-functions for usage help.

log_use_usplash () {
    if [ "${loop:-n}" = y ]; then
        return 1
    fi
    type usplash_write >/dev/null 2>&1
}

log_success_msg () {
    if log_use_usplash; then
        usplash_write "TEXT   $*" || true
    fi

    echo " * $@"
}

log_failure_msg () {
    if log_use_usplash; then
        usplash_write "TEXT   $*" || true
    fi

    if log_use_fancy_output; then
        RED=`$TPUT setaf 1`
        NORMAL=`$TPUT op`
        echo " $RED*$NORMAL $@"
    else
        echo " * $@"
    fi
}

log_warning_msg () {
    if log_use_usplash; then
        usplash_write "TEXT   $*" || true
    fi

    if log_use_fancy_output; then
        YELLOW=`$TPUT setaf 3`
        NORMAL=`$TPUT op`
        echo " $YELLOW*$NORMAL $@"
    else
        echo " * $@"
    fi
}

log_begin_msg () {
    log_daemon_msg "$1"
}

log_daemon_msg () {
    if [ -z "$1" ]; then
        return 1
    fi

    if log_use_usplash; then
        usplash_write "TEXT $*" || true
    fi

    if log_use_fancy_output && $TPUT xenl >/dev/null 2>&1; then
        COLS=`$TPUT cols`
        if [ "$COLS" ] && [ "$COLS" -gt 6 ]; then
            COL=`$EXPR $COLS - 7`
        else
        COLS=80
            COL=73
        fi
        # We leave the cursor `hanging' about-to-wrap (see terminfo(5)
        # xenl, which is approximately right). That way if the script
        # prints anything then we will be on the next line and not
        # overwrite part of the message.

        # Previous versions of this code attempted to colour-code the
        # asterisk but this can't be done reliably because in practice
        # init scripts sometimes print messages even when they succeed
        # and we won't be able to reliably know where the colourful
        # asterisk ought to go.

        printf " * $*       "
        # Enough trailing spaces for ` [fail]' to fit in; if the message
        # is too long it wraps here rather than later, which is what we
        # want.
        $TPUT hpa `$EXPR $COLS - 1`
        printf ' '
    else
        echo " * $@"
        COL=
    fi
}

log_progress_msg () {
    :
}

log_end_msg () {
    if [ -z "$1" ]; then
        return 1
    fi

    if log_use_usplash; then
        if [ "$1" -eq 0 ]; then
            usplash_write "SUCCESS OK" || true
        else
            usplash_write "FAILURE failed" || true
        fi
    fi

    if [ "$COL" ] && [ -x "$TPUT" ]; then
        printf "\r"
        $TPUT hpa $COL
        if [ "$1" -eq 0 ]; then
        printf '[ '
            $TPUT setaf 2
            printf OK
            $TPUT op # normal
            echo ' ]'
#            echo "[ OK ]"
    elif [ "$1" -eq 255 ]; then
        printf '['
        $TPUT setaf 3
        printf WARN
        $TPUT op # normal
            echo ']'
        else
            printf '['
            $TPUT setaf 1 # red
            printf FAIL
            $TPUT op # normal
            echo ']'
        fi
    else
        if [ "$1" -eq 0 ]; then
            echo "   ...done."
    elif [ "$1" -eq 255 ]; then
            echo "   ...warn."
        else
            echo "   ...fail!"
        fi
    fi
    return $1
}

log_action_msg () {
    if log_use_usplash; then
        usplash_write "TEXT $*" || true
    fi

    echo " * $@"
}

log_action_begin_msg () {
    log_daemon_msg "$@..."
}

log_action_cont_msg () {
    log_daemon_msg "$@..."
}

log_action_end_msg () {
    # In the future this may do something with $2 as well.
    log_end_msg "$1" || true
}
