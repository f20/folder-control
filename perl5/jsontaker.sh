#!/usr/bin/env bash

PATH=/usr/local/bin:/usr/local/git/bin:/usr/bin:/bin:/usr/sbin:/sbin::/opt/sbin:/opt/bin

function main() {
    git add "$1.txt"
    git commit -a -q -m "$1.txt"
    if [ -d ../%jbz ]
    then
	    bzip2 "$1.txt"
    	mv -f "$1.txt.bz2" "../%jbz/$1.jbz"
    fi
}

if [ ! -e .git ]; then git init; fi

main "$1" &
