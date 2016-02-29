#!/bin/sh
if [ "$*" != "" ]
then
	find "$@" -type f -not -perm +0220 -not -name .DS_Store | while read x
	do
		cat "$x" > "$x$$"
		if [ -x "$x" ]; then chmod +x "$x$$"; fi
		mv -f "$x$$" "$x"
	done
fi
