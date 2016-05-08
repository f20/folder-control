#!/bin/sh
if [ "$*" != "" ]
then
	find "$@" -type f -not -perm +020 -not -name .DS_Store | while read x
	do
		cat "$x" > "$x$$"
		chmod g+w "$x$$"
		if [ -x "$x" ]; then chmod +x "$x$$"; fi
		mv -f "$x$$" "$x"
	done
fi
