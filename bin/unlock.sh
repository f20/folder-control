#!/bin/sh
if [ "$*" != "" ]
then
	find "$@" -type f -links 1 -not -perm +020 -not -name .DS_Store -exec chmod g+w {} \;
	find "$@" -type f -not -perm +020 -not -name .DS_Store | while read x
	do	
		echo $x
		cat "$x" > "$x$$"
		chmod g+w "$x$$"
		if [ -x "$x" ]; then chmod +x "$x$$"; fi
		mv -f "$x$$" "$x"
	done
fi
