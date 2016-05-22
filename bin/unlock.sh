#!/bin/sh
if [ "$*" != "" ]
then
	find "$@" -type f -links 1 -not -perm -0220 -not -name .DS_Store -exec chmod ug+w {} \;
	find "$@" -type f -not -perm -0220 -not -name .DS_Store | while read x
	do	
		echo $x
		cat "$x" > "$x$$"
		chmod ug+w "$x$$"
		if [ -x "$x" ]; then chmod +x "$x$$"; fi
		mv -f "$x$$" "$x"
	done
fi
