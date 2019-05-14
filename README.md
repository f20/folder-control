github.com/f20/folder-control
=============================

This repository contains an open source Perl 5 system for file
cataloguing, file backup, synchronisation and file-level deduplication.

CPAN modules required:
* Needed for almost everything: DBD::SQLite; JSON (included in recent versions of Perl).
* Recommended for performance: JSON::XS.
* Needed for some features: BSD::Resource, Email::MIME, IO::KQueue, Linux::Inotify.

UNIX group IDs determine some of the behaviours of these scripts.  The default assumptions about group IDs, which are in the source code of FileMgt106::FileSystem, are as follows:
* 0 and 20 are exempted from some automatic tidying actions.
* 6 is a special Cyrus IMAP group.
* 1030 is for files which are world-readable.
* 1037 is a special management group.
* Any member of 1026 or 1037 is assumed to be a member of 1025.
* Any member of 1028 is assumed to be a member of 1025, 1026, 1066 and 1069.
* Any member of 1029, 1032 or 1034 is assumed to be a member of 1025 and 1026.
* Any member of 1066 is assumed to be a member of 1069.
