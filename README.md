github.com/f20/folder-control
=============================

This repository contains an open source Perl 5 system for file
cataloguing, file backup, synchronisation and file-level deduplication.

CPAN modules used:
* Needed by almost everything: DBD::SQLite.
* Needed by some features: Email::MIME BSD::Resource IO::KQueue Linux::Inotify.
* Recommended for performance: JSON::XS.

UNIX group IDs determine some of the behaviours of these scripts.  The default assumptions about group IDs, which are in the source code of FileMgt106::FileSystem, are as follows:
* 0 and 20 are exempted from some automatic tidying actions.
* 6 is a Cyrus IMAP group.
* 1030 is for files which are world-readable.
* 1037 is a management group.
* Any member of 1026 or 1037 is a member of 1025.
* Any member of 1028 is a member of 1025, 1026, 1066 and 1069.
* Any member of 1029, 1032 or 1034 is a member of 1025 and 1026.
* Any member of 1066 is a member of 1069.
