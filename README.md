github.com/f20/folder-control
=============================

This repository contains an open source Perl 5 system for file
cataloguing, file backup, synchronisation and file-level deduplication.

* CPAN module needed by almost everything:
    DBD::SQLite
    JSON

* CPAN module recommended for performance:
    JSON::XS

* CPAN modules needed by some features:
    BSD::Resource (FreeBSD and macOS)
    Digest::SHA3
    Email::MIME
    Excel::Writer::XLSX
    IO::KQueue (FreeBSD and macOS)
    Image::ExifTool
    Linux::Inotify (Linux)
    Thread::Pool

UNIX group IDs determine some of the behaviours of these scripts.  The default assumptions about group IDs, which are in the source code of FileMgt106::FileSystem, are as follows:
* Group ID 1037 is for system management.
* Any member of group ID 1037 is also a member of group ID 1028.
* Any member of group IDs 1028 or 1037 is also a member of group ID 1066.

Franck Latrémolière
18 November 2023
