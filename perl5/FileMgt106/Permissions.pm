package FileMgt106::Permissions;

# This module defines a hierarchical structure and some special features for group IDs.
# Most of this should really be in a configuration file.

use strict;
use warnings;
use utf8;

use FileMgt106::FileSystem;
use base 'Exporter';
our @EXPORT = qw(statFromGid);

sub statFromGid {
    my ($rgid) = @_;
    return unless $rgid;
    return FileMgt106::FileSystem::managementStat($rgid) if $rgid == 1037;
    return FileMgt106::FileSystem::imapStat($rgid)       if $rgid == 6;
    return FileMgt106::FileSystem::noInodeStat($rgid)    if $rgid == 666666;
    return FileMgt106::FileSystem::publishedStat($rgid)  if $rgid == 1030;

    FileMgt106::FileSystem::statFromGidAndMapping(
        $rgid,
        {
            # Categorisation system for gids:
            # 775 = files with this gid are world readable.
            # 431 = we can read files with this gid.
            # 279 = we may take over files with this gid.
            # otherwise we know nothing about this gid.
            1030 => 775,
            $rgid == 1030 ? () : ( $rgid => 431 ),
            1037 => 279,
            ( grep { $rgid == $_ }
                  qw(1026 1028 1029 1032 1034 1037 1038) ) ? ( 1025 => 431 )
            : (),
            ( grep { $rgid == $_ } qw(1026 1028 1029 1032 1034 1037) )
            ? ( 1026 => 431 )
            : (),
            $rgid == 1025
            ? ( map { $_ => 279; } qw(1026 1028 1029 1032 1034 1037 1038) )
            : (),
            $rgid == 1026
            ? ( map { $_ => 279; } qw(1028 1029 1032 1034 1037) )
            : (),
            $rgid == 1035
            ? ( map { $_ => 279; } qw(1026 1028 1029 1032 1034 1037) )
            : (),
            $rgid == 1028 ? ( 1066 => 431, 1069 => 431 ) : (),
            $rgid == 1066 ? ( 1028 => 279, 1069 => 431 ) : (),
            $rgid == 1069 ? ( 1028 => 279, 1066 => 279 ) : (),
        }
    );
}

1;
