package FileMgt106::FileSystem;

=head Copyright licence and disclaimer

Copyright 2011-2015 Franck Latrémolière, Reckon LLP.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY AUTHORS AND CONTRIBUTORS "AS IS" AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

=cut

use strict;
use warnings;
use utf8;

use base 'Exporter';
our @EXPORT = qw(STAT_DEV STAT_INO STAT_MODE STAT_NLINK
  STAT_UID STAT_GID STAT_SIZE STAT_MTIME STAT_CHMODDED);

use constant {
    STAT_DEV      => 0,     # device id
    STAT_INO      => 1,     # inode
    STAT_MODE     => 2,     # mode
    STAT_NLINK    => 3,     # number of links
    STAT_UID      => 4,     # user
    STAT_GID      => 5,     # group
    STAT_RDEV     => 6,     # not used
    STAT_SIZE     => 7,     # bytes
    STAT_ATIME    => 8,     # not used
    STAT_MTIME    => 9,     # date modified
    STAT_CTIME    => 10,    # not used
    STAT_BLKSIZE  => 11,    # not used
    STAT_BLOCKS   => 12,    # not used
    STAT_CHMODDED => 13,    # addition
};

my $diff =
    -e '/usr/bin/diff' ? '/usr/bin/diff'
  : -e '/opt/bin/diff' ? '/opt/bin/diff'
  :                      die 'No diff found';

sub filesDiffer($$) {
    ( system $diff, '--brief', '--', @_ ) >> 8;
}

sub justLookingStat {
    sub {
        my @stat = lstat $_[0] or return;
        $stat[STAT_CHMODDED] = 0;
        @stat;
    };
}

sub noInodeStat {
    sub {
        my @stat = lstat $_[0] or return;
        $stat[STAT_CHMODDED] = 0;
        $stat[STAT_MODE] &= 0555;
        @stat;
    };
}

sub managementStat {
    sub {
        my ( $name, $force ) = @_;
        my @stat = lstat $name or return;
        $stat[STAT_CHMODDED] = 0;
        return @stat
          unless $force && -f _ && $force > $stat[STAT_MTIME];
        if ( !$> && $stat[STAT_UID] ) {
            chown( 0, -1, $name ) or return @stat;
            $stat[STAT_UID]      = 0;
            $stat[STAT_CHMODDED] = 1;
        }
        my $rwx1 = 0777 & $stat[STAT_MODE];
        my $rwx2 = ( $stat[STAT_UID] ? 0555 : 0755 ) & $stat[STAT_MODE];
        if ( $rwx2 != $rwx1 ) {
            chmod $rwx2, $name or return @stat;
            $stat[STAT_MODE] += ( $stat[STAT_CHMODDED] = $rwx2 - $rwx1 );
        }
        @stat;
    };
}

sub imapStat {
    sub {
        my ( $name, $force ) = @_;
        my @stat = lstat $name or return;
        $stat[STAT_CHMODDED] = 0;
        return @stat unless $force and -f _ || -d _;
        if ( !$> and $stat[STAT_UID] != 60 || $stat[STAT_GID] != 6 ) {
            chown( 60, 6, $name ) or return @stat;
            $stat[STAT_UID]      = 60;
            $stat[STAT_GID]      = 6;
            $stat[STAT_CHMODDED] = 1;
        }
        my $rwx1 = 0777 & $stat[STAT_MODE];
        my $rwx2 = 0040 | ( $force > $stat[STAT_MTIME] ? 0550 : 0770 ) &
          $stat[STAT_MODE];
        if ( $rwx2 != $rwx1 ) {
            chmod $rwx2, $name or return @stat;
            $stat[STAT_MODE] += ( $stat[STAT_CHMODDED] = $rwx2 - $rwx1 );
        }
        @stat;
    };
}

sub publishedStat {
    my ($rgid) = @_;
    sub {
        my ( $name, $force ) = @_;
        my @stat = lstat $name or return;
        $stat[STAT_CHMODDED] = 0;
        return @stat unless -d _ || -f _;
        my $readOnlyFile =
             -f _
          && !( $stat[STAT_MODE] & 022 )
          && !( $stat[STAT_UID] && ( $stat[STAT_MODE] & 0200 ) );
        if ( $stat[STAT_GID] != $rgid && !$> ) {
            chown( -1, $rgid, $name ) or return @stat;
            $stat[STAT_GID]      = $rgid;
            $stat[STAT_CHMODDED] = 1;
        }
        if ( defined $force ) {
            if ( -f _ && $force > $stat[STAT_MTIME] ) {
                $readOnlyFile = 1;
                if ( !$> && $stat[STAT_UID] ) {
                    chown( 0, -1, $name ) or return @stat;
                    $stat[STAT_UID]      = 0;
                    $stat[STAT_CHMODDED] = 1;
                }
            }
            my $rwx1 = 0777 & $stat[STAT_MODE];
            my $rwx2 = 0;
            if ( -d _ ) {
                $rwx2 = 0775;
            }
            elsif ( -f _ ) {
                $rwx2 = $readOnlyFile && $stat[STAT_UID] ? 0400 : 0600;
                $rwx2 += $readOnlyFile ? 044 : 064;
                $rwx2 += 0111 if $rwx1 & 010;
            }
            if ( $rwx2 && $rwx2 != $rwx1 ) {
                chmod $rwx2, $name or return @stat;
                $stat[STAT_MODE] += ( $stat[STAT_CHMODDED] = $rwx2 - $rwx1 );
            }
        }
        @stat;
    };
}

sub statFromGidAndMapping {

    my ( $rgid, $groupStatusHashref ) = @_;

    my $allowGroupReadACL = sub { undef; };
    if ( my $setfacl = `which setfacl` ) { # FreeBSD or Linux $allowGroupReadACL
            # Volume-specific action, defaulting to POSIX style
            # FreeBSD supports both POSIX and NFSv4
        $setfacl =~ s/\s+$//s;
        my %devMap;    # undef = POSIX, 1 = NFSv4, 2 = none
        my @aclargsposix = ( $setfacl, '-m', "g:$rgid:r" );
        my @aclargsnfsv4 = ( $setfacl, '-m', "g:$rgid:r:allow" );
        $allowGroupReadACL = sub {
            my ( $filename, $devno ) = @_;
            unless ( $devMap{$devno} ) {
                system( @aclargsposix, $filename ) or return 1;
                ++$devMap{$devno};
            }
            return if $devMap{$devno} > 1;
            if ( system( @aclargsnfsv4, $filename ) ) {
                ++$devMap{$devno};
                warn "No ACL support on $devno, tested on $filename in "
                  . `pwd`;
                undef;
            }
            else {
                1;
            }
        };
    }
    elsif ( -e '/System/Library' ) {    # Mac OS X $allowGroupReadACL
            # Assume modern enough to use NFSv4-style ACLs
        my $grp = `dscl . -search /Groups PrimaryGroupID $rgid`;
        $grp =~ s/\t.*//s;
        if ($grp) {
            my @aclargs = ( qw(/bin/chmod +a), "group:$grp allow read" );
            $allowGroupReadACL = sub { !system @aclargs, $_[0]; };
        }
    }

    sub {
        my ( $name, $force ) = @_;
        my @stat = lstat $name or return;
        $stat[STAT_CHMODDED] = 0;
        return @stat unless -d _ || -f _;
        my $readOnlyFile =
             -f _
          && !( $stat[STAT_MODE] & 022 )
          && !( $stat[STAT_UID] && ( $stat[STAT_MODE] & 0200 ) );

        # Categorisation of gids we might encounter:
        # 775 = files with this gid are world readable.
        # 431 = we can read files with this gid.
        # 279 = we may take over files with this gid.
        # 0 = we know nothing about this gid.
        my $groupStatus = $groupStatusHashref->{ $stat[STAT_GID] } || 0;

        if ( !$> && $groupStatus < 400 ) {
            if (   $groupStatus == 279
                || !$readOnlyFile
                || !$allowGroupReadACL->( $name, $stat[STAT_DEV] ) )
            {
                chown( -1, $rgid, $name ) or return @stat;
                $stat[STAT_GID]      = $rgid;
                $stat[STAT_CHMODDED] = 1;
                $groupStatus         = 431;
            }
        }
        if ( defined $force && -f _ && $force > $stat[STAT_MTIME] ) {
            $readOnlyFile = 1;
            if ( !$> && $stat[STAT_UID] ) {
                chown( 0, -1, $name ) or return @stat;
                $stat[STAT_UID]      = 0;
                $stat[STAT_CHMODDED] = 1;
            }
        }
        my $rwx1 = 0777 & $stat[STAT_MODE];
        my $rwx2 = 0;
        if ( -d _ ) {
            $rwx2 = 0700;
            $rwx2 += 070 if $groupStatus && ( $rwx1 & 070 );
            $rwx2 += $rwx1 & 05 if $groupStatus == 775;
        }
        elsif ( -f _ ) {
            $rwx2 = $readOnlyFile && $stat[STAT_UID] ? 0400 : 0600;
            $rwx2 += $readOnlyFile ? 040 : 060 if $groupStatus;
            $rwx2 +=
              $rwx1 &
              ( !$groupStatus ? 0100 : $groupStatus == 775 ? 0111 : 0110 );
            $rwx2 += $rwx1 & 04 if $groupStatus == 775;
        }
        if ( $rwx2 && $rwx2 != $rwx1 ) {
            chmod $rwx2, $name or return @stat;
            $stat[STAT_MODE] += ( $stat[STAT_CHMODDED] = $rwx2 - $rwx1 );
        }
        @stat;
    };
}

1;
