package FileMgt106::FileSystem;

# Copyright 2011-2023 Franck Latrémolière and others.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY AUTHORS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use strict;
use warnings;
use Encode qw(decode_utf8);

use base 'Exporter';
our @EXPORT_OK = qw(
  STAT_DEV
  STAT_INO
  STAT_MODE
  STAT_NLINK
  STAT_UID
  STAT_GID
  STAT_SIZE
  STAT_MTIME
  STAT_CHMODDED
);

use constant {
    STAT_DEV      => 0,     # device id
    STAT_INO      => 1,     # inode
    STAT_MODE     => 2,     # mode
    STAT_NLINK    => 3,     # number of links
    STAT_UID      => 4,     # user
    STAT_GID      => 5,     # group
    STAT_RDEV     => 6,     # we do not use this
    STAT_SIZE     => 7,     # bytes
    STAT_ATIME    => 8,     # we do not use this
    STAT_MTIME    => 9,     # date modified
    STAT_CTIME    => 10,    # we do not use this
    STAT_BLKSIZE  => 11,    # we do not use this
    STAT_BLOCKS   => 12,    # we do not use this
    STAT_CHMODDED => 13,    # this is our own addition
};

my %aclStyleDevMapSingleton;    # undef = POSIX, 1 = NFSv4, 2 = none

my %macOSGroupFromGidSingleton; # name from gid

my @cmpToolSingleton =
    -e '/usr/bin/cmp'  ? ( '/usr/bin/cmp', '--' )
  : -e '/usr/bin/diff' ? ( '/usr/bin/diff', '--brief', '--' )
  : -e '/opt/bin/diff' ? ( '/opt/bin/diff', '--brief', '--' )
  :                      die 'No cmp found';

sub filesDiffer($$) {
    system( @cmpToolSingleton, @_ ) >> 8;
}

sub new {
    my ( $class, %gidInfo ) = @_;
    return $class unless %gidInfo;
    bless \%gidInfo, $class;
}

sub gidInfo {
    my ($self) = @_;
    return $self if ref $self;
    {
        1037 => [qw()],
        1066 => [qw(1028 1037)],
        1028 => [qw(1037)],
    };
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
        $stat[STAT_UID]      = 1;
        $stat[STAT_GID]      = 2;
        $stat[STAT_INO]      = 3;
        @stat;
    };
}

sub managementStat {
    sub {
        my ( $name, $forceReadOnlyTimeLimit ) = @_;
        my @stat = lstat $name or return;
        $stat[STAT_CHMODDED] = 0;
        return @stat unless -d _ || -f _ && -s _;
        my $readOnlyFile =
             -f _
          && !( $stat[STAT_MODE] & 022 )
          && !( $stat[STAT_UID] && ( $stat[STAT_MODE] & 0200 ) );
        if (   -f _
            && -s _
            && !$readOnlyFile
            && defined $forceReadOnlyTimeLimit
            && $forceReadOnlyTimeLimit > $stat[STAT_MTIME] )
        {
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
            $rwx2 = 0770;
        }
        elsif ( -f _ ) {
            $rwx2 = !$readOnlyFile ? 0660 : $stat[STAT_UID] ? 0440 : 0640;
            $rwx2 += $rwx1 & 0110;
        }
        if ( $rwx2 && $rwx2 != $rwx1 ) {
            chmod $rwx2, $name or return @stat;
            $stat[STAT_MODE] += ( $stat[STAT_CHMODDED] = $rwx2 - $rwx1 );
        }
        @stat;
    };
}

sub statFromGid {

    my ( $self, $rgid ) = @_;
    return unless $rgid;
    my $gidInfo = $self->gidInfo;
    my $myInfo  = $gidInfo->{$rgid} || '';
    return FileMgt106::FileSystem::managementStat($rgid)
      if $myInfo && !@$myInfo;

    # Categorisation system for gids we know something about:
    # 431: we can read files with this gid.
    # 279: we may convert files with this gid to our gid.
    my %map = ( $rgid => 431 );
    if ( ref $myInfo eq 'ARRAY' ) {
        $map{$_} = 279 foreach @$myInfo;
    }
    while ( my ( $gid, $info ) = each %$gidInfo ) {
        $map{$gid} = 431 if grep { $rgid == $_; } @$info;
    }

    my $allowGroupReadACL = sub { undef; };
    if ( my $setfacl = `which setfacl` ) { # FreeBSD or Linux $allowGroupReadACL
            # Volume-specific action, defaulting to POSIX style
            # FreeBSD supports both POSIX and NFSv4
        $setfacl =~ s/\s+$//s;
        my @aclargsposix = ( $setfacl, '-m', "g:$rgid:r" );
        my @aclargsnfsv4 = ( $setfacl, '-m', "g:$rgid:r:allow" );
        $allowGroupReadACL = sub {
            my ( $filename, $devno ) = @_;
            unless ( $aclStyleDevMapSingleton{$devno} ) {
                system( @aclargsposix, $filename ) or return 1;
                ++$aclStyleDevMapSingleton{$devno};
            }
            return if $aclStyleDevMapSingleton{$devno} > 1;
            if ( system( @aclargsnfsv4, $filename ) ) {
                ++$aclStyleDevMapSingleton{$devno};
                warn "No ACL support on $devno, tested on $filename in "
                  . decode_utf8(`pwd`);
                undef;
            }
            else {
                1;
            }
        };
    }
    elsif ( -e '/System/Library' ) {    # macOS $allowGroupReadACL
          # Modern versions of macOS use NFSv4-style ACLs but with names not ids
        my $grp = $macOSGroupFromGidSingleton{$rgid} ||=
          `dscl . -search /Groups PrimaryGroupID $rgid`;
        $grp =~ s/\t.*//s if $grp;
        if ($grp) {
            my @aclargs = ( qw(/bin/chmod +a), "group:$grp allow read" );
            $allowGroupReadACL = sub { !system @aclargs, $_[0]; };
        }
    }

    sub {
        my ( $name, $forceReadOnlyTimeLimit ) = @_;
        my @stat = lstat $name or return;
        $stat[STAT_CHMODDED] = 0;
        return @stat unless -d _ || -f _ && -s _;
        my $readOnlyFile =
             -f _
          && !( $stat[STAT_MODE] & 022 )
          && !( $stat[STAT_UID] && ( $stat[STAT_MODE] & 0200 ) );

        # Categorisation of gids we might encounter when scanning:
        # 775: files with this gid are world readable.
        # 431: we can read files with this gid.
        # 279: we may take over files with this gid.
        # undefined or zero: we know nothing about this gid.
        my $groupStatus = $map{ $stat[STAT_GID] } || 0;

        if (    $groupStatus < 400
            and $groupStatus == 279
            || !$readOnlyFile
            || !$allowGroupReadACL->( $name, $stat[STAT_DEV] )
            and chown( -1, $rgid, $name ) )
        {
            $stat[STAT_GID]      = $rgid;
            $stat[STAT_CHMODDED] = 1;
            $groupStatus         = 431;
        }
        if (   -f _
            && -s _
            && !$readOnlyFile
            && defined $forceReadOnlyTimeLimit
            && $forceReadOnlyTimeLimit > $stat[STAT_MTIME] )
        {
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
            $rwx2 += 070        if $groupStatus && ( $rwx1 & 070 );
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
    }
}

1;
