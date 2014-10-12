package FileMgt106::ScanAperture;

=head Copyright licence and disclaimer

Copyright 2011-2014 Franck Latrémolière.

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
use Cwd;
use Encode qw(decode_utf8 encode_utf8);
use FileMgt106::Scanner;
use FileMgt106::FileSystem;
use FileMgt106::Permissions;

use constant {
    LIB_DIR           => 0,
    LIB_MTIME         => 1,
    LIB_RGID          => 2,
    LIB_SCALAR        => 3,
    LIB_STARS_UUID    => 4,
    LIB_STARS_MASTERS => 5,
    LIB_JBZ           => 6,
};

sub new {
    my $class = shift;
    bless [@_], $class;
}

sub libraries {
    my $class   = shift;
    my $hints   = shift;
    my @liblist = map { /\.aplibrary\/*$/s ? decode_utf8($_) : (); } @_;
    unless (@liblist) {
        my $path;
        my %paths;
        warn 'Looking for Aperture libraries';
        $hints->beginInteractive;
        my $q =
          $hints->{dbHandle}
          ->prepare('select name, parid from locations where locid=?');
        $path = sub {
            my ($locid) = @_;
            return $paths{$locid} if exists $paths{$locid};
            $q->execute($locid);
            my ( $name, $parid ) = $q->fetchrow_array;
            return $paths{$locid} = undef unless defined $parid;
            return $paths{$locid} = $name unless $parid;
            my $p = $path->($parid);
            return $paths{$locid} = undef unless defined $p;
            $paths{$locid} = "$p/$name";
        };
        @liblist =
          map { defined $_ ? decode_utf8($_) : (); }
          map { $path->( $_->[0] ); } @{
            $hints->{dbHandle}->selectall_arrayref(
                'select locid from locations where name like "%.aplibrary"')
          };
        $q->finish;
        $hints->commit;
    }
    map {
        my @t = stat "$_/Database/apdb";
        @t ? $class->new( $_, $t[STAT_MTIME] ) : ();
    } @liblist;
}

sub setPathsCheckUpToDate {
    require Digest::SHA;
    my ( $lib, $startDir, $jbzDir ) = @_;
    $jbzDir ||= getcwd();
    chdir $startDir if $startDir;
    chdir $lib->[LIB_DIR] or return;
    my $jbz = $lib->[LIB_DIR] = decode_utf8 getcwd();
    my $shapp = ' ' . Digest::SHA::sha1_hex($jbz);
    $jbz =~ s#.*/##gs;
    $jbz = 'long name.aplibrary' if length( encode_utf8 $jbz) > 63;
    $jbz .= $shapp unless $jbz =~ s/\.aplibrary/$shapp.aplibrary/;
    $lib->[LIB_JBZ] = $jbz = "$startDir/$jbz.jbz";
    my @stat = stat $jbz;
    @stat and $stat[STAT_MTIME] > $lib->[LIB_MTIME] and return;
    $lib;
}

sub repairPermissions {
    my ($lib) = @_;
    unless ( chdir $lib->[LIB_DIR] ) {
        warn die "Cannot chdir $lib->[LIB_DIR]";
        return;
    }
    my $rgid = $lib->[LIB_RGID] = ( stat '.' )[STAT_GID];
    foreach (
        <*.xml>,                  <*.plist>,
        <Aperture.aplib/*.plist>, <Database/*.plist>,
        <Database/apdb/*>
      )
    {
        my @stat = lstat $_;
        next unless -f _;
        if ( $stat[STAT_NLINK] > 1 ) {
            system 'cp', '-p', '--', $_, 'aperturescantmp';
            unless ( rename 'aperturescantmp', $_ ) {
                warn "Cannot rename aperturescantmp to $_ in $lib->[LIB_DIR]";
                next;
            }
            @stat = lstat $_;
        }
        if ( $stat[STAT_NLINK] > 1 ) {
            warn "$_ still multilinked in $lib->[LIB_DIR]";
            next;
        }
        chown -1, $rgid, $_ unless $rgid == $stat[STAT_GID];
        chmod 0660, $_ unless $stat[STAT_MODE] & 020;
    }
    $lib;
}

sub scan {
    my ( $lib, $hints ) = @_;
    my $stat = statFromGid( $lib->[LIB_RGID] );
    $hints->beginInteractive;
    warn "Scanning $lib->[LIB_DIR]/Masters";
    FileMgt106::Scanner->new( "$lib->[LIB_DIR]/Masters", $hints, $stat )
      ->scan( time - 17 )
      if -d "$lib->[LIB_DIR]/Masters";
    warn "Scanning $lib->[LIB_DIR]";
    $lib->[LIB_SCALAR] =
      FileMgt106::Scanner->new( $lib->[LIB_DIR], $hints, $stat )->scan(0);
    $hints->commit;
    $lib;
}

sub stars {
    my ($self) = @_;
    my $lib = DBI->connect(
        "dbi:SQLite:dbname=$self->[LIB_DIR]/Database/apdb/Library.apdb",
        '', '', { sqlite_unicode => 0, AutoCommit => 0, } );
    unless ($lib) {
        warn "Cannot open $self->[LIB_DIR]/Database/apdb/Library.apdb";
        return;
    }
    my ( %starsVersion, %starsMaster );
    foreach (
        @{
            $lib->selectall_arrayref(
                    'select uuid, '
                  . 'masterUuid, rawMasterUuid, nonRawMasterUuid, '
                  . 'mainRating from RKVersion where mainRating'
            )
        }
      )
    {
        my ( $v, $m, $rm, $jm, $stars ) = @$_;
        $starsVersion{$v} = $stars;
        $starsMaster{$m}[0] = $stars
          unless $starsMaster{$m}[0] && $starsMaster{$m}[0] > $stars;
        $starsMaster{$rm}[1] = $stars
          unless !$rm
          || $starsMaster{$rm}[1] && $starsMaster{$rm}[1] > $stars;
        $starsMaster{$jm}[2] = $stars
          unless !$jm
          || $starsMaster{$jm}[2] && $starsMaster{$jm}[2] > $stars;
    }
    $self->[LIB_STARS_UUID] = { %starsVersion, %starsMaster };
    my %masters;
    foreach (
        @{ $lib->selectall_arrayref('select uuid, imagePath from RKMaster') } )
    {
        my ( $m, $path ) = @$_;
        my ( $a, $b, $c, $d, $e ) = split m#/#, $path;
        $masters{$a}{$b}{$c}{$d}{$e} = $starsMaster{$m};
    }
    $self->[LIB_STARS_MASTERS] = \%masters;
    $lib;
}

sub updateJbz {
    my ( $lib, $hints, $startDir, $jbzDir ) = @_;
    return
      unless defined $lib->[LIB_JBZ]
      || $lib->setPathsCheckUpToDate( $startDir, $jbzDir );
    $lib->repairPermissions;
    $lib->scan($hints);
    $lib->stars;
    my $jbz = {
        %{ $lib->[LIB_SCALAR] },
        '/LIB_DIR'           => $lib->[LIB_DIR],
        '/LIB_STARS_MASTERS' => $lib->[LIB_STARS_MASTERS],
        '/LIB_STARS_UUID'    => $lib->[LIB_STARS_UUID],
    };
    require FileMgt106::Tools;
    FileMgt106::Tools::saveJbzPretty( $lib->[LIB_JBZ] . $$, $jbz );
    rename $lib->[LIB_JBZ] . $$, $lib->[LIB_JBZ];
}

sub scalar {

    my ( $lib, $minStars, $mainOnly ) = @_;

    my $filter = sub {
        my ( $k, $o ) = @_;
        return unless $o;
        my $n;
        foreach my $a ( keys %$o ) {
            foreach my $b ( keys %{ $o->{$a} } ) {
                foreach my $c ( keys %{ $o->{$a}{$b} } ) {
                    foreach my $d ( keys %{ $o->{$a}{$b}{$c} } ) {
                        foreach my $e ( keys %{ $o->{$a}{$b}{$c}{$d} } ) {
                            my $s = $lib->[LIB_STARS_UUID]{$e};
                            $n->{$a}{$b}{$c}{$d}{$e} = $o->{$a}{$b}{$c}{$d}{$e}
                              if defined $s && $s >= $minStars;
                        }
                    }
                }
            }
        }
        $n ? ( $k => $n ) : ();
    };

    my $filterM = sub {
        my ( $k, $o ) = @_;
        return unless $o;
        my $n;
        foreach my $a ( keys %$o ) {
            foreach my $b ( keys %{ $o->{$a} } ) {
                foreach my $c ( keys %{ $o->{$a}{$b} } ) {
                    foreach my $d ( keys %{ $o->{$a}{$b}{$c} } ) {
                        foreach my $e ( keys %{ $o->{$a}{$b}{$c}{$d} } ) {
                            my $s =
                              $lib->[LIB_STARS_MASTERS]{$a}{$b}{$c}{$d}{$e};
                            $n->{$a}{$b}{$c}{$d}{$e} = $o->{$a}{$b}{$c}{$d}{$e}
                              if $s
                              and defined $s->[0] && $s->[0] >= $minStars
                              || !$mainOnly
                              && ( defined $s->[1] && $s->[1] >= $minStars
                                || defined $s->[2] && $s->[2] >= $minStars );
                        }
                    }
                }
            }
        }
        $n ? ( $k => $n ) : ();
    };

    {
        'Aperture.aplib' => $lib->[LIB_SCALAR]{'Aperture.aplib'},
        'Info.plist'     => $lib->[LIB_SCALAR]{'Info.plist'},
        Database         => {
            $filter->( Versions => $lib->[LIB_SCALAR]{Database}{Versions} ),
            map {
                $lib->[LIB_SCALAR]{Database}{$_}
                  ? ( $_ => $lib->[LIB_SCALAR]{Database}{$_} )
                  : ();
            } qw(DataModelVersion.plist Folders KeywordSets.plist Keywords.plist),
        },
        $filter->( Previews => $lib->[LIB_SCALAR]{Previews} ),
        $filterM->( Masters => $lib->[LIB_SCALAR]{Masters} ),
    };

}

1;
