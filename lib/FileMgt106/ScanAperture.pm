package FileMgt106::ScanAperture;

=head Copyright licence and disclaimer

Copyright 2011-2016 Franck Latrémolière.

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

use constant {
    LIB_DIR             => 0,
    LIB_MTIME           => 1,
    LIB_RGID            => 2,
    LIB_SCALAR          => 3,
    LIB_STARS_UUID      => 4,
    LIB_MASTER_METADATA => 5,
    LIB_KEYWORDS_UUID   => 6,
    LIB_JBZ             => 7,
};

sub new {
    my $class = shift;
    bless [@_], $class;
}

sub libraries {
    my $class   = shift;
    my $hints   = shift;
    my @liblist = map { /\.aplibrary(\/*|\.jbz)$/s ? decode_utf8($_) : (); } @_;
    foreach my $lib ( grep { s/\.jbz$//; } @liblist ) {
        my @stat = stat "$lib.jbz";
        unless (@stat) {
            warn "Ignored: $lib.jbz";
            next;
        }
        eval {
            unless ( -d $lib ) {
                mkdir $lib or die "Failed to mkdir $lib";
            }
            require FileMgt106::Tools;
            my $target = FileMgt106::Tools::loadNormalisedScalar("$lib.jbz");
            delete $target->{$_} foreach grep { /\//; } keys %$target;
            $hints->beginInteractive;
            FileMgt106::Scanner->new( $lib, $hints,
                $hints->statFromGid( $stat[STAT_GID] ) )->scan( 0, $target );
            $hints->commit;
            utime time, $stat[STAT_MTIME], $lib;
        };
        warn "Failed to rebuild $lib: $@" if $@;
    }
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
    my ( $lib, $startFolder, $jbzDir ) = @_;
    $jbzDir ||= getcwd();
    chdir $startFolder if $startFolder;
    chdir $lib->[LIB_DIR] or return;
    my $jbz = $lib->[LIB_DIR] = decode_utf8 getcwd();
    require Digest::SHA;
    my $shapp = ' ' . substr( Digest::SHA::sha1_hex($jbz), 0, 6 );
    $jbz =~ s#.*/##gs;
    $jbz = 'long name.aplibrary' if length( encode_utf8 $jbz) > 63;
    $jbz .= $shapp unless $jbz =~ s/\.aplibrary/$shapp/;
    $lib->[LIB_JBZ] = $jbz = "$jbzDir/$jbz";
    my @stat = stat "$jbz.aplibrary.jbz";
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
    my $repairer;
    $repairer = sub {
        foreach (@_) {
            my @stat = lstat $_;
            if ( -d _ ) {
                chdir $_ or next;
                opendir DIR, '.';
                my @list = grep { !/^\.\.?$/s; } readdir DIR;
                closedir DIR;
                $repairer->(@list);
                chdir '..';
            }
            elsif ( -f _ ) {
                if ( $stat[STAT_NLINK] > 1 ) {
                    system 'cp', '-p', '--', $_,
                      'aperture_repair_permissions_temporary';
                    unless ( rename 'aperture_repair_permissions_temporary',
                        $_ )
                    {
                        warn "Cannot rename to $_ in $lib->[LIB_DIR]";
                        next;
                    }
                    @stat = lstat $_;
                }
                if ( $stat[STAT_NLINK] > 1 ) {
                    warn "$_ still multilinked in $lib->[LIB_DIR]";
                    next;
                }
                chown -1, $rgid, $_ unless $rgid == $stat[STAT_GID];
                chmod 0660, $_ unless $stat[STAT_MODE] & 0660 == 0660;
            }
        }
    };
    opendir DIR, '.';
    my @list = grep { !/^\.\.?|Masters$/s; } readdir DIR;
    closedir DIR;
    $repairer->(@list);
    $lib;
}

sub scan {
    my ( $lib, $hints ) = @_;
    my $stat = $hints->statFromGid( $lib->[LIB_RGID] );
    $hints->beginInteractive;
    warn "Scanning $lib->[LIB_DIR]/Masters";
    FileMgt106::Scanner->new( "$lib->[LIB_DIR]/Masters", $hints, $stat )
      ->scan( time - 27 )
      if -d "$lib->[LIB_DIR]/Masters";
    warn "Scanning $lib->[LIB_DIR]";
    $lib->[LIB_SCALAR] =
      FileMgt106::Scanner->new( $lib->[LIB_DIR], $hints, $stat )->scan(0);
    $hints->commit;
    $lib;
}

sub extractStarRatings {
    my ($self) = @_;
    return unless -s "$self->[LIB_DIR]/Database/apdb/Library.apdb";
    my $lib = DBI->connect(
        "dbi:SQLite:dbname=$self->[LIB_DIR]/Database/apdb/Library.apdb",
        '', '', { sqlite_unicode => 0, AutoCommit => 1, } );
    unless ($lib) {
        warn "Cannot open $self->[LIB_DIR]/Database/apdb/Library.apdb";
        return;
    }
    my ( %starsVersion, %starsMaster );
    foreach (
        @{
            $lib->selectall_arrayref(
                    'select uuid, mainRating, isOriginal '
                  . 'masterUuid, rawMasterUuid, nonRawMasterUuid '
                  . ' from RKVersion'
            )
        }
      )
    {
        my ( $v, $stars, $isOriginal, @masters ) = @$_;
        next unless $stars || !$isOriginal;
        $stars ||= 0;
        $starsVersion{$v} = $stars;
        foreach my $m ( grep { $_ } @masters ) {
            $starsMaster{$m} = $stars
              unless $starsMaster{$m} && $starsMaster{$m} > $stars;
        }
    }
    $self->[LIB_STARS_UUID] = { %starsVersion, %starsMaster };
    my %keywords;
    foreach (
        @{
            $lib->selectall_arrayref(
                    'select RKKeyword.name,'
                  . ' RKVersion.uuid,'
                  . ' RKVersion.masterUuid,'
                  . ' RKVersion.rawMasterUuid,'
                  . ' RKVersion.nonRawMasterUuid'
                  . ' from RKKeywordForVersion, RKVersion, RKKeyword'
                  . ' where RKVersion.modelId = RKKeywordForVersion.versionId'
                  . ' and RKKeyword.modelId = RKKeywordForVersion.keywordId'
                  . ' order by RKKeyword.name'
            )
        }
      )
    {
        my ( $key, @uuids ) = @$_;
        my %uuids = map { ( $_ => undef ); } grep { $_ } @uuids;
        push @{ $keywords{$_} }, $key foreach keys %uuids;
    }
    $self->[LIB_KEYWORDS_UUID] = \%keywords;
    my %masters;
    foreach (
        @{ $lib->selectall_arrayref('select uuid, imagePath from RKMaster') } )
    {
        # This goes wrong if there are masters outside the .aplibrary folder
        my ( $m, $path ) = @$_;
        my ( $a, $b, $c, $d, $e ) = split m#/#, $path;
        $masters{$a}{$b}{$c}{$d}{$e} =
          [ $starsMaster{$m}, $keywords{$m} ? @{ $keywords{$m} } : () ];
    }
    $self->[LIB_MASTER_METADATA] = \%masters;
    $lib;
}

sub getFilteredScalar {

    my ( $lib, $minStars, $maxStars, $mainOnly ) = @_;

    my $filter = sub {
        my ( $k, $o ) = @_;
        return unless $o;
        my $n;
        foreach my $a ( keys %$o ) {
            foreach my $b ( keys %{ $o->{$a} } ) {
                foreach my $c ( keys %{ $o->{$a}{$b} } ) {
                    foreach my $d ( keys %{ $o->{$a}{$b}{$c} } ) {
                        foreach my $e ( keys %{ $o->{$a}{$b}{$c}{$d} } ) {
                            my $s = $lib->[LIB_STARS_UUID]{$e} || 0;
                            $n->{$a}{$b}{$c}{$d}{$e} = $o->{$a}{$b}{$c}{$d}{$e}
                              if defined $s
                              && $s >= $minStars
                              && $s <= $maxStars;
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
                              $lib->[LIB_MASTER_METADATA]{$a}{$b}{$c}{$d}{$e}
                              [0];
                            $n->{$a}{$b}{$c}{$d}{$e} = $o->{$a}{$b}{$c}{$d}{$e}
                              if defined $s
                              && $s >= $minStars
                              && $s <= $maxStars;
                        }
                    }
                }
            }
        }
        $n ? ( $k => $n ) : ();
    };

    $minStars
      ? {
        'Aperture.aplib' => $lib->[LIB_SCALAR]{'Aperture.aplib'},
        'Info.plist'     => $lib->[LIB_SCALAR]{'Info.plist'},
        Database         => {
            apdb => {},
            $filter->( Versions => $lib->[LIB_SCALAR]{Database}{Versions} ),
            map {
                $lib->[LIB_SCALAR]{Database}{$_}
                  ? ( $_ => $lib->[LIB_SCALAR]{Database}{$_} )
                  : ();
              } qw(DataModelVersion.plist KeywordSets.plist Keywords.plist
              Albums Faces Folders)
        },
        $filter->( Previews => $lib->[LIB_SCALAR]{Previews} ),
        $filterM->( Masters => $lib->[LIB_SCALAR]{Masters} ),
      }
      : {
        %{ $lib->[LIB_SCALAR] },
        $filter->( Previews => $lib->[LIB_SCALAR]{Previews} ),
        $filterM->( Masters => $lib->[LIB_SCALAR]{Masters} ),
      };

}

sub updateJbz {
    my ( $lib, $hints, $startFolder, $jbzDir ) = @_;
    return
      unless defined $lib->[LIB_JBZ]
      || $lib->setPathsCheckUpToDate( $startFolder, $jbzDir );
    $lib->repairPermissions;
    $lib->scan($hints);
    $lib->extractStarRatings;
    my $jbz = {
        %{ $lib->[LIB_SCALAR] },
        '/LIB_DIR'             => $lib->[LIB_DIR],
        '/LIB_KEYWORDS_UUID'   => $lib->[LIB_KEYWORDS_UUID],
        '/LIB_MASTER_METADATA' => $lib->[LIB_MASTER_METADATA],
        '/LIB_STARS_UUID'      => $lib->[LIB_STARS_UUID],
    };
    require FileMgt106::Tools;
    FileMgt106::Tools::saveJbzPretty( $lib->[LIB_JBZ] . $$, $jbz );
    rename $lib->[LIB_JBZ] . $$, "$lib->[LIB_JBZ].aplibrary.jbz";

    foreach ( [ 0, 5 ], [ 4, 5 ] ) {
        FileMgt106::Tools::saveJbzPretty( $lib->[LIB_JBZ] . $$,
            $lib->getFilteredScalar(@$_) );
        rename $lib->[LIB_JBZ] . $$,
          $lib->[LIB_JBZ] . '.' . join( 'to', @$_ ) . '.aplibrary.jbz';
    }

}

1;
