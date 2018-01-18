package FileMgt106::Database;

=head Copyright licence and disclaimer

Copyright 2011-2018 Franck Latrémolière, Reckon LLP.

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

=head Interface

my $hints = FileMgt106::Database->new( $sqliteFilePath );

$hints->beginInteractive;
$hints->commit;

$hints->enqueue( $queue, sub { my ($hints) = @_; } );
$hints->dequeued($runner);

my $locid = $hints->{folder}->( $parid, $name, $dev, $ino );
# {folder} will automatically duplicate and uproot trees for a folder found by dev/ino.

my $locid = $hints->{topFolder}->( $path, $dev, $ino );
# {topFolder} works like {folder} to find a tree root.

my ( $locid, $sha1, $looksChanged ) = $hints->{file}->( $parid, $name, $dev, $ino, $size, $mtime );
# $sha1 will only be set if dev/ino/size/mtime passed as arguments agreed with the database,
# possibly on a record duplicated from something found by dev/ino from another parid/name.

$hints->{updateSha1}->( $sha1, $locid );
$hints->{updateLocation}->( $dev, $ino, $size, $mtime, $locid );

my $iterator = $hints->{searchSha1}->( $sha1, $dev, $inoAvoid, $inoMaxFlag );
while ( my ( $path, $statref ) = $iterator->() ) { }
# $dev argument is required; if no $inoAvoid then $dev is only a preference; otherwise it is a requirement.

my ($newName, $newFullPath) = $hints->{findName}->( $parid, $name, $path );

my $nameToLocidHashref = $hints->{children}->( $locid );
foreach my $locid ( values %$nameToLocidHashref ) { $hints->{uproot}->( $locid ); }
$hints->{moveByParidName}->( $newparid, $newname, $oldparid, $oldname );
$hints->{moveByLocid}->( $newparid, $newname, $oldlocid );

=cut

use strict;
use warnings;
use utf8;
use Encode qw(decode_utf8);
use DBI;
use File::Spec::Functions qw(abs2rel);

use constant {
    STAT_DEV   => 0,
    STAT_INO   => 1,
    STAT_GID   => 5,
    STAT_SIZE  => 7,
    STAT_MTIME => 9,
};

sub gidInfo {
    my ($self) = @_;
    return $self->{gidInfo} if $self->{gidInfo};
    my $file = $self->{sqliteFile} . '-gidInfo.txt';
    my $fh;
    if ( open $fh, '<', $file ) {
        binmode $fh, ':utf8';
        local $/ = "\n";
        while (<$fh>) {
            next unless my ( $k, $v ) = /(\S+):(.+)/;
            $self->{gidInfo}{$k} =
              $v =~ /^([0-9,]+)\s*$/s ? [ split /,/, $1 ] : $v;
        }
        close $fh;
    }
    unless ( $self->{gidInfo} ) {
        $self->{gidInfo} = {
            6    => 'imap',
            1037 => 'mgt',
            1030 => 'world',
            1025 => [qw(1026 1028 1029 1032 1034 1037 1038)],
            1026 => [qw(1028 1029 1032 1034 1037)],
            1066 => [qw(1028)],
            1069 => [qw(1028 1066)],
        };
        if ( open $fh, '>', $file . $$ ) {
            binmode $fh, ':utf8';
            print $fh "$_:"
              . (
                ref $self->{gidInfo}{$_}
                ? join( ',', @{ $self->{gidInfo}{$_} } )
                : $self->{gidInfo}{$_}
              )
              . "\n"
              foreach sort { $a <=> $b } keys %{ $self->{gidInfo} };
            close $fh;
            rename $file . $$, $file;
        }
    }
    $self->{gidInfo};
}

sub statFromGid {

    my ( $self, $rgid ) = @_;
    return unless $rgid;
    my $gidInfo = $self->gidInfo;

    my $myInfo = $gidInfo->{$rgid} || '';
    return FileMgt106::FileSystem::managementStat($rgid) if $myInfo eq 'mgt';
    return FileMgt106::FileSystem::imapStat($rgid)       if $myInfo eq 'imap';
    return FileMgt106::FileSystem::publishedStat($rgid)  if $myInfo eq 'world';

    # Categorisation system for gids:
    # 775 = files with this gid are world readable.
    # 431 = we can read files with this gid.
    # 279 = we may take over files with this gid.
    # otherwise we know nothing about this gid.
    my %map = ( $rgid => 431 );
    if ( ref $myInfo eq 'ARRAY' ) {
        $map{$_} = 279 foreach @$myInfo;
    }
    while ( my ( $gid, $info ) = each %$gidInfo ) {
        if ( ref $info eq 'ARRAY' ) {
            $map{$gid} = 431 if grep { $rgid == $_ } @$info;
        }
        elsif ( $info eq 'mgt' ) {
            $map{$gid} = 279;
        }
        elsif ( $info eq 'world' ) {
            $map{$gid} = 775;
        }
    }

    FileMgt106::FileSystem::statFromGidAndMapping( $rgid, \%map );

}

sub new {

    my ( $self, $sqliteFile, $readOnly ) = @_;
    $self = bless {}, $self unless ref $self;
    $self->{sqliteFile} = $sqliteFile;
    my $writeable = !$readOnly && ( -w $sqliteFile || !-e $sqliteFile );

    my $makeNewHandle = sub {
        $self->{dbHandle} = DBI->connect(
            'dbi:SQLite:dbname='
              . ( -l $sqliteFile ? readlink $sqliteFile : $sqliteFile ),
            '',
            '',
            { sqlite_unicode => 0, AutoCommit => !$writeable, }
        ) or die "Cannot open SQLite database $sqliteFile";
    };

    my $dbHandle = $makeNewHandle->();

    my %rootidFromDev;
    if ($writeable) {
        $dbHandle->sqlite_busy_timeout(600);    # 600 ms instead of default 30 s
        _setupTables($dbHandle);
    }
    _setSQLitePragmas($dbHandle);

    my (
        $qGetLocid,                $qGetLocidRootidIno,
        $qGetLocidByNameRootidIno, $qGetLocation,
        $qChangeRootid,            $qChangeIno,
        $qInsertLocid,             $qInsertLocation,
        $qGetChildren,             $qUpdateLocation,
        $qUpdateSha1,              $qUpdateSha1if,
        $qGetBySha1,               $qGetBySha1Rootid,
        $qGetBySha1InoAvoid,       $qGetBySha1InoMax,
        $qGetParidName,            $qGetLikeDesc,
        $qUproot,                  $qMoveByParidName,
        $qMoveByLocid,             $qClone,
        $qAlreadyThere,
    );
    my $prepareStatements = sub {
        (
            $qGetLocid,                $qGetLocidRootidIno,
            $qGetLocidByNameRootidIno, $qGetLocation,
            $qChangeRootid,            $qChangeIno,
            $qInsertLocid,             $qInsertLocation,
            $qGetChildren,             $qUpdateLocation,
            $qUpdateSha1,              $qUpdateSha1if,
            $qGetBySha1,               $qGetBySha1Rootid,
            $qGetBySha1InoAvoid,       $qGetBySha1InoMax,
            $qGetParidName,            $qGetLikeDesc,
            $qUproot,                  $qMoveByParidName,
            $qMoveByLocid,             $qClone,
            $qAlreadyThere,
          )
          = map { my $q; sleep 1 while !( $q = $dbHandle->prepare($_) ); $q; }
          split /\n/, <<EOL;
select locid from locations where parid=? and name=?
select locid, rootid, ino from locations where parid=? and name=?
select locid from locations where name=? and rootid=? and ino=?
select locid, rootid, ino, size, mtime, sha1 from locations where parid=? and name=?
update or replace locations set rootid=? where locid=?
update or replace locations set ino=? where locid=?
insert or replace into locations (parid, name, rootid, ino) values (?, ?, ?, ?)
insert or replace into locations (parid, name, rootid, ino, size, mtime) values (?, ?, ?, ?, ?, ?)
select locid, name, sha1 from locations where parid=?
update or replace locations set rootid=?, ino=?, size=?, mtime=? where locid=?
update locations set sha1=? where locid=?
update locations set sha1=? where ( sha1 is null or sha1<>? ) and locid=?
select locid, parid, name, rootid, ino, size, mtime from locations where sha1=? order by rootid=? desc
select locid, parid, name, rootid, ino, size, mtime from locations where sha1=? and rootid=?
select locid, parid, name, rootid, ino, size, mtime from locations where sha1=? and rootid=? and ino<>?
select locid, parid, name, rootid, ino, size, mtime from locations where sha1=? and rootid=? and ino<?
select parid, name from locations where locid=?
select name from locations where parid=? and name like ? order by name desc
update locations set parid=null where locid=?
update or replace locations set parid=?, name=? where parid=? and name=?
update or replace locations set parid=?, name=? where locid=?
insert or replace into locations (parid, name, rootid, ino, size, mtime, sha1) select ?, name, rootid, ino, size, mtime, case when size is null then null else sha1 end from locations where locid=?
select 1 from locations where parid=? and sha1=? and (name=? or name like ?)
EOL
    };

    $prepareStatements->();

    my $needsNap;
    my $nap = sub {
        $self->commit;
        undef $needsNap;
        warn 'Commit done in ' . `pwd`;
        sleep 4;
        $self->beginInteractive;
    };
    $self->{scheduleNap} = sub { $needsNap = $nap; };
    $self->{cleanup} = sub {
        undef $needsNap;
        undef $_
          foreach $qGetLocid, $qGetLocidRootidIno,
          $qGetLocidByNameRootidIno, $qGetLocation,
          $qChangeRootid,            $qChangeIno,
          $qInsertLocid,             $qInsertLocation,
          $qGetChildren,             $qUpdateLocation,
          $qUpdateSha1,              $qUpdateSha1if,
          $qGetBySha1,               $qGetBySha1Rootid,
          $qGetBySha1InoAvoid,       $qGetBySha1InoMax,
          $qGetParidName,            $qGetLikeDesc,
          $qUproot,                  $qMoveByParidName,
          $qMoveByLocid,             $qClone,
          $qAlreadyThere;
        $dbHandle->disconnect;
        undef $dbHandle;
    };

    my $file = $self->{file} = sub {
        my ( $parid, $name, $dev, $ino, $size, $mtime ) = @_;
        $needsNap->() if $needsNap;
        my $rootid = $rootidFromDev{$dev};
        die "Device $dev not known" unless defined $rootid;
        $qGetLocation->execute( $parid, $name );
        my ( $locid, $lrootid, $lino, $lsize, $lmtime, $sha1 ) =
          $qGetLocation->fetchrow_array;
        $qGetLocation->finish;
        if ( !defined $locid && $ino ) {
            $qGetLocidByNameRootidIno->execute( $name, $rootid, $ino );
            if ( my ($locidToClone) =
                $qGetLocidByNameRootidIno->fetchrow_array )
            {
                $qGetLocidByNameRootidIno->finish;
                $qClone->execute( $parid, $locidToClone );
                $qGetLocation->execute( $parid, $name );
                ( $locid, $lrootid, $lino, $lsize, $lmtime, $sha1 ) =
                  $qGetLocation->fetchrow_array;
            }
        }
        unless ( defined $locid ) {
            $qInsertLocation->execute( $parid, $name, $rootid, $ino, $size,
                $mtime )
              or die "Could not insert location: $parid $name";
            $qGetLocid->execute( $parid, $name );
            ($locid) = $qGetLocid->fetchrow_array;
            $qGetLocid->finish;
            return ( $locid, undef, 1 );
        }
        return ( $locid, $sha1 )
          if !$ino
          || defined $lsize
          && defined $sha1
          && $lrootid == $rootid
          && $lino == $ino
          && $lsize == $size
          && $lmtime == $mtime;
        $qUpdateLocation->execute( $rootid, $ino, $size, $mtime, $locid );
        return ( $locid, $sha1, 1 );
    };

    my $clone;
    $clone = sub {
        my ( $locid, $parid, $name ) = @_;
        $qClone->execute( $parid, $locid );
        $qGetLocid->execute( $parid, $name );
        my ($cloneLocid) = $qGetLocid->fetchrow_array;
        $qGetLocid->finish;
        $qGetChildren->execute($locid);
        $clone->( $_->[0], $cloneLocid, $_->[1] )
          foreach @{ $qGetChildren->fetchall_arrayref };
    };

    $self->{checkFolder} = sub {
        my ( $parid, $name, $dev, $ino ) = @_;
        my $rootid = $parid ? $rootidFromDev{$dev} : $dev;
        die "Device $dev not known" unless defined $rootid;
        $qGetLocidRootidIno->execute( $parid, $name );
        my ( $locid, $rootiddb, $inodb ) = $qGetLocidRootidIno->fetchrow_array
          or return;
        $qGetLocidRootidIno->finish;
        return unless $rootiddb && $rootid && $rootiddb == $rootid;
        return unless $inodb && $ino == $inodb;
        $locid;
    };

    my $folder = $self->{folder} = sub {
        my ( $parid, $name, $dev, $ino ) = @_;
        my $rootid = $parid ? $rootidFromDev{$dev} : $dev;
        die "Device $dev not known" unless defined $rootid;
        $qGetLocidRootidIno->execute( $parid, $name );
        if ( my ( $locid, $rootiddb, $inodb ) =
            $qGetLocidRootidIno->fetchrow_array )
        {
            $qGetLocidRootidIno->finish;
            $qChangeRootid->execute( $rootid, $locid )
              unless $rootiddb && $rootid && $rootiddb == $rootid;
            $qChangeIno->execute( $ino, $locid )
              unless $inodb && $ino == $inodb;
            $rootidFromDev{$dev} = $locid unless $parid;
            return $locid;
        }
        $qGetLocidByNameRootidIno->execute( $name, $rootid, $ino );
        if ( my ($locidToClone) = $qGetLocidByNameRootidIno->fetchrow_array ) {
            $qGetLocidByNameRootidIno->finish;
            $clone->( $locidToClone, $parid, $name );
            $qUproot->execute($locidToClone);
        }
        else {
            $qInsertLocid->execute( $parid, $name, $rootid, $ino );
        }
        $qGetLocid->execute( $parid, $name );
        my ($locid) = $qGetLocid->fetchrow_array;
        $qGetLocid->finish;
        $rootidFromDev{$dev} = $locid unless $parid;
        $locid;
    };

    my $topFolder;
    $topFolder = $self->{topFolder} = sub {
        my ( $path, $dev, $ino ) = @_;
        $qGetLocid->execute( 0, $path );
        if ( !$qGetLocid->fetchrow_array
            && ( my ( $root, $seg ) = ( $path =~ m#^(.*?)/+([^/]*)/*$#s ) ) )
        {
            my @stat = lstat( $root eq '' ? '/' : $root );
            if ( @stat && $stat[STAT_DEV] == $dev ) {
                my ( $parid, $rootid, $rootname ) =
                  $topFolder->( $root, @stat[ STAT_DEV, STAT_INO ] );
                my $locid = $folder->( $parid, $seg, $dev, $ino );
                return wantarray ? ( $locid, $rootid, $rootname ) : $locid;
            }
        }
        $qGetLocid->finish;
        my $locid = $folder->( 0, $path, $dev, $ino );
        return wantarray ? ( $locid, $locid, $path ) : $locid;
    };

    {
        my $oldDbPath = sub {
            my $oldrootid = -1;
            my ( $parid, $name );
            my $oldpath = '';
            while (1) {
                $qGetParidName->execute($oldrootid);
                ( $parid, $name ) = $qGetParidName->fetchrow_array;
                $qGetParidName->finish;

                # commit or rollback needed here if no auto commit; not sure why
                $dbHandle->rollback if $writeable;

                last unless defined $parid;
                $oldpath = "$name/$oldpath";
                last unless $parid;
                $oldrootid = $parid;
            }
            chop $oldpath;
            $oldpath, $name, $oldrootid;
        };
        while (1) {
            my ( $sqlitePath, $mountPointPath, $mountPointId ) = $oldDbPath->();
            if ( $sqlitePath eq $sqliteFile ) {
                my $dev = ( stat( $mountPointPath ||= '/' ) )[STAT_DEV];
                $self->{canonicalPath} = sub {
                    my ($dir) = @_;
                    my @stat = stat $dir;
                    @stat && $stat[STAT_DEV] == $dev
                      ? abs2rel( $dir, $mountPointPath )
                      : $dir;
                };
                last;
            }
            warn $sqlitePath
              ? "Database has moved from $sqlitePath to $sqliteFile"
              : "New database at $sqliteFile";
            unless ($writeable) {
                warn "Cannot modify $sqliteFile, giving up";
                last;
            }
            $self->beginInteractive;
            my ( $oldpath, $oldrootname, $oldrootid ) = $oldDbPath->();
            if ( $oldpath eq $sqliteFile ) {
                warn "Someone else has recorded the move to $sqliteFile";
                last;
            }
            my ( $locid, $rootid, $rootname ) = $topFolder->(
                $sqliteFile, ( stat $sqliteFile )[ STAT_DEV, STAT_INO ]
            );
            if ( defined $oldrootname ) {
                warn "Replacing $oldrootname ($oldrootid)"
                  . " with $rootname ($rootid)";
                $dbHandle->do(
                    'update or replace locations set name=? where locid=?',
                    undef, $rootname, $oldrootid );
            }
            $dbHandle->do(
                'update or replace locations set locid=-1 where locid=?',
                undef, $locid );
            my $status;
            sleep 2 while !( $status = $dbHandle->commit );
        }
    }

    $self->{updateLocation} = sub {
        my ( $dev, $ino, $size, $mtime, $locid ) = @_;
        $qUpdateLocation->execute( $rootidFromDev{$dev},
            $ino, $size, $mtime, $locid );
    };

    $self->{updateSha1} = sub {
        $qUpdateSha1->execute(@_);
    };

    $self->{updateSha1if} = sub {
        $qUpdateSha1if->execute( @_[ 0, 0, 1 ] );
    };

    my $paridNameFromLocid = $self->{paridNameFromLocid} = sub {
        my ($locid) = @_;
        $qGetParidName->execute($locid);
        my @paridName = $qGetParidName->fetchrow_array;
        $qGetParidName->finish;
        @paridName;
    };

    $self->{pathFinderFactory} = sub {
        my $pathFinder;
        my %paths;
        $pathFinder = sub {
            my ($locid) = @_;
            return $paths{$locid} if exists $paths{$locid};
            my ( $parid, $name ) = $paridNameFromLocid->($locid);
            return $paths{$locid} = undef unless defined $parid;
            return $paths{$locid} = $name unless $parid;
            my $p = $pathFinder->($parid);
            return $paths{$locid} = undef unless defined $p;
            $paths{$locid} = "$p/$name";
        };
    };

    $self->{pathFromLocid} = sub {
        my ($locid) = @_;
        $qGetParidName->execute($locid);
        my ( $parid, $path ) = $qGetParidName->fetchrow_array;
        $qGetParidName->finish;
        return unless defined $path;
        while ($parid) {
            $qGetParidName->execute($parid);
            my ( $grandid, $parname ) = $qGetParidName->fetchrow_array;
            $qGetParidName->finish;
            return unless defined $parname;
            $path  = decode_utf8($parname) . '/' . $path;
            $parid = $grandid;
        }
        $path;
    };

    $self->{searchSha1} = sub {
        my ( $sha1, $dev, $inoAvoid, $inoMaxFlag ) = @_;
        foreach (
            @{
                $dbHandle->selectall_arrayref(
                    'select locid, name from locations where parid=0'
                )
            }
          )
        {
            my ( $locid, $name ) = @$_;
            $name = '/' if $name eq '';
            my @stat = stat $name or next;
            $rootidFromDev{ $stat[STAT_DEV] } ||= $locid;
        }
        my $rootid = $rootidFromDev{$dev};
        my $q;
        if ($inoMaxFlag) {
            ( $q = $qGetBySha1InoMax )->execute( $sha1, $rootid, $inoAvoid );
        }
        elsif ($inoAvoid) {
            ( $q = $qGetBySha1InoAvoid )->execute( $sha1, $rootid, $inoAvoid );
        }
        else {
            ( $q = defined $inoAvoid ? $qGetBySha1Rootid : $qGetBySha1 )
              ->execute( $sha1, $rootid );
        }
        my $a = $q->fetchall_arrayref;
        undef $q;
        sub {
          ITERATION: while (1) {
                return unless @$a;
                my ( $locid, $parid, $path, $rootid, $ino, $size, $mtime ) =
                  @{ shift @$a };
                $path = decode_utf8 $path;
                while ($parid) {
                    $qGetParidName->execute($parid);
                    my ( $grandid, $parname ) = $qGetParidName->fetchrow_array;
                    $qGetParidName->finish;
                    next ITERATION unless defined $parname;
                    $path  = decode_utf8($parname) . '/' . $path;
                    $parid = $grandid;
                }
                next unless defined $parid;
                next unless my @stat = lstat $path;
                next
                  unless my $drootid = $rootidFromDev{ $stat[STAT_DEV] };
                undef $locid
                  unless defined $size
                  && defined $rootid
                  && $rootid == $drootid
                  && $ino == $stat[STAT_INO]
                  && $size == $stat[STAT_SIZE]
                  && defined $mtime
                  && $mtime == $stat[STAT_MTIME];
                return wantarray ? ( $path, \@stat, $locid ) : $path;
            }
        };
    };

    $self->{alreadyThere} = sub {
        my ( $parid, $name, $sha1 ) = @_;
        my ( $base, $extension ) = ( $name =~ m#^(.*?)(\.[a-zA-Z]\S*)$#s );
        ( $base, $extension ) = ( $name, '' ) unless defined $extension;
        $qAlreadyThere->execute( $parid, $sha1, $name,
            $base . '~___' . $extension );
        my @r = $qAlreadyThere->fetchrow_array;
        $qAlreadyThere->finish;
        @r;
    };

    my $nextVersion = sub {
        my ( $parid, $name )      = @_;
        my ( $base,  $extension ) = ( $name =~ m#^(.*?)(\.[a-zA-Z]\S*)$#s );
        ( $base, $extension ) = ( $name, '' ) unless defined $extension;
        $name = '~001' . $extension unless length $base;
        $qGetLocid->execute( $parid, $name );
        return $name unless $qGetLocid->fetchrow_array;
        $qGetLocid->finish;
        $base .= '~';
        my $number = 2;
        $qGetLikeDesc->execute( $parid, "$base%$extension" );

        while ( my ($found) = $qGetLikeDesc->fetchrow_array ) {
            $found = decode_utf8 $found;
            next unless substr( $found, 0, length($base) ) eq $base;
            next
              unless substr( $found, length($found) - length($extension) ) eq
              $extension;
            $found =
              substr( $found, length($base),
                length($found) - length($base) - length($extension) );
            next unless $found =~ /^[0-9]+$/s;
            $number = $found + 1;
            last;
        }
        $qGetLikeDesc->finish;
        $base
          . ( $number < 10 ? '00' : $number < 100 ? '0' : '' )
          . $number
          . $extension;
    };

    $self->{findName} = sub {
        my ( $parid, $name, $path ) = @_;
        while (1) {
            my $newName = $nextVersion->( $parid, $name );
            my $newPath = "$path/$newName";
            return wantarray ? ( $newName, $newPath ) : $newName
              unless my @stat = lstat $newPath;
            -d _
              ? $folder->( $parid, $newName, @stat[ STAT_DEV, STAT_INO ] )
              : $file->(
                $parid, $newName,
                @stat[ STAT_DEV, STAT_INO, STAT_SIZE, STAT_MTIME ]
              );
        }
    };

    $self->{children} = sub {
        $qGetChildren->execute(@_);
        my %hash;
        while ( my ( $locid, $name ) = $qGetChildren->fetchrow_array ) {
            $hash{ decode_utf8($name) } = $locid;
        }
        \%hash;
    };

    $self->{childrenSha1} = sub {
        $qGetChildren->execute(@_);
        my %hash;
        while ( my ( undef, $name, $sha1 ) = $qGetChildren->fetchrow_array ) {
            $hash{ decode_utf8($name) } = $sha1;
        }
        \%hash;
    };

    $self->{uproot} = sub {
        $qUproot->execute(@_);
    };

    $self->{moveByParidName} = sub {
        $qMoveByParidName->execute(@_);
    };

    $self->{moveByLocid} = sub {
        $qMoveByLocid->execute(@_);
    };

    $self;

}

sub _setupTables {
    my ($dbHandle) = @_;
    do { sleep 1 while !$dbHandle->do($_); }
      foreach grep { $_ } split /;\s*/s, <<EOSQL;
create table if not exists locations (
	locid integer primary key,
	parid integer,
	name char,
	rootid integer,
	ino integer,
	size integer,
	mtime integer,
	sha1 text collate binary
);
create unique index if not exists locationsparidname on locations (parid, name);
create index if not exists locationssha1 on locations (sha1);
create index if not exists locationsnamerootidino on locations (name, rootid, ino);
commit;
EOSQL

}

sub _setSQLitePragmas {
    my ($dbHandle) = @_;
    do { $dbHandle->do($_); }
      foreach grep { $_ } split /;\s*/s, <<EOSQL;
pragma temp_store = memory;
pragma cache_size = 48000;
EOSQL
}

sub beginInteractive {
    my ( $self, $noAutoCommit ) = @_;
    my $dbHandle = $self->{dbHandle};
    alarm 0;
    my $timeout = 0;
    while (1) {
        $dbHandle->sqlite_busy_timeout( $timeout += 10_000 );
        last if $dbHandle->do('begin immediate transaction');
        my $eString = $dbHandle->errstr;
        die $eString unless $eString =~ /locked/;
    }
    return if $noAutoCommit;
    $SIG{ALRM} = $self->{scheduleNap};
    alarm 555;
}

sub enqueue {
    my $self  = shift;
    my $queue = shift;
    my $ttr   = time + 1;
    push @{ $self->{codequeue} }, @_;
    unless ( $self->{queue} && $self->{queue} == $queue ) {
        $self->{queue}->remove_item( $self->{qid}, sub { $_[0] == $self; } )
          if defined $self->{qid};
        $self->{queue} = $queue;
        delete $self->{qid};
    }
    if ( exists $self->{qid} ) {
        delete $self->{qid}
          unless $self->{ttr} <= $ttr || $self->{queue}->set_priority(
            $self->{qid},
            sub { $_[0] == $self },
            $self->{ttr} = $ttr
          );
    }
    $self->{qid} = $self->{queue}->enqueue( $self->{ttr} = $ttr, $self )
      unless exists $self->{qid};
}

sub dequeued {
    my ( $self, $runner ) = @_;
    delete $self->{qid};
    my $dbHandle = $self->{dbHandle};
    my $ttr      = time + 11;
    if ( $dbHandle->do('begin immediate transaction') ) {
        while ( my $code = shift @{ $self->{codequeue} } ) {
            eval { $code->($self); };
            warn "$code: $@" if $@;
            if ( time > $ttr && @{ $self->{codequeue} } ) {
                $self->{qid} =
                  $self->{queue}->enqueue( $self->{ttr} = $ttr, $self );
                last;
            }
        }
        my $status;
        sleep 2 while !( $status = $dbHandle->commit );
    }
    else {    # Database seems to be locked
        $dbHandle->rollback;
        $self->{qid} = $self->{queue}->enqueue( $self->{ttr} = $ttr, $self );
    }
}

sub commit {
    my $dbHandle = $_[0]{dbHandle};
    if ( $SIG{ALRM} ) {
        alarm 0;
        delete $SIG{ALRM};
    }
    my $status;
    sleep 2 while !( $status = $dbHandle->commit );
    $status;
}

sub disconnect {
    my ($hints) = @_;
    $hints->{cleanup}->();
    delete $hints->{$_} foreach keys %$hints;
}

1;
