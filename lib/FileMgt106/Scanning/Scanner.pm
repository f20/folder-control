package FileMgt106::Scanning::Scanner;

# Copyright 2011-2024 Franck Latrémolière and others.
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
use utf8;
use Encode qw(decode_utf8);
use POSIX       ();
use Digest::SHA ();
use FileMgt106::FileSystem qw(
  STAT_DEV
  STAT_INO
  STAT_MODE
  STAT_NLINK
  STAT_UID
  STAT_SIZE
  STAT_MTIME
  STAT_CHMODDED
);

my $_sha1Machine = new Digest::SHA;
my $_sha1Empty   = $_sha1Machine->digest;

sub new {

    my ( $className, $dir, $hints, $rstat ) = @_;
    my $allowActions = $rstat;
    $rstat = FileMgt106::FileSystem->justLookingStat
      unless $allowActions;
    my $self = bless {}, $className;

    my $regexIgnoreEntirely = qr/(?:
        ^:2eDS_Store$|
        ^Icon\r|
        ^\.DS_Store$|
        ^\._|
        ^\.git$|
        ^\.svn$|
        ^\.vs$|
        ^cyrus\.cache$|
        ^write-lock$|
        ^~\$|
        \.core$|
        \.pyc$
      )/sx;
    my $regexIgnoreFolderContents =
      qr/(?:\.app|\.download|\.lrdata|\.tmp|_modules)$/is;

    my $regexWatchThisFile =
      -e '/System/Library'    # Test whether the system is a Mac
      ? qr/\.(?:R|c|command|clj|cpp|css|do|doc|docx|h|java|js|json|m|pl|pm|pptx|py|swift|txt|yml)$/isx
      : qr/\.(?:R|c|command|clj|cpp|css|do|h|java|js|json|m|pl|pmπ|py|swift|txt|yml)$/isx;
    my $regexAlwaysRecheckThisFile = qr/\.xls$/is;

    my $regexNeverWatchFolder    = qr/^Y_|\.sparsebundle$/is;
    my $regexWatchFolderForADay  = qr/^[@#]/is;
    my $regexAlwaysWatchFolder   = qr/^[OWXZ🗄️📎✉️📨📬]_/is;
    my $regexQuicklyMakeReadOnly = qr/^(?:🗄️|📎|✉️|📨|📬|X_|Y_)/is;
    my $regexNeverMakeReadOnly   = qr/\.sparsebundle$/is;

    my ( $dev, $rootLocid, $makeChildStasher, $makeChildBackuper, $repoDev );
    {
        my @stat = stat $dir or die "$dir: cannot stat";
        $dev       = $stat[STAT_DEV];
        $rootLocid = $hints->{topFolder}->( $dir, $dev, $stat[STAT_INO] )
          or die "$dir: no root locid";
    }

    my (
        $folder,          $file,        $children,     $updateSha1,
        $updateLocation,  $searchSha1,  $alreadyThere, $findName,
        $moveByParidName, $moveByLocid, $uproot,       $checkFolder,
      )
      = @{$hints}{
        qw(folder file children updateSha1 updateLocation),
        qw(searchSha1 alreadyThere findName),
        qw(moveByParidName moveByLocid uproot checkFolder),
      };

    my $create = sub {
        my ( $name, $folderLocid, $whatYouWant, $devNo, $pathToFolder,
            $forceReadOnlyTimeLimit, )
          = @_;

        # $whatYouWant is either a hashref or a binary sha1.

        my $fileName = defined $pathToFolder ? "$pathToFolder/$name" : $name;
        my $sha1;
        if ( ref $whatYouWant ) {
            if ( $whatYouWant->{$name} =~ /([0-9a-fA-F]{40})/ ) {
                $sha1 = pack( 'H*', $1 );
            }
            elsif ( ref $whatYouWant->{$name} eq 'ARRAY' ) {
                return 1;
            }
            elsif ( symlink $whatYouWant->{$name}, $fileName ) {
                delete $whatYouWant->{$name};
                return 1;
            }
            else {
                warn "symlink $whatYouWant->{$name}, $fileName: $!";
                return;
            }
        }
        else {
            return unless $sha1 = $whatYouWant;
        }
        my @stat;
        if ( $sha1 eq $_sha1Empty ) {
            unless ( open my $fh, '>', $fileName ) {
                warn "Could not create empty file $fileName";
                return;
            }
            @stat = $rstat->($fileName);
        }
        else {
            my $iterator = $searchSha1->( $sha1, $devNo );
            my @wouldNeedToCopy;
            while ( !@stat
                && ( my ( $path, $statref, $locid ) = $iterator->() ) )
            {
                next unless -f _;
                if (   $locid
                    && $statref->[STAT_DEV] == $devNo
                    && _isMergeable($statref)
                    && link( $path, $fileName ) )
                {
                    @stat = $rstat->($fileName);
                }
                else {
                    push @wouldNeedToCopy, $path;
                }
            }
            while ( !@stat && @wouldNeedToCopy ) {
                my $source = pop @wouldNeedToCopy;
                system qw(cp -p --), $source, $fileName;
                @stat = $rstat->( $fileName, $forceReadOnlyTimeLimit );
                my $newsha1 = sha1File($fileName);
                unless ( defined $newsha1 && $sha1 eq $newsha1 ) {
                    warn 'SHA1 mismatch after trying to copy '
                      . "$source to $fileName";
                    @stat = ();
                    unlink $fileName;
                }
            }
            unless (@stat) {
                symlink $whatYouWant->{$name}, $fileName if ref $whatYouWant;
                return;
            }
        }
        my ($fileLocid) = $file->(
            $folderLocid, $name,
            @stat[ STAT_DEV, STAT_INO, STAT_SIZE, STAT_MTIME ]
        );
        $updateSha1->( $sha1, $fileLocid );
        delete $whatYouWant->{$name} if ref $whatYouWant;
        1;
    };

    my $resolveLocidClosure = sub {
        my ( $parentClosure, $name )     = @_;
        my ( $parentLocid, $parentPath ) = $parentClosure->();
        my $path = "$parentPath/$name";
        my @stat = stat $path;
        unless ( -d _ ) {
            if ( -e _ ) {
                my ( $newName, $newPath ) =
                  $findName->( $parentLocid, $name, $parentPath );
                rename $path, $newPath;
                $moveByParidName->(
                    $parentLocid, $newName, $parentLocid, $name
                );
            }
            mkdir $path;
            die "mkdir $path: $!" unless @stat = $rstat->($path);
        }
        ( $folder->( $parentLocid, $name, @stat[ STAT_DEV, STAT_INO ] ),
            $path );
    };

    my $createTree;
    $createTree = sub {
        my ( $whatYouWant, $devNo, $folderLocid, $pathToFolder, $backuper ) =
          @_;

        # Scalar representing missing objects (or false if none).
        my $returnValue;

        while ( my ( $name, $what ) = each %$whatYouWant ) {
            next if $name eq '' || $name eq '..' || $name =~ m#/#;
            my $path = "$pathToFolder/$name";
            if ( ref $what eq 'HASH' ) {
                ( $name, $path ) =
                  $findName->( $folderLocid, $name, $pathToFolder )
                  if -e $path
                  && !-d _;
                my $rv = $createTree->(
                    $what, $devNo,
                    $resolveLocidClosure->(
                        sub { ( $folderLocid, $pathToFolder ) }, $name
                    ),
                    $backuper
                    ? $makeChildBackuper->( $backuper, $name )
                    : undef
                );
                $returnValue->{$name} = $rv if $rv;
                next;
            }
            if ( -l $path ) {
                unlink $path or warn "unlink $path: $!";
            }
            elsif ( -e _ ) {
                ( $name, $path ) =
                  $findName->( $folderLocid, $name, $pathToFolder );
            }
            if ( $what =~ /([0-9a-fA-F]{40})/ ) {
                my $sha1 = pack( 'H*', $1 );
                if (
                    $create->(
                        $name, $folderLocid, $sha1, $devNo, $pathToFolder
                    )
                  )
                {
                    $backuper->( $name, $folderLocid, $sha1 ) if $backuper;
                }
                else {
                    symlink $what, $path;
                    $returnValue->{$name} = $what;
                }
                next;
            }
            next if ref $what or symlink $what, $path;
            $returnValue->{$name} = $what;
        }

        $returnValue;

    };

    my $scanDir;
    $scanDir = sub {
        my ( $locid, $path, $forceReadOnlyTimeLimit, $watchMaster, $hashref,
            $stasher, $backuper, $watchTimeLimit, )
          = @_;
        my $timeNow = time;
        my $mergeEveryone =
          $forceReadOnlyTimeLimit && $forceReadOnlyTimeLimit > $timeNow;
        my $target              = !defined $watchMaster && $hashref;
        my $runningUnderWatcher = $hashref              && $watchMaster;
        my $oldChildrenHashref  = $children->($locid);
        if ($runningUnderWatcher) {
            $oldChildrenHashref->{$_} ||= 0 foreach keys %$hashref;
        }
        $hashref = {} if $target || !$hashref;
        if ($watchMaster) {
            if ( defined $watchTimeLimit && $watchTimeLimit < 0 ) {
                $watchTimeLimit = -$watchTimeLimit;
            }
            else {
                $watchMaster->watchFolder( $scanDir, $locid, $path, $hashref,
                    $forceReadOnlyTimeLimit, $stasher, $backuper );
            }
        }
        my %targetHasBeenApplied;
        my @list;
        {
            my $handle;
            opendir $handle, '.' or return;
            @list = map { decode_utf8 $_; }
              grep { !/^\.\.?$/s } readdir $handle;
        }
        if ($target) {
            my %list = map { $_ => undef } @list;
            foreach ( grep { !/\// && !exists $list{$_}; } keys %$target ) {
                if ( -e $_ ) {
                    warn "Unexpectedly found $_ in $dir/$path";
                }
                elsif ( $target->{$_} && !ref $target->{$_} ) {
                    undef $targetHasBeenApplied{$_};
                    push @list, $_ if $create->( $_, $locid, $target, $dev );
                }
                elsif ( mkdir $_ and $rstat->($_) ) {
                    push @list, $_;
                }
                else {
                    warn "Could not mkdir $_: $! (in $dir/$path)";
                }
            }
        }

        my %binned = ();

        foreach (@list) {

            if (/$regexIgnoreEntirely/s) {
                delete $oldChildrenHashref->{$_};
                my @stat = lstat        or next;
                $stat[STAT_DEV] == $dev or next;
                -d _
                  ? $folder->( $locid, $_, @stat[ STAT_DEV, STAT_INO ] )
                  : $file->(
                    $locid, $_,
                    @stat[ STAT_DEV, STAT_INO, STAT_SIZE, STAT_MTIME ]
                  );
                next;
            }

            my @stat = $rstat->( $_, $forceReadOnlyTimeLimit );
            next if $stat[STAT_DEV] && $stat[STAT_DEV] != $dev;

            my $mustBeTargeted = $target && !exists $targetHasBeenApplied{$_};
            undef $targetHasBeenApplied{$_};

            if ($mustBeTargeted) {
                if ( !$target->{$_} ) {
                    warn "No hints update when stashing $_ from $dir/$path"
                      unless $stasher->( $_, $locid ) > 0;
                    delete $oldChildrenHashref->{$_};
                    next;
                }
                elsif ( !ref $target->{$_} && !-f _ ) {
                    -l _ ? unlink($_) : $stasher->( $_, $locid );
                    redo if $create->( $_, $locid, $target, $dev );
                    lstat $_;
                }
                elsif ( ref $target->{$_} eq 'HASH' && !-d _ ) {
                    $stasher->( $_, $locid );
                    redo if mkdir $_ and $rstat->($_);
                }
                elsif ( ref $target->{$_} eq 'ARRAY' ) {
                    delete $target->{$_};
                    undef $mustBeTargeted;
                }
            }

            if ( -f _ ) {

                my ( $fileLocid, $sha1, $rehash ) = $file->(
                    $locid, $_,
                    @stat[ STAT_DEV, STAT_INO, STAT_SIZE, STAT_MTIME ]
                );

                my $readOnly = _isReadOnly( \@stat );

                $rehash = 1
                  if !$rehash
                  and $allowActions
                  and $stat[STAT_CHMODDED]
                  || !$runningUnderWatcher
                  && !$readOnly
                  && $_ =~ $regexAlwaysRecheckThisFile;

                my $mergeCandidate =
                     $readOnly
                  && $forceReadOnlyTimeLimit
                  && $allowActions
                  && ( $mergeEveryone || $rehash || $stat[STAT_CHMODDED] )
                  && _isMergeable( \@stat );

                if ($rehash) {
                    my $newsha1 = sha1File($_);
                    unless ( defined $newsha1 ) {
                        warn "Could not sha1 $dir/$path$_";
                        next;
                    }
                    $updateSha1->( $newsha1, $fileLocid )
                      unless defined $sha1 && $newsha1 eq $sha1;
                    if ($backuper) {
                        $backuper->( $_, $locid, $sha1 )
                          if defined $sha1 && $newsha1 ne $sha1;
                        unless ( $backuper->( $_, $locid, $newsha1 ) ) {
                            $updateSha1->( undef, $fileLocid )
                              ;    # undo sha1 storage
                            mkdir "Could not backup $_"
                              ;    # inform file system watcher
                            rmdir "Could not backup $_";    # clean up
                            die "Could not backup $dir/$path$_";
                        }
                    }
                    $sha1 = $newsha1;
                }

                if ($mergeCandidate) {
                    my $iterator = $searchSha1->(
                        $sha1, $stat[STAT_DEV], $stat[STAT_INO],
                        $stat[STAT_NLINK] > 1
                    );
                    while (
                        $iterator
                        && ( my ( $ipath, $statref, $mergelocid ) =
                            $iterator->() )
                      )
                    {
                        next
                          unless -f _
                          && $mergelocid
                          && $statref->[STAT_DEV] == $dev
                          && _isMergeable($statref);
                        my $tfile;
                        do { $tfile = '~$ temporary merge file ' . rand(); }
                          while -e $tfile;
                        next unless link $ipath, $tfile;
                        warn "$_ <= $ipath\n";
                        my @stat2 = $rstat->($tfile);
                        if ( FileMgt106::FileSystem::filesDiffer( $_, $tfile ) )
                        {
                            warn 'Merging cancelled';
                            warn unpack( 'H*', sha1File($_) )
                              . " $dir/$path$_\n";
                            warn unpack( 'H*', sha1File($tfile) ) . " $ipath\n";
                            unlink $tfile;
                            next;
                        }
                        if (   $stat[STAT_MTIME]
                            && $stat[STAT_MTIME] < $stat2[STAT_MTIME] )
                        {
                            $stat2[STAT_MTIME] = $stat[STAT_MTIME]
                              if utime $timeNow, $stat[STAT_MTIME], $tfile;
                        }
                        next unless rename $tfile, $_;
                        $updateLocation->(
                            @stat2[ STAT_DEV, STAT_INO, STAT_SIZE, STAT_MTIME ],
                            $fileLocid
                        );
                        undef $iterator;
                    }
                }

                $watchMaster->watchFile( $scanDir, $locid, $path, $hashref, $_,
                    $stasher, $backuper )
                  if $watchMaster
                  and !$readOnly
                  and $stat[STAT_MTIME] > $timeNow - 60
                  || /$regexWatchThisFile/;

                $hashref->{$_} = unpack 'H*', $sha1;

                if ($mustBeTargeted) {
                    if ( lc $target->{$_} eq $hashref->{$_} ) {
                        delete $target->{$_};
                        delete $oldChildrenHashref->{$_};
                    }
                    else {
                        $stasher->( $_, $locid );
                        redo if $create->( $_, $locid, $target, $dev );
                    }
                }
                else {
                    delete $oldChildrenHashref->{$_};
                }

            }

            elsif ( -d _ ) {

                delete $oldChildrenHashref->{$_};

                if ( $allowActions && $stasher && /^Z_/is && -w _ ) {

                    if ($watchMaster) {
                        $watchMaster->watchFolder( $scanDir, $locid, $path,
                            $hashref,  $forceReadOnlyTimeLimit, $stasher,
                            $backuper, -15,                     $_ );
                    }

                    chdir $_ or next;

                    if ( my @items = _listDirectory() ) {
                        my ( $stashLocid, $stash ) = $stasher->();
                        my $name = $_;
                        $name =~ s/^Z_/Y_/is;
                        $name .=
                          POSIX::strftime( ' %Y-%m-%d %a %H%M%S', localtime );
                        my $binName = $name;
                        foreach ( -9 .. 0 ) {
                            my $p2 = $stash . '/' . $binName;
                            last if mkdir $p2 and $rstat->($p2);
                            die "mkdir $stash/$binName: $!" unless $_;
                            $binName = $name . ' #' . _randomString(3);
                        }
                        my $crashIndicatorSymlink = "$dir/$path~\$temp $_";
                        if (
                            -e $crashIndicatorSymlink
                            || !symlink "$stash/$binName",
                            $crashIndicatorSymlink
                          )
                        {
                            foreach ( -9 .. 0 ) {
                                my $n =
                                  $crashIndicatorSymlink . ' #'
                                  . _randomString(3);
                                if ( symlink "$stash/$binName", $n ) {
                                    $crashIndicatorSymlink = $n;
                                    last;
                                }
                            }
                            die $crashIndicatorSymlink unless $_;
                        }
                        rename( $_, "$stash/$binName/$_" )
                          || die "rename $_, $stash/$binName/$_: $!"
                          foreach @items;
                        unless (/^Z_(?:Archive|Cellar|Infill|Reuse|Rubbish)$/is)
                        {
                            chdir "$dir/$path"
                              or die "chdir $dir/$path: $!";
                            rmdir $_;
                        }
                        chdir "$stash/$binName"
                          or die "chdir $stash/$binName: $!";
                        unless (/^Z_(?:Infill|Rubbish)/is) {
                            require FileMgt106::Catalogues::LoadSaveNormalize;
                            FileMgt106::Catalogues::LoadSaveNormalize::renameFilesToNormalisedScannable(
                                '.');
                        }
                        my $frotl =
                          /^Z_(?:Archive|Cellar)/is
                          ? $timeNow + 604_800
                          : $forceReadOnlyTimeLimit;
                        my $cat =
                          substr( $stash, 0, length($dir) + 1 ) eq "$dir/"
                          ? $scanDir->(
                            $folder->(
                                $stashLocid, $binName,
                                ( stat '.' )[ STAT_DEV, STAT_INO ]
                            ),
                            substr( $stash, length($dir) + 1 ) . "/$binName/",
                            $frotl
                          )
                          : FileMgt106::Scanning::Scanner->new(
                            "$stash/$binName", $hints, $rstat )->scan($frotl);
                        $binned{"$binName"} = [ $cat, $crashIndicatorSymlink ];
                    }
                }
                else {

                    if ( $runningUnderWatcher && $hashref->{$_} ) {
                        if (
                            $checkFolder->(
                                $locid, $_, @stat[ STAT_DEV, STAT_INO ]
                            )
                          )
                        {
                            next;
                        }
                        else {
                            delete $hashref->{$_};
                        }
                    }

                    if (/$regexIgnoreFolderContents/s) {
                        $folder->( $locid, $_, @stat[ STAT_DEV, STAT_INO ] );
                        next;
                    }

                    unless ( chdir $_ ) {
                        warn "chdir $dir/$path$_: $!";
                        next;
                    }

                    my $forceReadOnlyTimeLimitForChild =
                      $forceReadOnlyTimeLimit;
                    if ( $allowActions && /$regexQuicklyMakeReadOnly/is ) {
                        $forceReadOnlyTimeLimitForChild = $timeNow - 42
                          unless $forceReadOnlyTimeLimit
                          && $forceReadOnlyTimeLimit > $timeNow;
                    }
                    elsif (/$regexNeverMakeReadOnly/) {
                        $forceReadOnlyTimeLimitForChild = 0;
                    }

                    my ( $watchMasterForChild, $watchTimeLimitForChild );
                    $watchMasterForChild = $watchMaster
                      unless /$regexNeverWatchFolder/is;
                    if ( $watchMasterForChild && !/$regexAlwaysWatchFolder/is )
                    {
                        my $ageMax = $timeNow -
                          ( /$regexWatchFolderForADay/is ? 86_400 : 2_419_200 );
                        $ageMax = $watchTimeLimit
                          if $watchTimeLimit && $watchTimeLimit > $ageMax;
                        $watchTimeLimitForChild =
                          $stat[STAT_MTIME] < $ageMax ? -$ageMax : $ageMax;
                    }

                    my ( $targetForChild, $stasherForChild, $backuperForChild );
                    if ($mustBeTargeted) {
                        $targetForChild = $target->{$_};
                    }
                    elsif ( ref $hashref->{$_} ) {
                        $targetForChild = $hashref->{$_};
                    }
                    $stasherForChild = $makeChildStasher->( $stasher, $_ )
                      if $stasher;
                    $backuperForChild = $makeChildBackuper->( $backuper, $_ )
                      if $backuper;

                    $hashref->{$_} = $scanDir->(
                        $folder->( $locid, $_, @stat[ STAT_DEV, STAT_INO ] ),
                        "$path$_/",
                        $forceReadOnlyTimeLimitForChild,
                        $watchMasterForChild,
                        $targetForChild,
                        $stasherForChild,
                        $backuperForChild,
                        $watchTimeLimitForChild,
                    );

                    delete $target->{$_}
                      if $mustBeTargeted && !keys %{ $target->{$_} };

                }

                chdir "$dir/$path" or die "chdir $dir/$path: $!";

            }

            elsif ( -l _ ) {
                delete $oldChildrenHashref->{$_};
                $hashref->{$_} = decode_utf8 readlink;
            }

            else {
                delete $oldChildrenHashref->{$_};
                $hashref->{$_} = \@stat;
            }

        }

        while ( my ( $kname, $klocid ) = each %$oldChildrenHashref ) {
            delete $hashref->{$kname} if $runningUnderWatcher;
            $uproot->($klocid)        if $klocid;
        }

        if ( keys %binned ) {

            my $changed = 0;
            undef my $filter;
            while ( my ( $binName, $binDataArrayRef ) = each %binned ) {

                if ( !$target && $backuper && $binName =~ /^Y_Archive/is ) {
                    warn "Archiving from $dir/$path: $binName";
                    my $missing = $createTree->(
                        { $binName => $binDataArrayRef->[0] },
                        $repoDev, $backuper->()
                    );
                    if ($missing) {
                        require FileMgt106::Catalogues::LoadSaveNormalize;
                        FileMgt106::Catalogues::LoadSaveNormalize::saveJbz(
                            "$dir/$path$binName-failed.jbz", $missing );
                    }
                }

                if ( $binName =~ /^Y_(?:In-?fill|Re-?use)/is ) {
                    warn "Infilling from $dir/$path: $binName";
                    unless ( defined $filter ) {
                        my %seen;
                        $filter = sub {
                            my ($hash) = @_;
                            my %newHash;
                            foreach (
                                sort { length $a <=> length $b }
                                keys %$hash
                              )
                            {
                                my $what = $hash->{$_};
                                if ( ref $what eq 'HASH' ) {
                                    $what = $filter->($what);
                                    $newHash{$_} = $what if $what;
                                }
                                elsif ( defined $what && !exists $seen{$what} )
                                {
                                    undef $seen{$what};
                                    s/\s+/ /gs;
                                    s/^ //;
                                    s/ \././g;
                                    s/ $//;
                                    $newHash{ $_ || ( '__' . rand() ) } = $what;
                                }
                            }
                            keys %newHash ? \%newHash : undef;
                        };
                        my $preloadFilter;
                        $preloadFilter = sub {
                            my ($hash) = @_;
                            foreach ( values %$hash ) {
                                if ( ref $_ eq 'HASH' ) {
                                    $preloadFilter->($_);
                                }
                                else {
                                    undef $seen{$_};
                                }
                            }
                        };
                        $preloadFilter->(
                              $runningUnderWatcher
                            ? $scanDir->( $locid, $path )
                            : $hashref
                        );
                    }
                    my $filtered = $filter->( $binDataArrayRef->[0] );
                    if ( $filtered && keys %$filtered ) {
                        ++$changed;
                        $createTree->( $filtered, $dev, $locid, '.',
                            $backuper );
                    }
                }

                unlink $binDataArrayRef->[1];

            }

            goto &$scanDir if $changed;

        }

        $hashref;

    };

    $self->{scan} = sub {

        my ( $forceReadOnlyTimeLimit, $targetHashref, $stashPair, $repoPair,
            $watchMaster, )
          = @_;
        chdir $dir or die "chdir $dir: $!";

        my $doStashing = sub {
            my ( $stashLocid, $stashPath, $name, $locid, $suggestedNewName ) =
              @_;
            my ( $stashName, $stashFile ) =    # assume all on same dev
              $findName->( $stashLocid, $suggestedNewName || $name,
                $stashPath );
            if ( rename $name, $stashFile ) {
                $moveByParidName->( $stashLocid, $stashName, $locid, $name );
            }
            else {
                warn "rename $name, $stashFile: $!";
            }
        };
        $makeChildStasher = sub {
            my @makeClosureArg = @_;
            my $path;
            my $main                 = $resolveLocidClosure;
            my $doStashingForClosure = $doStashing;
            sub {
                ( $main, $path ) = $main->(@makeClosureArg) if ref $main;
                @_
                  ? $doStashingForClosure->( $main, $path, @_ )
                  : ( $main, $path );
            };
        };
        my $rootStasher;
        if ($stashPair) {
            my @stat = stat $stashPair->[0];
            if ( -d _ && -w _ && $stat[STAT_DEV] == $dev ) {
                if ( my $stashLocid =
                    $hints->{topFolder}
                    ->( $stashPair->[0], @stat[ STAT_DEV, STAT_INO ] ) )
                {
                    my $doStashingForClosure = $doStashing;
                    $rootStasher = $makeChildStasher->(
                        sub { ( $stashLocid, $stashPair->[0] ); },
                        $stashPair->[1]
                    );
                }
            }
        }
        unless ($rootStasher) {
            $rootStasher =
              $makeChildStasher->( sub { ( $rootLocid, $dir ); }, '~$stash' );
        }

        my $rootBackuper;
        if ($repoPair) {
            my @stat = stat $repoPair->[0];
            unless ( -e _ ) {
                system qw(mkdir -p), $repoPair->[0];
                @stat = stat $repoPair->[0];
            }
            unless ( -d _ && -w _ ) {
                warn "$dir: cannot use $repoPair->[0]";
                undef $repoPair;
            }
            if ($repoPair) {
                my $repoRootLocid =
                  $hints->{topFolder}
                  ->( $repoPair->[0], @stat[ STAT_DEV, STAT_INO ] )
                  or die
                  "$dir: no root locid for repository folder $repoPair->[0]";
                $repoDev = $stat[STAT_DEV];
                my $nextWeek = time + 604_800;
                my $doBackup = sub {
                    my ( $repoLocid, $repoPath, $name, $locid, $sha1 ) = @_;
                    return 1 if $alreadyThere->( $repoLocid, $name, $sha1 );
                    my $repoName = $findName->( $repoLocid, $name, $repoPath );
                    $create->(
                        $repoName, $repoLocid, $sha1,
                        $repoDev,  $repoPath,  $nextWeek,
                    );
                };
                $makeChildBackuper = sub {
                    my @makeClosureArg = @_;
                    my $path;
                    my $main               = $resolveLocidClosure;
                    my $doBackupForClosure = $doBackup;
                    sub {
                        ( $main, $path ) = $main->(@makeClosureArg)
                          if ref $main;
                        @_
                          ? $doBackupForClosure->( $main, $path, @_ )
                          : ( $main, $path );
                    };
                };
                $rootBackuper = $makeChildBackuper->(
                    sub { ( $repoRootLocid, $repoPair->[0] ); },
                    $repoPair->[1]
                );
            }
        }

        my $scalar = $scanDir->(
            $rootLocid,     '',           $forceReadOnlyTimeLimit, $watchMaster,
            $targetHashref, $rootStasher, $rootBackuper
        );

        wantarray ? ( $scalar, $rootLocid ) : $scalar;

    };

    $self->{infill} = sub {
        my ($whatYouWant) = @_;
        chdir $dir or die "chdir $dir: $!";
        $createTree->( $whatYouWant, $dev, $rootLocid, '.' );
    };

    $self;

}

sub scan {
    goto &{ shift->{scan} };
}

sub infill {
    goto &{ shift->{infill} };
}

sub sha1File($) {
    warn "@_\n"
      ;  # warn join (' ', map { defined $_ ? $_ : undef; } @_, caller ) . "\n";
    my ( %sig, %received );
    my $catch = sub { undef $received{ $_[0] }; };
    foreach ( grep { $SIG{$_} } keys %SIG ) {
        $sig{$_} = $SIG{$_};
        $SIG{$_} = $catch;
    }
    my $sha1 = eval {
        open my $f, '<', $_[0];
        $_sha1Machine->addfile($f)->digest;
    };
    %SIG = %sig;
    $sig{$_}->($_) foreach grep { $sig{$_} } keys %received;
    $sha1;
}

sub _listDirectory {
    my $handle;
    opendir $handle, $_[0] || '.' or return;
    map { decode_utf8 $_; } grep { !/^\.\.?$/s; } readdir $handle;
}

sub _isReadOnly {
    !(
        $_[0][STAT_MODE] & (
            !$_[0][STAT_UID] || $_[0][STAT_UID] == 60
            ? 0020
            : 0220
        )
    );
}

sub _isMergeable {
    return unless $_[0][STAT_INO] && $_[0][STAT_SIZE];
    $_[0][STAT_UID] < 500
      ? 0040 == ( $_[0][STAT_MODE] & 0060 )
      : 0000 == ( $_[0][STAT_MODE] & 0220 );
}

srand;
my @_charset =
  qw(0 1 2 3 4 5 6 7 8 9 A B C E F G H J K L M N P Q R S T V W X Y Z);

sub _randomString {
    join '', map { $_charset[ rand(32) ] } 1 .. $_[0];
}

1;
