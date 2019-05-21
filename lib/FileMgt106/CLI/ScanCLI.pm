package FileMgt106::CLI::ScanCLI;

# Copyright 2011-2019 Franck Latrémolière, Reckon LLP.
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

use warnings;
use strict;
use utf8;

require POSIX;
use File::Basename qw(dirname);
use File::Spec::Functions qw(catfile rel2abs abs2rel);
use FileMgt106::Database;
use FileMgt106::FileSystem qw(STAT_DEV STAT_MTIME);

sub new {
    my ( $class, $startFolder, $perl5dir, $fs ) = @_;
    $fs ||= FileMgt106::FileSystem->new;
    bless [ $startFolder, $perl5dir, $fs ], $class;
}

sub process {
    my ( $self, @arguments ) = @_;
    local $_ = $arguments[0];
    return $self->help unless defined $_;
    return $self->$_(@arguments) if s/^-+//s && UNIVERSAL::can( $self, $_ );
    require FileMgt106::CLI::ScanProcessor;
    my ( $scalarAcceptor, $folderAcceptor, $finisher, $legacyArgumentsAcceptor )
      = $self->makeProcessor;
    $legacyArgumentsAcceptor->(@arguments);
    $finisher->();
}

sub help {
    warn <<EOW;
Usage:
    scan.pl -autograb [options] <catalogue-files>
    scan.pl -help
    scan.pl -migrate[=<old-hints-file>]
    scan.pl -watchtest
    scan.pl <legacy-arguments>
EOW
}

sub watchtest {
    my ( $self,        @arguments ) = @_;
    my ( $startFolder, $perl5dir )  = @$self;
    my $module   = 'Daemon112::SimpleWatch';
    my $nickname = 'wtest';
    my ( $logging, $hintsFile, $top, $repoPath, $gitPath, $jbzPath, $parent ) =
      map { defined $_ ? rel2abs($_) : $_; }
      grep { !/^-+watch/i; } @arguments;
    $parent ||= $startFolder;
    require Daemon112::Daemon;
    Daemon112::Daemon->run(
        $module,   $nickname, $logging, $hintsFile, $top,
        $repoPath, $gitPath,  $jbzPath, $parent
    );
}

sub autograb {

    my ( $self,        @arguments ) = @_;
    my ( $startFolder, $perl5dir )  = @$self;
    my @grabSources = map { /^-+grab=(.+)/s ? $1 : (); } @arguments;
    require FileMgt106::CLI::ScanProcessor;
    my ( $scalarAcceptor, $folderAcceptor, $finisher, undef, $chooserMaker ) =
      $self->makeProcessor( @grabSources ? @grabSources : '' );
    my $chooser = $chooserMaker->( grep { /^-initial/s; } @arguments );
    my $stashLoc;
    my @fileList = map {
        if (/^-+stash=(.+)/) {
            local $_ = $1;
            $stashLoc = m#^/# ? $_ : "$startFolder/$_";
            ();
        }
        elsif (/^-$/s) {
            local $/ = "\n";
            map { chomp; $_; } <STDIN>;
        }
        else {
            $_;
        }
    } @arguments;

    foreach (@fileList) {
        $_ = abs2rel( $_, $startFolder ) if m#^/#s;
        chdir $startFolder;
        my @targetStat = stat;
        -f _ or next;
        my @components = split /\/+/;
        my $canonical  = pop @components;
        next
          unless $canonical =~ s/(\.jbz|\.json\.bz2|\.json|\.txt|\.yml)$//s;
        my $extension = $1;
        my $source    = $components[0];
        $source =~ s/^[^a-z]+//i;
        $canonical = "\@$source $canonical";

        if ( my ( $scalar, $folder ) =
            $chooser->( $_, $canonical, $extension, $targetStat[STAT_DEV] ) )
        {
            $scalarAcceptor->(
                $scalar, $folder, $1,
                \@targetStat,
                {
                    restamp => 1,
                    stash   => $stashLoc,
                }
            );
        }
    }

    $finisher->();

}

sub migrate {
    my ( $self, $command, $oldFileName ) = @_;
    my ( $startFolder, $perl5dir ) = @$self;
    $oldFileName = rel2abs( $oldFileName, $startFolder )
      if defined $oldFileName;
    chdir dirname($perl5dir) or die "chdir dirname($perl5dir): $!";
    unless ( $oldFileName && -f $oldFileName ) {
        my $mtime = ( stat '~$hints' )[STAT_MTIME]
          or die 'No existing hints file';
        $mtime = POSIX::strftime( '%Y-%m-%d %H-%M-%S %Z', localtime($mtime) );
        $oldFileName = '~$hints ' . $mtime;
        rename '~$hints', $oldFileName
          or die "Cannot move ~\$hints to $oldFileName: $!";
    }
    my $hintsFile = catfile( dirname($perl5dir), '~$hints' );
    my $hints = FileMgt106::Database->new($hintsFile)
      or die "Cannot create database $hintsFile";
    my $db = $hints->{dbHandle};
    $db->{AutoCommit} = 1;
    $db->do( 'pragma journal_mode=' . ( /nowal/i ? 'delete' : 'wal' ) )
      if /wal/i;
    $db->do("attach '$oldFileName' as old");

    my $prettifyField = sub {
        my ( $number, $spaces ) = @_;
        do { } while $number =~ s/([0-9])([0-9]{3})(?:,|$)/$1,$2/s;
        $spaces -= length $number;
        ( $spaces > 0 ? ' ' x $spaces : '' ) . $number;
    };
    my $reportProgress = sub {
        warn join( '',
            $prettifyField->( $_[0], 5 ),
            map { $prettifyField->( $_, 15 ); } @_[ 1 .. $#_ ] )
          . "\n";
    };
    if ( $db->do('begin exclusive transaction') ) {
        $reportProgress->(qw(Level Added Total));
        $db->do('delete from main.locations');
        $hints->{deepCopy}->( 'old.locations', 0, 0, undef, $reportProgress );
        warn 'Committing changes';
        sleep 2 while !$db->commit;
        FileMgt106::Database->new($hintsFile)
          or die 'Cannot complete database initialisation';
    }
    else {
        warn 'New database is in use: no migration done';
    }
}

1;
