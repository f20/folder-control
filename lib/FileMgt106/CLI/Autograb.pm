package FileMgt106::CLI::ScanCLI;

# Copyright 2019-2021 Franck Latrémolière and others.
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

# This code probably only works with UNIX-style paths.

use warnings;
use strict;
use File::Spec::Functions qw(catdir catfile rel2abs abs2rel);
use FileMgt106::FileSystem qw(STAT_DEV);
use File::Basename qw(dirname);
use utf8;

sub scan_command_autograb {

    my ( @grabSources, $stashLoc, @fileList, %options );
    my $self    = shift;
    my $command = shift;
    foreach (@_) {
        if (/^-+grab=(.+)/s) {
            push @grabSources, $1;
        }
        elsif (/^-+all/s) {
            $options{initFlag} = 2;
        }
        elsif (/^-+init/s) {
            $options{initFlag} = 1;
        }
        elsif (/^-+stash=(.+)/) {
            local $_ = $1;
            $stashLoc = m#^/# ? $_ : catdir( $self->startFolder, $_ );
        }
        elsif (/^-+symlink/s) {
            $options{symlinkCats}    = 1;
            $options{symlinkFolders} = 1;
        }
        elsif (/^-$/s) {
            local $/ = "\n";
            push @fileList, map { chomp; $_; } <STDIN>;
        }
        else {
            push @fileList, $_;
        }
    }

    require FileMgt106::CLI::ScanProcessor;
    my ( $scalarAcceptor, $folderAcceptor, $finisher, undef ) =
      $self->makeProcessor( grabSources => \@grabSources );
    my $hints = $self->hintsObj;

    foreach (@fileList) {
        $_ = abs2rel( $_, $self->startFolder ) if m#^/#s;
        chdir $self->startFolder;
        my @catStat = stat;
        -f _ or next;
        my @components = split /\/+/;
        my $name       = pop @components;
        next unless $name =~ s/(\.jbz|\.json\.bz2|\.json|\.txt|\.yml)$//s;
        my $fileExtension = $1;
        my $target =
          FileMgt106::Catalogues::LoadSaveNormalize::loadNormalisedScalar($_);

        my $category = pop @components;
        $category ||= 'NoCategory';
        my $source = shift @components;
        $source ||= 'NoSource';
        $source =~ s/^[^a-z]+//i;
        my $fallbackFolder = my $caseidsha1hex = $target->{'.caseid'};
        undef $caseidsha1hex if ref $caseidsha1hex;

        my $folderPath;

        if ($caseidsha1hex) {
            $hints->beginInteractive;
            my $iterator =
              $hints->{searchSha1}
              ->( pack( 'H*', $caseidsha1hex ), $catStat[STAT_DEV] );
            while ( my ($path) = $iterator->() ) {
                next if $path =~ m#/Y_Cellar.*/#;
                my $newFolder = dirname($path);
                symlink $newFolder, catdir( "\@$source $category", $name )
                  if $options{symlinkFolders};
                $folderPath = $newFolder;
                last;
            }
            $hints->commit;
            if ( !defined $folderPath && $options{initFlag} ) {
                $folderPath = catdir( "\@$source $category", $name );
                $folderPath .= '_' while -e $folderPath;
                mkdir "\@$source $category" unless -e "\@$source $category";
                mkdir $folderPath;
                if ( $options{initFlag} < 2 ) {
                    open my $fh, '>', catfile( $folderPath, '🚫.json' );
                    print {$fh} '{".":"no"}';
                }
            }
        }

        if ( defined $folderPath ) {

            if (
                my ($buildExclusionsFile) =
                grep { -f catfile( $folderPath, $_ ); } '🚫.json', '🚫.txt',
                '⛔️.json', '⛔️.txt', '⚠️.json', '⚠️.txt', '🔺.json',
              )
            {
                rename(
                    catfile( $folderPath, $buildExclusionsFile ),
                    catfile( $folderPath, $buildExclusionsFile = '⛔️.json' )
                ) if $buildExclusionsFile =~ /^⚠/;
                unlink catfile( $folderPath, "📖$fileExtension" );
                symlink rel2abs( $_, $self->startFolder ),
                  catfile( $folderPath, "📖$fileExtension" );
                my $exclusions =
                  FileMgt106::Catalogues::LoadSaveNormalize::loadNormalisedScalar(
                    catfile( $folderPath, $buildExclusionsFile ) );
                if ( $buildExclusionsFile eq '🔺.json' ) {
                    require FileMgt106::Catalogues::ConsolidateFilter;
                    my $consolidator =
                      FileMgt106::Catalogues::ConsolidateFilter->new;
                    $consolidator->baseProcessor->($exclusions);
                    my $processor = $consolidator->unseenProcessor;
                    $processor->( $target, 'Z' );
                    $target = $processor->()->{Z};
                }
                else {
                    ($target) = _filterExclusions( $target, $exclusions );
                }
                $target->{'.caseid'} = $caseidsha1hex if $caseidsha1hex;
                $target->{"📖$fileExtension"} = [];
                $target->{$buildExclusionsFile} = [];
            }

            $scalarAcceptor->(
                $target,
                $folderPath,
                $1,
                \@catStat,
                {
                    restamp => 1,
                    stash   => $stashLoc,
                }
            );

        }

    }

    $finisher->();

}

sub _filterExclusions {
    my ( $src, $excl ) = @_;
    return unless defined $src;
    return $src unless $excl;
    return ( undef, $src ) if !ref $excl || $excl->{'.'};
    my %included = %$src;
    my %excluded;
    ( $included{$_}, $excluded{$_} ) =
      _filterExclusions( $included{$_}, $excl->{$_} )
      foreach keys %$excl;
    delete $included{$_}
      foreach grep { !defined $included{$_}; } keys %included;
    my $included = %included ? \%included : undef;
    return $included unless wantarray;
    delete $excluded{$_}
      foreach grep { !defined $excluded{$_}; } keys %excluded;
    $included, %excluded ? \%excluded : undef;
}

1;
