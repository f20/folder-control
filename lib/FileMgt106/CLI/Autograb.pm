package FileMgt106::CLI::ScanCLI;

# Copyright 2019 Franck Latrémolière, Reckon LLP.
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

sub autograb {

    my $self = shift;
    my ( @grabSources, $stashLoc, @fileList, %options );
    foreach (@_) {
        if (/^-+grab=(.+)/s) {
            push @grabSources, $1;
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
        my $folder     = pop @components;
        next
          unless $folder =~ s/(\.jbz|\.json\.bz2|\.json|\.txt|\.yml)$//s;
        my $fileExtension = $1;
        my $source        = $components[0];
        $source =~ s/^[^a-z]+//i;
        $folder = "\@$source $folder";
        my $target = FileMgt106::LoadSaveNormalize::loadNormalisedScalar($_);

        if ( !-d $folder && ( my $caseidsha1hex = $target->{'.caseid'} ) ) {
            $hints->beginInteractive;
            my $iterator =
              $hints->{searchSha1}
              ->( pack( 'H*', $caseidsha1hex ), $catStat[STAT_DEV] );
            my $destination;
            while ( my ($path) = $iterator->() ) {
                next if defined $destination;
                next if $path =~ m#/Y_Cellar.*/#;
                my $newFolder = dirname($path);
                symlink $newFolder, $folder if $options{symlinkFolders};
                $folder = $newFolder;
                last;
            }
            $hints->commit;
            if ( !-d $folder && $options{initFlag} ) {
                mkdir $folder;
                open my $fh, '>', catfile( $folder, '🚫.txt' );
                print {$fh} '{".":"no"}';
            }
        }
        if ( -d $folder ) {
            if (
                my ($buildExclusionsFile) =
                grep { -f catfile( $folder, $_ ); } '🚫.txt', '⛔️.txt'
              )
            {
                unlink catfile( $folder, "📖$fileExtension" );
                symlink rel2abs( $_, $self->startFolder ),
                  catfile( $folder, "📖$fileExtension" );
                ( $target, my $excluded ) = _filterExclusions(
                    $target,
                    FileMgt106::LoadSaveNormalize::loadNormalisedScalar(
                        catfile( $folder, $buildExclusionsFile )
                    )
                );
                $target->{$buildExclusionsFile} =
                  $target->{"📖$fileExtension"} = [];
            }
            $scalarAcceptor->(
                $target, $folder, $1,
                \@catStat,
                {
                    restamp => 1,
                    stash   => $stashLoc,
                }
            );
        }
        elsif ( $options{symlinkCats} ) {
            symlink $_, "$folder$fileExtension";
        }
    }

    $finisher->();

}

1;
