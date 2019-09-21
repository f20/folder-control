package FileMgt106::Builder;

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

use strict;
use warnings;
use Encode qw(decode_utf8);
use File::Spec::Functions qw(catdir catfile);
use FileMgt106::Database;
use FileMgt106::FileSystem qw(STAT_DEV STAT_INO STAT_MODE STAT_UID);

sub makeHintsBuilder {

    my ( $hintsFile, $useSymlinksNotCopies, $infillFlag ) = @_;
    my $hints = FileMgt106::Database->new( $hintsFile, 1 );
    my $searchSha1 = $hints->{searchSha1};

    my $createTree;
    $createTree = sub {

        my ( $whatYouWant, $whereYouWantIt, $devNo ) = @_;
        return unless $whatYouWant;

        unless ($devNo) {
            $whereYouWantIt ||= '.';
            mkdir $whereYouWantIt unless -e $whereYouWantIt;
            die "No device for $whereYouWantIt"
              unless $devNo = ( stat $whereYouWantIt )[STAT_DEV];
        }

        # Scalar representing missing objects (or false if none).
        my $returnValue;

        my $stashFolder;
        my $dh;
        opendir $dh, $whereYouWantIt;
        my %toDelete =
          map { ( decode_utf8($_) => undef ); }
          grep { !/^(?:\.\.?|Icon\r)$/s; } readdir $dh;
        closedir $dh;

      ENTRY: while ( my ( $name, $what ) = each %$whatYouWant ) {
            next unless defined $what;
            next if $name =~ m#/#;
            delete $toDelete{$name};
            my $fileName = catfile( $whereYouWantIt, $name );
            my @existingStat = lstat $fileName;
            if ( -l _ ) {
                unlink $fileName or die "unlink $fileName: $!";
            }
            if ( ref $what ) {
                if ( ref $what eq 'HASH' ) {
                    if ( !-d _ ) {
                        if (@existingStat) {
                            unless ( defined $stashFolder ) {
                                mkdir $stashFolder =
                                  catdir( $whereYouWantIt, "Z_Stashed-$$" );
                            }
                            die "Cannot move $fileName to $stashFolder "
                              . catfile( $stashFolder, $name )
                              unless rename $fileName,
                              catfile( $stashFolder, $name );
                        }
                        mkdir $fileName;
                    }
                    my $rv = $createTree->( $what, $fileName, $devNo );
                    $returnValue->{$name} = $rv if $rv;
                }
                next;
            }
            unless ( $what =~ /([0-9a-fA-F]{40})/ ) {
                symlink $what, $fileName or $returnValue->{$name} = $what;
                next;
            }
            my $sha1 = pack( 'H*', $1 );
            my $iterator = $searchSha1->( $sha1, $devNo );
            my ( @stat, @candidates, @reservelist );
            while ( !@stat
                && ( my ( $path, $statref, $locid ) = $iterator->() ) )
            {
                next unless -f _;
                if (
                    !$locid
                    || ( $statref->[STAT_UID]
                        && ( $statref->[STAT_MODE] & 0200 ) )
                    || ( $statref->[STAT_MODE] & 022 )
                  )
                {
                    push @reservelist, $path;
                    next;
                }
                if (   $statref->[STAT_DEV] != $devNo
                    || $statref->[STAT_UID] && $statref->[STAT_UID] < 500 )
                {
                    push @candidates, $path;
                    next;
                }
                if (@existingStat) {
                    next ENTRY
                      if $existingStat[STAT_DEV] == $statref->[STAT_DEV]
                      && $existingStat[STAT_INO] == $statref->[STAT_INO];
                    unless ( defined $stashFolder ) {
                        mkdir $stashFolder =
                          catdir( $whereYouWantIt, "Z_Stashed-$$" );
                    }
                    die "Cannot move $fileName to $stashFolder"
                      unless rename $fileName,
                      catfile( $stashFolder, $name );
                }
                next ENTRY if link $path, $fileName;
            }
            foreach ( @candidates, @reservelist ) {
                next ENTRY
                  if $useSymlinksNotCopies
                  && ( $fileName !~ m^\.aplibrary/^
                    || $fileName =~ m^\.aplibrary/Masters/^ )
                  ? symlink( $_, $fileName )
                  : _copyFile( $_, $fileName );
            }
            symlink $what, $fileName unless $useSymlinksNotCopies;
            $returnValue->{$name} = $what;

        }

        if ( !$infillFlag && %toDelete ) {
            unless ( defined $stashFolder ) {
                mkdir $stashFolder = catdir( $whereYouWantIt, "Z_Stashed-$$" );
            }
            foreach ( keys %toDelete ) {
                rename catfile( $whereYouWantIt, $_ ),
                  catfile( $stashFolder, $_ )
                  or die "Cannot move $_ to $stashFolder";
            }
        }

        $returnValue;

    };

}

sub _copyFile {
    my $status = system qw(cp -p --), @_;
    return 1 if 0 == $status;
    warn join ' ', qw(system cp -p --), @_, 'returned',
      unpack( 'H*', pack( 'n', $status ) ), 'Caller:', caller,
      'Cwd:',
      decode_utf8(`pwd`);
    return;
}

1;
