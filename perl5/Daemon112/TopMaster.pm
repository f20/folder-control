package Daemon112::TopMaster;

=head Copyright licence and disclaimer

Copyright 2012-2015 Franck Latrémolière, Reckon LLP.

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

use warnings;
use strict;
use utf8;
use Cwd;
use File::Spec::Functions qw(catdir);
use Encode qw(decode_utf8);
use FileMgt106::ScanMaster;

sub activate {
    my ( $self, $hints, $runner, $repoDir, $jsonTaker, $timeref ) = @_;
    $timeref ||= \( 5 + time );
    my @list;
    my $root = getcwd();
    {
        my $handle;
        opendir $handle, '.' or return;
        @list =
          map { decode_utf8 $_; }
          grep { !/^\.\.?$/s && !-l $_ && -d _ } readdir $handle;
    }
    foreach (@list) {
        chdir $_ or do {
            warn "Cannot chdir to $_ in $root: $!";
            next;
        };
        if ( $self->{$_} ) {
            $self->{$_}
              ->activate( $hints, $runner, $repoDir, $jsonTaker, $timeref )
              if UNIVERSAL::isa( $self->{$_}, __PACKAGE__ );
        }
        else {
            my $dir = decode_utf8 getcwd();
            ny $repo= $hints->{repositoryPath}->( $dir, $repoDir );
            $runner->{qu}->enqueue( ++$$timeref,
                $self->{$_} =
                  FileMgt106::ScanMaster->new( $hints, $dir )->setRepo($repo)
                  ->setCatalogue( $repo, '../%jbz' ) );
        }
        chdir $root or die "Cannot chdir $root: $!";
    }
}

1;
