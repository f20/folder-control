package FileMgt106::Merge;

=head Copyright licence and disclaimer

Copyright 2016 Franck Latrémolière, Reckon LLP.

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

use FileMgt106::Tools;

use constant {
    M_FOLDER    => 0,
    M_OLDSCALAR => 1,
    M_NEWSCALAR => 2,
};

sub new {
    my $class = shift;
    bless [@_], $class;
}

sub listLocalFolders {
    my ( $self ) = @_;
    die 'Not implemented yet';
}

sub getLocalFolderContents {
    my ( $self, $folder ) = @_;
    die 'Not implemented yet';
}

sub _conflicts {
    return unless defined $_[0] && defined $_[1];
    goto &_hasNewItems;
}

sub _hasNewItems {
    my ( $existing, $new ) = @_;
    my $filter = FileMgt106::Tools::makeInfillFilter();
    $filter->($existing);
    return $filter->($new);
}

sub setLocalFolderContents {
    my ( $self, $folder, $content ) = @_;
    die 'Not implemented yet';
}

sub loadRemote {
    my ( $self, $hashref, $time, $source ) = @_;
    die 'Not implemented yet';
}

sub performMerge {
    my ($self) = @_;
    die 'Not implemented yet';
}

1;
