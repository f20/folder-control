package FileMgt106::CLI::ScanCLI;

# Copyright 2011-2019 Franck Latrémolière, Reckon LLP and others.
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

use Cwd qw(getcwd);
use Encode qw(decode_utf8);
use File::Basename qw(dirname);
use File::Spec::Functions qw(catfile rel2abs);
use FileMgt106::CLI::Autograb;
use FileMgt106::CLI::MigVol;
use FileMgt106::Database;
use FileMgt106::FileSystem qw(STAT_MTIME);

use constant {
    SCLI_START    => 0,
    SCLI_PERL5DIR => 1,
    SCLI_FSOBJ    => 2,
    SCLI_HINTS    => 3,
};

sub new {
    my $class = shift;
    bless [@_], $class;
}

sub startFolder {
    my ($self) = @_;
    $self->[SCLI_START];
}

sub homePath {
    my ($self) = @_;
    dirname( $self->[SCLI_PERL5DIR] );
}

sub fileSystemObj {
    my ($self) = @_;
    $self->[SCLI_FSOBJ] ||= FileMgt106::FileSystem->new;
}

sub hintsObj {
    my ($self) = @_;
    return $self->[SCLI_HINTS] if $self->[SCLI_HINTS];
    my $hintsFile = catfile( $self->homePath, '~$hints' );
    $self->[SCLI_HINTS] = FileMgt106::Database->new($hintsFile)
      or die "Cannot create database $hintsFile";
}

sub process {
    my ( $self, @arguments ) = @_;
    local $_ = $arguments[0];
    return $self->scan_command_help unless defined $_;
    return $self->$_(@arguments)
      if s/^-+/scan_command_/s && UNIVERSAL::can( $self, $_ );
    require FileMgt106::CLI::ScanProcessor;
    my ( $scalarAcceptor, $folderAcceptor, $finisher, $legacyArgumentsAcceptor )
      = $self->makeProcessor;
    $legacyArgumentsAcceptor->(@arguments);
    $finisher->();
}

sub scan_command_help {
    warn <<EOW;
Usage:
    scan.pl -autograb [options] <catalogue-files>
    scan.pl -help
    scan.pl -migrate [<old-hints-file>]
    scan.pl -volume [options]
    scan.pl <legacy-arguments>
EOW
}

sub scan_command_top {
    my $self    = shift;
    my $command = shift;
    my %locs;
    my $hints = $self->hintsObj;
    require Daemon112::TopMaster;
    my $readOnlyFlag;
    foreach (@_) {
        if (/^-+stash=(.+)/) {
            local $_ = $1;
            $locs{stash} = rel2abs( $_, $self->startFolder );
            next;
        }
        elsif (/^-+backup=?(.*)/) {
            local $_ = $1;
            $locs{repo} = rel2abs( $_, $self->startFolder );
            next;
        }
        elsif (/^-+git=?(.*)/) {
            local $_ = $1;
            $locs{git} = rel2abs( $_, $self->startFolder );
            next;
        }
        elsif (/^-+resolve/) {
            $locs{resolve} = 1;
            next;
        }
        elsif (/^-+read-?only/) {
            $readOnlyFlag = 1;
            next;
        }
        elsif (/^$/s) {
            next;
        }
        elsif ( chdir rel2abs( $_, $self->startFolder ) ) {
            $hints->beginInteractive;
            Daemon112::TopMaster->new(
                $readOnlyFlag
                ? (
                    '/scanMasterConfig' => sub {
                        $_[0]->setFrotl(604_800);
                    }
                  )
                : ()
              )->attach( decode_utf8 getcwd() )->dequeued(
                {
                    hints => $hints,
                    locs  => \%locs,
                }
              );
            $hints->commit;
        }
        else {
            warn "Ignored: $_";
        }
    }
}

1;
