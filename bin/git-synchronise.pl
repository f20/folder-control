#!/usr/bin/env perl

# Copyright 2019-2022 Franck Latrémolière and others.
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
use File::Spec::Functions qw(catdir rel2abs);

my ( $homePath, $coldPath );
my $codeRepo = $ENV{FOLDER_CONTROL_HOME};
my $selfid   = `hostname -s`;
chomp $selfid;
my $scriptPath = rel2abs($0);
if ( $scriptPath =~ m#^(/(cold[^/]+))#s ) {
    $coldPath = $1;
    $selfid   = $2;
    $codeRepo = catdir( $coldPath, 'folder-control' );
}
elsif ( $scriptPath =~ m#^(/(?:Users|Volumes)/([^/]+))#s ) {
    $homePath = $1;
    $selfid .= '-' . $2;
    $codeRepo =
      catdir( $homePath, 'Documents', 'Archive', 'folder-control-daemon' )
      unless $codeRepo && substr( $codeRepo, 0, length $homePath ) eq $homePath;
}

my $monorepoUrl = $ARGV[0];
if ($monorepoUrl) {
    $monorepoUrl = rel2abs($monorepoUrl) unless $monorepoUrl =~ /^ssh:/;
    warn "### Begun: $selfid to synchronise with $monorepoUrl\n";
}
else {
    warn "### Begun: $selfid\n";
}

my %localRepos;
( $localRepos{"Autocats/$selfid"} ) =
  sort grep { -d "$_/.git"; } $homePath
  ? (
    <"$homePath/Documents/Archive/Catalogues">,
    <"$homePath/Documents/FolderControl/Catalogues">,
    <"$homePath/Management/catalogues">,
  )
  : $coldPath ? <"$homePath/catalogues">
  :   ( grep { !/^\/cold/si; } <"/*/catalogues">, <"/share/*/*/catalogues">, );

foreach (
      $homePath ? grep { !/\(/; } <"$homePath/*/*/.git">
    : $coldPath ? ()
    : (
        grep { !/^\/cold/si; } <"/*/folder-control/.git">,
        <"/*/storage-info/.git">,
        <"/*/usage/.git">,
        <"/share/*/*/folder-control/.git">,
    )
  )
{
    m#^(.*?([^/]+))/\.git$# or next;
    chdir $1;
    warn "* Not clean: $1\n" if `git status -s`;
    $localRepos{"Work/$2"} = $1
      unless $2 eq 'FolderControl'
      || lc($2) eq 'monorepo'
      || lc($2) eq 'catalogues';
}
if ($monorepoUrl) {
    my %refsFromMonorepoAndLocal;
    foreach ( split /\n/, `git ls-remote '$monorepoUrl'` ) {
        my ( $sha1, $ref ) = split /\s+/;
        $refsFromMonorepoAndLocal{$ref}[0] = $sha1;
    }
    while ( my ( $repoKey, $path ) = each %localRepos ) {
        next unless defined $path;
        chdir $path or next;
        my @showRef = split /\n/, `git show-ref`;
        if (@showRef) {
            foreach (@showRef) {
                my ( $sha1, $ref ) = split /\s+/;
                $ref =~ s#^(refs/[^/]+)#$1/$repoKey#;
                $refsFromMonorepoAndLocal{$ref}[1] = $sha1;
            }
        }
        else {
            $refsFromMonorepoAndLocal{"refs/heads/$repoKey/master"}[1] =
              'not yet';
        }
    }
    my ( %toPull, %toPush );
    foreach ( sort keys %refsFromMonorepoAndLocal ) {
        my ( $repoKey, $branch ) = m#^refs/heads/([^/]+/[^/]+)/(.+)$# or next;
        my ( $mono,    $local )  = @{ $refsFromMonorepoAndLocal{$_} };
        next unless defined $local;
        if ( !defined $mono ) {
            $toPush{$repoKey}{$branch} = 1;
        }
        elsif ( $local ne $mono ) {
            $toPull{$repoKey}{$branch} = 1
              unless $repoKey eq "Autocats/$selfid"
              || $repoKey eq "Backup/$selfid";
            $toPush{$repoKey}{$branch} = 1 unless $local eq 'not yet';
        }
    }
    foreach my $repoKey ( keys %toPull ) {
        chdir $localRepos{$repoKey} or die $repoKey;
        foreach ( split /\n\n/, `git worktree list --porcelain` ) {
            my ( $worktreePath, $worktreeBranch ) =
              m/worktree\s*(.+)\n.*\nbranch\s*refs\/heads\/(\S+)/s
              or next;
            chdir $worktreePath or next;
            warn "* Pulling $worktreeBranch from $repoKey\n";
`git pull -q --no-tags '$monorepoUrl' refs/heads/$repoKey/$worktreeBranch`
              if delete $toPull{$repoKey}{$worktreeBranch};
        }
        if (
            my @mappings = map { "refs/heads/$repoKey/$_:refs/heads/$_"; }
            keys %{ $toPull{$repoKey} }
          )
        {
            warn '* Fetching '
              . ( @mappings > 1 ? ( @mappings . ' branches' ) : 'one branch' )
              . " from $repoKey\n";
            `git fetch -q --no-tags '$monorepoUrl' @mappings`;
        }
    }
    foreach my $repoKey ( keys %toPush ) {
        chdir $localRepos{$repoKey} or die $repoKey;
        my @mappings = map { "refs/heads/$_:refs/heads/$repoKey/$_"; }
          keys %{ $toPush{$repoKey} };
        warn '* Pushing '
          . (
            @mappings > 1
            ? ( @mappings . ' branches' )
            : 'one branch'
          ) . " to $repoKey\n";
        `git push -q --no-tags '$monorepoUrl' @mappings`;
    }
}

if ( $codeRepo && chdir $codeRepo ) {
    system qw(git stash);
    foreach (qw(Work/folder-control/master)) {
        my ( $repoKey, $branch ) = m#^([^/]+/[^/]+)/(.+)$# or next;
        system qw(git pull -q --no-edit --no-tags),
          defined $localRepos{$repoKey}
          ? ( $localRepos{$repoKey}, $branch )
          : ( $monorepoUrl, $_ );
    }
}

warn "### Ended: $selfid\n";
