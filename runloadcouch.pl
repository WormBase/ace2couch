#!/usr/bin/perl
# runloadcouch.pl

# run NUM_FORKS loaders at a time

use strict;
use warnings;
use File::Spec;
use Parallel::ForkManager;

use constant NUM_FORKS => 8;

my $dir = shift;
unless (-e $dir and -d $dir) {
    die "$dir is not a directory\n";
}

my $pm = Parallel::ForkManager->new(NUM_FORKS);

mkdir 'logs';
mkdir 'err';

opendir(my $dirh, $dir);
my @files = sort readdir($dirh);
for my $base (@files) {
    next unless $base =~ /\.jace$/;
    my $file = File::Spec->catfile($dir, $base);

    $pm->start and next;
    my $cmd = qq(perl loadcouch.pl --q @ARGV "$file" > "logs/$base.log" 2> "err/$base.err");
    print $cmd, "\n";
    system($cmd);
    print 'Done ', $base, "\n";
    $pm->finish;
}

$pm->wait_all_children;
