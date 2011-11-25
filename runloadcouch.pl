#!/usr/bin/perl
# runloadcouch.pl

# run NUM_FORKS loaders at a time

use strict;
use warnings;
use File::Spec;
use Parallel::ForkManager;

use constant NUM_FORKS => 3; # number of cores available
my $EXTENSION = ".jace";
my $DB_PREFIX = "ws228_experimental_";

my $dir = shift or die "Need directory\n";
unless (-e $dir and -d $dir) {
    die "$dir is not a directory\n";
}

my $pm = Parallel::ForkManager->new(NUM_FORKS);

mkdir 'logs';
mkdir 'err';

opendir(my $dirh, $dir);
my @files = sort readdir($dirh);
for my $base (@files) {
    next unless $base =~ s/\Q$EXTENSION\E$//;
    my $file = File::Spec->catfile($dir, $base . $EXTENSION);

    $pm->start and next;
    my $cmd = qq(perl loadcouch.pl --db "${DB_PREFIX}\L${base}\E" --q @ARGV "$file" > "logs/$base.log" 2> "err/$base.err");
    print $cmd, "\n";
    system($cmd);
    print 'Done ', $base, "\n";
    $pm->finish;
}

$pm->wait_all_children;
