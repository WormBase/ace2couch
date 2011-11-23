package WormBase::ModelConverter;

# converts Ace models into CouchDB views

# namespace change: WormBase::Convert, i.e. WormBase::Convert::AceModel ?

use strict;
use warnings;
use Ace;
use JSON;
use CouchDB::View::Document;
use Exporter 'import';

our @EXPORT = qw(treematrix2hash);

sub run {
    # for testing and as modulino
}

