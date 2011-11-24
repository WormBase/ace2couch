use strict;
use warnings;

use Ace;
use WormBase::ModelConverter;
use AD::Couch;

my $ace = Ace->connect(-host => 'dev.wormbase.org', -port => 2005)
    or die 'Connection error: ', Ace->error;
my $couch = AD::Couch->new(
    host     => 'localhost',
    port     => 5984,
    database => 'test',
) or die "Connection error on CouchDB\n";

for my $model (get_models($ace)) {
    print $model->name, "\n";
    my $ddoc = model2designdoc($model);
    $couch->add_doc($ddoc);
}

print $INC{'Ace/Model.pm'}, "\n";
