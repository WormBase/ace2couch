use strict;
use warnings;

use AnyEvent::CouchDB;
use Ace;
use WormBase::ModelConverter;

my $db    = shift or die "Need DB\n";
my $class = shift or die "Need class\n";

my $ace = Ace->connect(-host => 'dev.wormbase.org', -port => 2005)
    or die 'Connection error: ', Ace->error;

my $couchconn = AnyEvent::CouchDB->new('http://localhost:5984/');
my $couch     = $couchconn->db($db);
FINDDB: {
    my $dbs = $couchconn->all_dbs->recv;
    foreach (@$dbs) {
        last FINDDB if $db eq $_;
    }
    $couch->create->recv;
}

my $model = $ace->model($class) or die "Could not fetch model for class $class\n";
my $ddoc = model2designdoc($model);
$couch->save_doc($ddoc)->recv;

print $INC{'Ace/Model.pm'}, "\n";
