use common::sense;

use lib '.';
use Ace::Model;
use Ace;
use AnyEvent::CouchDB;
use WormBase::Convert::AceModel;
use Try::Tiny;

my $db = shift or die "Need DB\n";

die 'Require modified Model.pm but got another from ', $INC{'Ace/Model.pm'}, "\n"
    unless $INC{'Ace/Model.pm'} eq 'Ace/Model.pm';

my $ace = Ace->connect(-host => 'dev.wormbase.org', -port => 2005)
    or die 'Connection error', Ace->error;

my $couchconn = AnyEvent::CouchDB->new('http://localhost:5984/');
my $couch     = $couchconn->db($db);
FINDDB: {
    my $dbs = $couchconn->all_dbs->recv;
    foreach (@$dbs) {
        last FINDDB if $db eq $_;
    }
    $couch->create->recv;
}

for my $model (get_models($ace)) {
    for my $pdoc ( model2pathdocs($model) ) {
        try {
            my $exist = $couch->open_doc($pdoc->{_id})->recv;
            $pdoc->{_rev} = $exist->{_rev};
        }
        catch {
            when (!/404 - Object Not Found/) { warn $_ }
        };
        $couch->save_doc($pdoc)->recv;
    }
}
