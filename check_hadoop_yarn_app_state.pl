#!/usr/bin/perl -T

$DESCRIPTION = "Nagios Plugin to check Hadoop Yarn app state via the Resource Manager's REST API

Optional thresholds are applied to the number of running apps on the cluster.";

$VERSION = "0.1";

use strict;
use warnings;
BEGIN {
    use File::Basename;
    use lib dirname(__FILE__) . "/lib";
}
use HariSekhonUtils;
use Data::Dumper;
use JSON::XS;
use LWP::Simple '$ua';

$ua->agent("Hari Sekhon $progname version $main::VERSION");

set_port_default(8088);

env_creds(["HADOOP_YARN_RESOURCE_MANAGER", "HADOOP"], "Yarn Resource Manager");

my $appname;

%options = (
    %hostoptions,
    %thresholdoptions,
    "a|application=s"      =>  [ \$appname,  "Application name" ],
);

get_options();

if(!defined($appname)){
        usage "no valid application specified";
}

$host       = validate_host($host);
$port       = validate_port($port);
validate_thresholds(0, 0, { "simple" => "upper", "positive" => 1, "integer" => 0 });

vlog2;
set_timeout();

$status = "OK";


my $url = "http://$host:$port/ws/v1/cluster/apps?states=running";

my $content = curl $url;

try{
    $json = decode_json $content;
};
catch{
    quit "invalid json returned by Yarn Resource Manager at '$url'";
};
vlog3(Dumper($json));

$msg = "app stats: ";
my @stats = get_field_array("apps.app");

my %stats;
my $state;
my $name;
foreach (@stats){
    $name = lc(substr(get_field2($_, "name"), 0, -2));
    $state = get_field2($_, "state");
    $stats{$name}=$state;
}

$msg = "Application $appname is running";

$state = lc($stats{lc($appname)});
if($state ne "running"){
    if ($status ne "CRITICAL") {
        $msg = "Application $appname not running";
        $status = "CRITICAL";
    }
}
quit $status, $msg;
