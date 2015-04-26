#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'Hadoop::Oozie::Job' ) || print "Bail out!\n";
}

diag( "Testing Hadoop::Oozie::Job $Hadoop::Oozie::Job::VERSION, Perl $], $^X" );
