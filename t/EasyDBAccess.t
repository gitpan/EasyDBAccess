#!/usr/bin/perl
use strict;
use Test;
BEGIN { plan tests => 105 }
use EasyDBAccess;

my $dba;
my $dbh;
my $rc;
my ($err_code,$err_detail,$err_pkg);

my $test_db_option={host=>'pig.lua.cn',usr=>'test_usr',pass=>'passwd',database=>'test_db'};
my $test_db_wrong_option={host=>'192.168.1.53',usr=>'test',pass=>'WRONG PASS',database=>'test'};

#===new

#===param count error
EasyDBAccess->once();
($dba,$err_code,$err_detail,$err_pkg)=EasyDBAccess->new('A','B','C');
ok($dba,undef,'test new 1');
ok($err_code,2,'test new 2');


#===connect database fail
EasyDBAccess->once();
($dba,$err_code,$err_detail,$err_pkg)=EasyDBAccess->new($test_db_wrong_option);

ok($dba,undef,'test new 3');
ok($err_code,3,'test new 4');

#===connect success
EasyDBAccess->once();
($dba,$err_code,$err_detail,$err_pkg)=EasyDBAccess->new($test_db_option);
ok(ref $dba,'EasyDBAccess','test new 5');
ok($err_code,0,'test new 6');
$dba->close;

#===dbh
EasyDBAccess->once();
($dba,$err_code,$err_detail,$err_pkg)=EasyDBAccess->new($test_db_option);

$dbh=$dba->dbh();
ok(ref $dbh,'DBI::db','test dbh');
$dba->close;

#===close
EasyDBAccess->once();
($dba,$err_code,$err_detail,$err_pkg)=EasyDBAccess->new($test_db_option);

$dba->close;
ok($dba,undef,'test close');

#===type
EasyDBAccess->once();
($dba,$err_code,$err_detail,$err_pkg)=EasyDBAccess->new($test_db_option);

ok($dba->type,'mysql','test type');
$dba->close;

#===execute

$EasyDBAccess::_DEBUG=0;
($dba,$err_code,$err_detail,$err_pkg)=EasyDBAccess->new($test_db_option);


#===sql null error
($rc,$err_code,$err_detail,$err_pkg)=$dba->execute(undef,[],{start_pos=>1});
ok($rc,undef);ok($err_code,2);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select(undef,[],{start_pos=>1});
ok($rc,undef);ok($err_code,2);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_row(undef,[],{start_pos=>1});
ok($rc,undef);ok($err_code,2);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_col(undef,[],{start_pos=>1});
ok($rc,undef);ok($err_code,2);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_one(undef,[],{start_pos=>1});
ok($rc,undef);ok($err_code,2);ok($err_pkg,'EasyDBAccess');

#===there ok a null value in inline_param
($rc,$err_code,$err_detail,$err_pkg)=$dba->execute('SELECT * FROM RES LIMIT %start_pos,1',[],{start_pos=>undef});
ok($rc,undef);ok($err_code,2);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select('SELECT * FROM RES LIMIT %start_pos,1',[],{start_pos=>undef});
ok($rc,undef);ok($err_code,2);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_row('SELECT * FROM RES LIMIT %start_pos,1',[],{start_pos=>undef});
ok($rc,undef);ok($err_code,2);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_col('SELECT * FROM RES LIMIT %start_pos,1',[],{start_pos=>undef});
ok($rc,undef);ok($err_code,2);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_one('SELECT * FROM RES LIMIT %start_pos,1',[],{start_pos=>undef});
ok($rc,undef);ok($err_code,2,);ok($err_pkg,'EasyDBAccess');

#===sql execute error
($rc,$err_code,$err_detail,$err_pkg)=$dba->execute('SELECT FROM RES LIMIT %start_pos,1',[],{start_pos=>1});
ok($rc,undef);ok($err_code,5);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select('SELECT FROM RES LIMIT %start_pos,1',[],{start_pos=>1});
ok($rc,undef);ok($err_code,5);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_row('SELECT FROM RES LIMIT %start_pos,1',[],{start_pos=>1});
ok($rc,undef);ok($err_code,5);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_col('SELECT FROM RES LIMIT %start_pos,1',[],{start_pos=>1});
ok($rc,undef);ok($err_code,5);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_one('SELECT FROM RES LIMIT %start_pos,1',[],{start_pos=>1});
ok($rc,undef);ok($err_code,5);ok($err_pkg,'EasyDBAccess');

#===execute on a none select sql
($rc,$err_code,$err_detail,$err_pkg)=$dba->execute('DROP TABLE IF EXISTS HELLO');
ok($rc,'0E0');ok($err_code,0);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select('DROP TABLE IF EXISTS HELLO');
ok($rc,undef);ok($err_code,5);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_row('DROP TABLE IF EXISTS HELLO');
ok($rc,undef);ok($err_code,5);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_col('DROP TABLE IF EXISTS HELLO');
ok($rc,undef);ok($err_code,5);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_one('DROP TABLE IF EXISTS HELLO');
ok($rc,undef);ok($err_code,5);ok($err_pkg,'EasyDBAccess');

#===SQL execute success

($rc,$err_code,$err_detail,$err_pkg)=$dba->execute('DROP TABLE IF EXISTS PERSON');
ok($rc,'0E0');ok($err_code,0);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->execute('CREATE TABLE PERSON(NAME VARCHAR(255) NOT NULL,AGE INT NOT NULL)');
ok($rc,'0E0');ok($err_code,0);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_row('SELECT * FROM PERSON ORDER BY NAME ASC');
ok($rc,undef);ok($err_code,1);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_one('SELECT NAME FROM PERSON ORDER BY NAME ASC');
ok($rc,undef);ok($err_code,1);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->execute('INSERT INTO PERSON VALUES(?,?)',['QIAN YU',23]);
ok($rc,1);ok($err_code,0);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->execute('INSERT INTO PERSON VALUES(?,?)',['GAO LEI',23]);
ok($rc,1);ok($err_code,0);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->execute('INSERT INTO PERSON VALUES(?,?)',['FAN WEN BO',24]);
ok($rc,1);ok($err_code,0);ok($err_pkg,'EasyDBAccess');
($rc,$err_code,$err_detail,$err_pkg)=$dba->select('SELECT * FROM PERSON ORDER BY NAME ASC');
ok(EasyDBAccess::_dump($rc),EasyDBAccess::_dump(
[
  {"name" => "FAN WEN BO", "age" => 24},
  {"name" => "GAO LEI", "age" => 23},
  {"name" => "QIAN YU", "age" => 23},
]));
ok($err_code,0);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_row('SELECT * FROM PERSON ORDER BY NAME ASC');
ok(EasyDBAccess::_dump($rc),EasyDBAccess::_dump(
{"name" => "FAN WEN BO", "age" => 24}
));
ok($err_code,0);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_one('SELECT NAME FROM PERSON ORDER BY NAME ASC');
ok($rc,"FAN WEN BO");
ok($err_code,0);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->select_col('SELECT NAME FROM PERSON ORDER BY NAME ASC');
ok(EasyDBAccess::_dump($rc),EasyDBAccess::_dump(
["FAN WEN BO","GAO LEI","QIAN YU"]));
ok($err_code,0);ok($err_pkg,'EasyDBAccess');

($rc,$err_code,$err_detail,$err_pkg)=$dba->execute('DROP TABLE IF EXISTS PERSON');
ok($rc,'0E0');ok($err_code,0);ok($err_pkg,'EasyDBAccess');

