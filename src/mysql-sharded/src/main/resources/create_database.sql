drop database if exists user_database;

create database user_database;

grant all privileges on user_database.* to ''@'%';

use user_database;

drop table if exists user_table;

create table user_table(
  ycsb_key varchar(32) primary key,
  field1 varchar(100), field2 varchar(100), field3 varchar(100), field4 varchar(100), field5 varchar(100),
  field6 varchar(100), field7 varchar(100), field8 varchar(100), field9 varchar(100), field10 varchar(100)) engine=myisam;

drop database test;

show create table user_table;