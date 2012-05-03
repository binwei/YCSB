drop database if exists user_database;

create database user_database;

grant all privileges on user_database.* to ''@'%';

use user_database;

drop table if exists user_table;

create table user_table(
  ycsb_key varchar(255) primary key,
  field1 varchar(255), field2 varchar(255), field3 varchar(255), field4 varchar(255), field5 varchar(255),
  field6 varchar(255), field7 varchar(255), field8 varchar(255), field9 varchar(255), field10 varchar(255)) engine=myisam;

drop database test;

show create table user_table;