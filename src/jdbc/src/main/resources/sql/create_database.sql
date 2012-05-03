drop database if exists user_database;

create database user_database;
use user_database;

drop table if exists user_table;

create table user_table(
  ycsb_key varchar (255) primary key,
  field1 text, field2 text, field3 text, field4 text, field5 text,
  field6 text, field7 text, field8 text, field9 text, field10 text);
