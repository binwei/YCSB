drop database if exists user_database;
create database user_database;
grant all privileges on user_database.* to ''@'%';

use user_database;

drop table if exists user_table;
create table user_table(
  ycsb_key varchar(32) primary key,
  field1 varchar(100), field2 varchar(100), field3 varchar(100), field4 varchar(100),
  field5 varchar(100), field6 varchar(100), field7 varchar(100), field8 varchar(100),
  field9 varchar(100), field10 varchar(100))
  max_rows=1000000000 engine=ndbcluster partition by key(ycsb_key);

-- initial size 1073741824 bytes or 1 gigabytes, undo buffer size 134217728 bytes or 128 megabytes
create logfile group log_group add undofile 'log_group1.log' initial_size=1073741824 undo_buffer_size=134217728 engine=ndbcluster;
alter logfile group log_group add undofile 'log_group2.log' initial_size=1073741824 engine=ndbcluster;

create tablespace user_table_space add datafile 'user_table_space_data1.dat' use logfile group log_group initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data2.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data3.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data4.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data5.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data6.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data7.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data8.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data9.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data10.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data11.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data12.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data13.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data14.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data15.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data16.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data17.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data18.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data19.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data20.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data21.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data22.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data23.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data24.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data25.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data26.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data27.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data28.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data29.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data30.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data31.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data32.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data33.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data34.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data35.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data36.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data37.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data38.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data39.dat' initial_size=1073741824 engine=ndbcluster;
alter tablespace user_table_space add datafile 'user_table_space_data40.dat' initial_size=1073741824 engine=ndbcluster;

alter table user_table tablespace user_table_space storage disk engine=ndbcluster;

show create table user_table;