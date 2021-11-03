sudo mysql -e '
    select user from mysql.user;
    create user if not exists "mike_admin" identified by "mike_admin";

    create schema if not exists person_db;

    drop table if exists person_db.persons;

    create table if not exists person_db.persons (
      login char(50) not null primary key,
      first_name char(50) not null,
      last_name char(50) not null,
      age int not null
    );

    describe person_db.persons;

    insert into person_db.persons values(
      "mike", "Mikhail", "Lepekhin", 22
    );

    insert into person_db.persons values(
      "vasya", "Vasiliy", "Ivanov", 20
    );

    select * from person_db.persons;
    '
