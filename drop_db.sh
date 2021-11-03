
sudo mysql -e '
    select user from mysql.user;
    create user if not exists "mike_admin" identified by "mike_admin";
    create schema if not exists person_db;
    drop table if exists person_db.persons;
    '
