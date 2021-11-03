sudo mysql -e '
    select user from mysql.user;
    create user if not exists "mike_admin" identified by "mike_admin";

    select * from person_db.persons;
    select * from person_db.persons where first_name == "Mikhail";	    
    select * from person_db.persons where login == "vasya";	    
    '
