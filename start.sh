sudo mysql -e '
    select user from mysql.user;
    create user if not exists "mike_admin" identified by "mike_admin";
'

sudo ./build/hl_mai_lab_01 --host=localhost --port=3306 --login=mike_admin --password=1234 --database=person_db

