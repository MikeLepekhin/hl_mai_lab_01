#include "person.h"
#include "database.h"
#include "../config/config.h"

#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>
#include <cppkafka/cppkafka.h>

#include <sstream>
#include <exception>
#include <fstream>


using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;

namespace database
{
    void Person::init()
    {
        try
        {

            Poco::Data::Session session = database::Database::get().create_session_write();
            //*
            Statement drop_stmt(session);
            drop_stmt << "DROP TABLE IF EXISTS Person", now;
            //*/

            // (re)create table
            Statement create_stmt(session);
            create_stmt << "CREATE TABLE IF NOT EXISTS `Person` (`login` VARCHAR(50) NOT NULL PRIMARY KEY,"
                        << "`first_name` VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,"
                        << "`last_name` VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,"
                        << "`age` INT NOT NULL,"
                        << "PRIMARY KEY (`login`),KEY `fn` (`first_name`),KEY `ln` (`last_name`));",
                now;
        }

        catch (Poco::Data::MySQL::ConnectionException &e)
        {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e)
        {

            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    Poco::JSON::Object::Ptr Person::toJSON() const {
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();

        root->set("login", _login);
        root->set("first_name", _first_name);
        root->set("last_name", _last_name);
        root->set("age", _age);
        return root;
    }

    Person Person::fromJSON(const std::string &str) {
        Person person;
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(str);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        person.login() = object->getValue<std::string>("login");
        person.first_name() = object->getValue<std::string>("first_name");
        person.last_name() = object->getValue<std::string>("last_name");
        person.age() = object->getValue<long>("age");
        return person;
    }

    std::optional<Person> Person::read_by_login(std::string login) {
        try {
            Poco::Data::Session session = database::Database::get().create_session_read();
            Poco::Data::Statement select(session);

            Person a;

            select << "SELECT login, first_name, last_name, age FROM Person WHERE login=?",
                into(a._login),
                into(a._first_name),
                into(a._last_name),
                into(a._age),
                use(login),
                range(0, 1);

            if (!select.done()) {
                if (!select.execute()) 
		            return std::nullopt;
            }

            return a;
        }

        catch (Poco::Data::MySQL::ConnectionException &e) {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e) {
            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    std::vector<Person> Person::read_all() {
        try {
            Poco::Data::Session session = database::Database::get().create_session_read();
            Statement select(session);
            std::vector<Person> result;
            Person a;

            select << "SELECT login, first_name, last_name, age FROM Person",
                into(a._login),
                into(a._first_name),
                into(a._last_name),
                into(a._age),
                range(0, 1); //  iterate over result set one row at a time

            while (!select.done()) {
                select.execute();
                result.push_back(a);
            }
            return result;
        }

        catch (Poco::Data::MySQL::ConnectionException &e) {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        }
        catch (Poco::Data::MySQL::StatementException &e) {
            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    std::vector<Person> Person::search(std::optional<std::string> first_name, std::optional<std::string> last_name) {
        try {
            Poco::Data::Session session = database::Database::get().create_session_read();
            Statement select(session);
            
	    std::vector<Person> result;
            
	    Person a;
            
	    std::string first_name_pattern = "%" + first_name.value_or("") + "%";
            std::string last_name_pattern = "%" + last_name.value_or("") + "%";
            
	    select << "SELECT login, first_name, last_name, age FROM persons WHERE first_name LIKE ? AND last_name LIKE ?",
                into(a._login),
                into(a._first_name),
                into(a._last_name),
                into(a._age),
                use(first_name_pattern),
                use(last_name_pattern),
                range(0, 1);

            while (!select.done()) {
                if (select.execute()) {
                    result.push_back(a);
                }
            }
            return result;
        } catch (Poco::Data::MySQL::ConnectionException &e) {
            std::cout << "connection:" << e.what() << std::endl;
            throw;
        } catch (Poco::Data::MySQL::StatementException &e) {
            std::cout << "statement:" << e.what() << std::endl;
            throw;
        }
    }

    void Person::send_to_queue() {
        cppkafka::Configuration config = {
            {"metadata.broker.list", Config::get().get_queue_host()}};

        cppkafka::Producer producer(config);
        std::stringstream ss;
        Poco::JSON::Stringifier::stringify(toJSON(), ss);
        std::string message = ss.str();
        producer.produce(cppkafka::MessageBuilder(Config::get().get_queue_topic()).partition(0).payload(message));
        producer.flush();
    }   

    void Person::save_to_mysql() {
        try {
            Poco::Data::Session session = database::Database::get().create_session_write();
            Poco::Data::Statement insert(session);

            insert << "INSERT INTO persons (login, first_name, last_name, age) VALUES(?, ?, ?, ?)",
                use(_login),
                use(_first_name),
                use(_last_name),
                use(_age);

            insert.execute();
        } catch (Poco::Data::MySQL::ConnectionException &e) {
            std::cout << "connection:" << e.what() << std::endl;
            throw std::string("connection error");
        } catch (Poco::Data::MySQL::StatementException &e) {
            std::cout << "statement:" << e.what() << std::endl;
            throw std::string("statement error");
        }
    }

    const std::string& Person::get_login() const {
        return _login;
    }

    const std::string& Person::get_first_name() const {
        return _first_name;
    }

    const std::string& Person::get_last_name() const {
        return _last_name;
    }

    long Person::get_age() const {
        return _age;
    }

    std::string& Person::login() {
        return _login;
    }

    std::string& Person::first_name() {
        return _first_name;
    }

    std::string& Person::last_name() {
        return _last_name;
    }

    long& Person::age() {
        return _age;
    }
}


