#include "person.h"
#include "database.h"
#include "cache.h"
#include "../config/config.h"

#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <sstream>
#include <exception>


using namespace Poco::Data::Keywords;
using Poco::Data::Session;
using Poco::Data::Statement;

namespace database
{

    Poco::JSON::Object::Ptr Person::toJSON() const {
        Poco::JSON::Object::Ptr root = new Poco::JSON::Object();

        root->set("login", login);
        root->set("first_name", first_name);
        root->set("last_name", last_name);
        root->set("age", age);
        return root;
    }

    Person Person::fromJSON(const std::string &str) {
        Person person;
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(str);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();

        person.login = object->getValue<std::string>("login");
        person.first_name = object->getValue<std::string>("first_name");
        person.last_name = object->getValue<std::string>("last_name");
        person.age = object->getValue<int>("age");
        return person;
    }

    void Person::init() {
        try {

            Poco::Data::Session session = database::Database::get().create_session();
            
	    Statement drop_stmt(session);
            drop_stmt << "DROP TABLE IF EXISTS persons", now;
            
	    Statement create_stmt(session);
            create_stmt << "CREATE TABLE IF NOT EXISTS persons ("
                           "   login CHAR(50) NOT NULL PRIMARY KEY,"
                           "   first_name CHAR(50) NOT NULL,"
                           "   last_name CHAR(50) NOT NULL,"
                           "   age INT NOT NULL"
                           " );",
                           now;
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

    std::optional<Person> Person::read_by_login(std::string login) {
        try {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement select(session);

            Person a;

            select << "SELECT login, first_name, last_name, age FROM persons WHERE login=?",
                into(a.login),
                into(a.first_name),
                into(a.last_name),
                into(a.age),
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


    std::optional<Person> Person::read_from_cache_by_login(std::string login) {
        try {
            std::string result;

            if (database::Cache::get().get(login, result))
                return fromJSON(result);
            else
                throw std::logic_error("key not found in the cache");
        } catch (std::exception err) {
            //std::cout << "error:" << err.what() << std::endl;
            throw;
        }
    }

    
    std::vector<Person> Person::read_all() {
        try {
            Poco::Data::Session session = database::Database::get().create_session();
            Statement select(session);
            std::vector<Person> result;
            Person a;

            select << "SELECT login, first_name, last_name, age FROM persons",
                into(a.login),
                into(a.first_name),
                into(a.last_name),
                into(a.age),
                range(0, 1); //  iterate over result set one row at a time

            while (!select.done()) {
                select.execute();
                result.push_back(a);
            }
            return result;
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

    void Person::warm_up_cache() {
        std::cout << "wharming up persons cache ...";
        auto array = read_all();
        long count = 0;

        for (auto &a : array) {
            a.save_to_cache();
            ++count;
        }
        std::cout << "done: " << count << std::endl;
    }


    size_t Person::size_of_cache() {
        return database::Cache::get().size();
    }

    void Person::save_to_cache() {
        std::stringstream ss;
        Poco::JSON::Stringifier::stringify(toJSON(), ss);
        std::string message = ss.str();
        database::Cache::get().put(login, message);
    }

    std::vector<Person> Person::search(std::optional<std::string> first_name, std::optional<std::string> last_name) {
        try {
            Poco::Data::Session session = database::Database::get().create_session();
            Statement select(session);
            
	    std::vector<Person> result;
            
	    Person a;
            
	    std::string first_name_pattern = "%" + first_name.value_or("") + "%";
            std::string last_name_pattern = "%" + last_name.value_or("") + "%";
            
	    select << "SELECT login, first_name, last_name, age FROM persons WHERE first_name LIKE ? AND last_name LIKE ?",
                into(a.login),
                into(a.first_name),
                into(a.last_name),
                into(a.age),
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

   
    void Person::save_to_mysql() {
        try {
            Poco::Data::Session session = database::Database::get().create_session();
            Poco::Data::Statement insert(session);

            insert << "INSERT INTO persons (login, first_name, last_name, age) VALUES(?, ?, ?, ?)",
                use(login),
                use(first_name),
                use(last_name),
                use(age);

            insert.execute();
        } catch (Poco::Data::MySQL::ConnectionException &e) {
            std::cout << "connection:" << e.what() << std::endl;
            throw std::string("connection error");
        } catch (Poco::Data::MySQL::StatementException &e) {
            std::cout << "statement:" << e.what() << std::endl;
            throw std::string("statement error");
        }
    }
}
