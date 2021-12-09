#ifndef USER_H
#define USER_H

#include <string>
#include <vector>
#include <optional>
#include "Poco/JSON/Object.h"

namespace database
{
    class Person {
        public:
            std::string _login;
            std::string _first_name;
            std::string _last_name;
            long _age;

            static Person fromJSON(const std::string& str);

            const std::string& get_login() const;
            const std::string& get_first_name() const;
            const std::string& get_last_name() const;
            long get_age() const;            

            std::string& login();
            std::string& first_name();
            std::string& last_name();
            long& age();

            Poco::JSON::Object::Ptr toJSON() const;
            
            static void init();

            static std::optional<Person> read_by_login(std::string login);
            static std::vector<Person> search(std::optional<std::string> first_name, std::optional<std::string> last_name);
            static std::vector<Person> read_all();
            void save_to_mysql();
            void send_to_queue();
    };
}

#endif
