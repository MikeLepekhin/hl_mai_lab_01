#ifndef USER_H
#define USER_H

#include <string>
#include <vector>
#include <optional>
#include "Poco/JSON/Object.h"

namespace database
{
    class Person {
        private:
            std::string _login;
            std::string _first_name;
            std::string _last_name;
            long _age;

        public:

            static Person fromJSON(const std::string& str);
            static void preload(const std::string & str);

            const std::string& get_login() const;
            const std::string& get_first_name() const;
            const std::string& get_last_name() const;
            long get_age() const;

            std::string& login();
            std::string& first_name();
            std::string& last_name();
            long& age();


            static void init();
            static std::optional<Person> read_by_login(std::string login);
            static std::vector<Person> read_all();
            static std::vector<Person> search(std::optional<std::string> first_name, std::optional<std::string> last_name);
            void save_to_mysql();

            Poco::JSON::Object::Ptr toJSON() const;
    };
}

#endif
