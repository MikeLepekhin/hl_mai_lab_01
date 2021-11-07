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
            std::string login;
            std::string first_name;
            std::string last_name;
            int age;

            Poco::JSON::Object::Ptr toJSON() const;
            static Person fromJSON(const std::string& str);
            static void init();
            static void warm_up_cache();

            static std::optional<Person> read_by_login(std::string login);
            static std::optional<Person> read_from_cache_by_login(std::string login);
            static std::vector<Person> read_all();
            static std::vector<Person> search(std::optional<std::string> first_name, std::optional<std::string> last_name);
            
            void save_to_mysql();
            void save_to_cache();
            static size_t size_of_cache();
    };
}

#endif
