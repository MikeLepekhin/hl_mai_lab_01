#ifndef CACHE_THICK_H
#define CACHE_THICK_H

#include <string>
#include <memory>
#include <ignite/ignite.h>
#include <ignite/ignition.h>

namespace database
{
    class CacheThick {
        private:
            ignite::Ignite &_client;
            ignite::cache::Cache<std::string, std::string> _cache;
            CacheThick(ignite::Ignite &client);

        public:
            static CacheThick get();

            void put(const std::string& login, const std::string& val);
            bool get(const std::string& login, std::string& val);
            size_t size();

            void remove(const std::string& login);
            void remove_all();
            ~CacheThick();
    };
}

#endif
