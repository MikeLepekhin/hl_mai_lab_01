#ifndef USERHANDLER_H
#define USERHANDLER_H

#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Timestamp.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/Exception.h"
#include "Poco/ThreadPool.h"
#include "Poco/Util/ServerApplication.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include "Poco/JSON/Object.h"
#include "Poco/JSON/Parser.h"
#include <iostream>
#include <iostream>
#include <fstream>

using Poco::DateTimeFormat;
using Poco::DateTimeFormatter;
using Poco::Dynamic::Var;
using Poco::ThreadPool;
using Poco::Timestamp;
using Poco::Net::HTMLForm;
using Poco::Net::HTTPRequestHandler;
using Poco::Net::HTTPRequestHandlerFactory;
using Poco::Net::HTTPServer;
using Poco::Net::HTTPServerParams;
using Poco::Net::HTTPServerRequest;
using Poco::Net::HTTPServerResponse;
using Poco::Net::NameValueCollection;
using Poco::Net::ServerSocket;
using Poco::Util::Application;
using Poco::Util::HelpFormatter;
using Poco::Util::Option;
using Poco::Util::OptionCallback;
using Poco::Util::OptionSet;
using Poco::Util::ServerApplication;
using Poco::JSON::Object;
using Poco::JSON::Parser;

#include "../../database/person.h"

void MakeResponseBody(HTTPServerResponse &response, const std::string& body) {
    std::ostream &ostr = response.send();
    ostr << body;
}

void MakeResponse400(HTTPServerResponse &response) {
	response.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
	response.setContentLength(0);
	response.send();
}

void MakeResponse404(HTTPServerResponse &response) {
	response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
	response.setContentLength(0);
	response.send();
}

void MakeResponse409(HTTPServerResponse &response) {
	response.setStatus(Poco::Net::HTTPResponse::HTTP_CONFLICT);
	response.setContentLength(0);
	response.send();
}

void MakeResponse413(HTTPServerResponse &response) {
	response.setStatus(Poco::Net::HTTPResponse::HTTP_REQUEST_ENTITY_TOO_LARGE);
	response.setContentLength(0);
	response.send();
}

void MakeResponse500(HTTPServerResponse &response) {
	response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
	response.setContentLength(0);
	response.send();
}

class PersonHandler : public HTTPRequestHandler {
private:

    void GetByLogin(HTMLForm &form, HTTPServerResponse &response) const {
        try {
            std::optional<database::Person> result = database::Person::read_by_login(form.get("login").c_str());
            if (result) {
                std::ostream &ostr = response.send();
                Poco::JSON::Stringifier::stringify(result->toJSON(), ostr);
            } else {
                MakeResponse404(response);
            }
        } catch (std::exception& s) {
            MakeResponse500(response);
        }
    }

    void GetByNamePattern(HTMLForm &form, HTTPServerResponse &response) const {
        try {
            std::optional<std::string> fn = form.has("first_name") ? std::make_optional(form.get("first_name")) : std::nullopt;
            std::optional<std::string> ln = form.has("last_name") ? std::make_optional(form.get("last_name")) : std::nullopt;
            auto results = database::Person::search(fn, ln);
            Poco::JSON::Array arr;
            for (auto s : results)
                arr.add(s.toJSON());
            std::ostream &ostr = response.send();
            Poco::JSON::Stringifier::stringify(arr, ostr);
        } catch (...) {
            MakeResponse500(response);
        }
    }

    void CreatePerson(HTTPServerRequest &request, HTTPServerResponse &response) const {
        std::istream& client_body = request.stream();
		int length_msg = request.getContentLength();
		if (length_msg > 1024) {
		    MakeResponse413(response);
		    return;
		}
		char buffer[1025];
		client_body.read(buffer, length_msg);
		buffer[length_msg] = '\n';
		std::cout << buffer << std::endl;
        
	Parser parser;
        Var result;
        
	try {
            result = parser.parse(buffer);
        } catch (std::exception& exc) {
            MakeResponse400(response);
            return;
        }

        Object::Ptr object = result.extract<Object::Ptr>();
        database::Person person;
        
	try {
            person.login = object->get("login").toString();
            person.first_name = object->get("first_name").toString();
            person.last_name = object->get("last_name").toString();
            person.age = std::atoi(object->get("age").toString().c_str());
        } catch (...) {
            MakeResponse400(response);
            return;
        }

        try {
            person.save_to_mysql();
        } catch (const std::string& exc) {
            if (exc == "statement error") {
                // Already exists
                MakeResponse409(response);
                return;
            }
            MakeResponse500(response);
            return;
        }
        MakeResponseBody(response, "{\"created\":true}");
    }

public:
    PersonHandler(const std::string &format) : _format(format) {
    }

    void handleRequest(HTTPServerRequest &request,
                       HTTPServerResponse &response) {
        HTMLForm form(request);

        response.setChunkedTransferEncoding(true);
        response.setContentType("application/json");

        if (request.getMethod() == "GET") {
            if (form.has("login")) {
                GetByLogin(form, response);
                return;
            }
            if ((form.has("first_name") && form.get("first_name").length() > 0) ||
                (form.has("last_name") && form.get("last_name").length() > 0)) {
                GetByNamePattern(form, response);
                return;
            }
        }

        if (request.getMethod() == "POST") {
            CreatePerson(request, response);
            return;
        }

        MakeResponse400(response);
    }

private:
    std::string _format;
};
#endif // !USERHANDLER_H
