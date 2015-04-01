#ifndef UTILS_H
#define UTILS_H
#include <curl/curl.h>
#include <openssl/crypto.h>
#include <string>
#include <sstream>
#include <cstdint>
#include <vector>
#include <iostream>
#include "pugixml.hpp"
#include <exception>
#include <stdexcept>
#include <thread>
#include <fstream>
#include <mutex>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/filesystem.hpp>

#define ForEachI(cont, ptr) for(auto ptr = cont.begin(); ptr != cont.end(); ptr++)

typedef uint32_t Id;
typedef uint32_t Vertex;
typedef std::vector<Vertex> Neighboors;
typedef std::pair<Vertex, Neighboors> AdjacElem;
typedef std::vector<AdjacElem> AdjacList;
const int VK_ERR_ACCESS_DENIED = 15;

namespace utils {
    class Error : public std::runtime_error
    {
    public:
            Error(const std::string& msg) : runtime_error(msg) {}
    };

    class HttpError : public std::runtime_error
    {
    public:
            HttpError(const std::string& msg) : runtime_error(msg) {}
    };

    class FileSysError : public std::runtime_error
    {
    public:
        FileSysError(const std::string& msg) : runtime_error(msg) {}
    };

    class CurlError : public std::runtime_error
    {
    public:
        CurlError(const std::string& msg) : runtime_error(msg) {}
    };

    template<typename Waiter>
    class WaitUntil
    {
    public:
        WaitUntil(const Waiter& waiter, int time) : _waiter(waiter), _time(time) {}

        operator bool() {
            while(_time > 0) {
                if (_waiter())
                    return true;
                --_time;
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            return false;
        }

        std::string GetResponse() {
            return _waiter.Response();
        }

    private:
        int _time;
        Waiter _waiter;
    };

    template<typename Number>
    std::string toStr(Number num)
    {
        std::stringstream stream;
        stream << num;
        return stream.str();
    }

    template<typename String>
    Id toId(String s)
    {
        std::stringstream stream(s);
        Id result;
        stream >> result;
        return result;
    }

    template<typename String>
    int toInt(String s)
    {
        std::stringstream stream(s);
        int result;
        stream >> result;
        return result;
    }

    struct sort_pred {
        bool operator()(const std::pair<int,double> &left, const std::pair<int,double> &right) {
            return left.second < right.second;
        }
    };

    size_t write_data(char *ptr, size_t size, size_t nmemb, void *userdata) {
        std::ostringstream *stream = (std::ostringstream*)userdata;
        size_t count = size * nmemb;
        stream->write(ptr, count);
        return count;
    }

    class HttpQuery
    {
    public:
        HttpQuery(const std::string& url) {
            CRYPTO_set_locking_callback(locking_function);
            CRYPTO_THREADID_set_callback(threadid_func);
            curl = curl_easy_init();
            while(!curl) {
                std::cerr << "Новая попытка инициализировать curl" << std::endl;
                curl = curl_easy_init();
            }

            curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_HEADER, 0);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data );
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &_stream);
        }

        HttpQuery& Query()
        {
            curl_easy_perform(curl);
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &_http_code);
            return *this;
        }

        long GetCode() {
            return _http_code;
        }

        std::string Str()
        {
            return _stream.str();
        }

        ~HttpQuery() {
            curl_easy_cleanup( curl );
        }

        static void locking_function(int mode, int n, const char *file, int line)
        {
                if (mode & CRYPTO_LOCK) {
                        mutex_array[n].lock();
                } else {
                        mutex_array[n].unlock();
                }
        }

        static void threadid_func(CRYPTO_THREADID *id)
        {
                std::stringstream str;
                str << std::this_thread::get_id();
                unsigned long this_id;
                str >> this_id;
                CRYPTO_THREADID_set_numeric(id, this_id);
        }

    private:
        std::ostringstream _stream;
        static std::vector<std::mutex> mutex_array;
        long _http_code;
        CURL *curl;
    };

    std::vector<std::mutex> HttpQuery::mutex_array(CRYPTO_num_locks());

    class ResponseWaiter
    {
    public:
        ResponseWaiter(const std::string url) : _url(url) {}

        virtual std::string Response() const {
            return _response;
        }

    protected:
        const std::string _url;
        std::string _response;
    };

    class HttpPageWaiter : public ResponseWaiter
    {
    public:
        HttpPageWaiter(const std::string url) : ResponseWaiter(url) {}

        bool operator()() {
            HttpQuery http_query(_url);
            if (http_query.Query().GetCode() != 200)
                return false;

            _response = http_query.Str();
            return true;
        }
    };

    class VkXmlPageWaiter : public ResponseWaiter
    {
    public:
        VkXmlPageWaiter(const std::string url) : ResponseWaiter(url) {}

        bool operator()() {
            WaitUntil<HttpPageWaiter> wait_until(HttpPageWaiter(_url), 300);
            if (!wait_until) throw HttpError("Код HTTP ответа запрашиваемой страницы не равен 200");
            _response = wait_until.GetResponse();

            pugi::xml_document doc;
            pugi::xml_parse_result result = doc.load(_response.c_str());
            if (!result) return false;

            pugi::xpath_node error_code = doc.select_single_node("/error/error_code");
            pugi::xpath_node error_msg = doc.select_single_node("/error/error_msg");
            if (error_code || error_msg) {
                int code = toInt(error_code.node().child_value());
                std::string msg = error_msg.node().child_value();
                if(code == 15 && msg.find("user deactivated") != msg.npos) {
                    return true;
                } else {
                    return false;
                }
            }
            return true;
        }
    };

    class VkCatalogPageWaiter : public ResponseWaiter
    {
    public:
        VkCatalogPageWaiter(const std::string url) : ResponseWaiter(url) {}

        bool operator()() {
            WaitUntil<HttpPageWaiter> wait_until(HttpPageWaiter(_url), 300);
            if (!wait_until) throw HttpError("Код HTTP ответа запрашиваемой страницы не равен 200");
            _response = wait_until.GetResponse();

            const std::string pattern = "<a href=\"id";
            size_t pos = _response.find(pattern);
            size_t quote_pos = _response.find('"', pos+pattern.length());
            return (quote_pos != std::string::npos && pos != std::string::npos);
        }
    };

    std::string VkCatalogRequest(const std::string& url) {
        WaitUntil<VkCatalogPageWaiter> wait_until(VkCatalogPageWaiter(url), 3);
        if (!wait_until) throw Error("Не найден тэг <a href=id по адресу: " + url);
        return wait_until.GetResponse();
    }

    std::string VkXmlRequest(const std::string& url) {
        std::cerr << url << std::endl;
        WaitUntil<VkXmlPageWaiter> wait_until(VkXmlPageWaiter(url), 3);
        if (!wait_until) throw Error("Получен error_code не равный 15 или ошибка при выполнении doc.load");
        return wait_until.GetResponse();
    }
} //end namespace
#endif // UTILS_H
