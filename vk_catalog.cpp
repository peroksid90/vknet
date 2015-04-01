#include "utils.h"
#include <iostream>
#include <set>
#include <fstream>
#include <map>
#include <omp.h>
#include <iterator>
#include <algorithm>
#include "pugixml.hpp"
using namespace utils;

class TestLostUser
{
public:
    TestLostUser(const std::string& file_path) : _file(file_path) {
        std::ifstream read_lost_id(_file.c_str());
        int val;
        while(read_lost_id >> val)
            _vec_id.push_back(val);
    }
    void Run() {
        #pragma omp parallel for num_threads(100)
        for(int i = 0; i < _vec_id.size(); i++) {
            if(i % 1000 == 0)
                std::cerr << i << std::endl;
            pugi::xml_document doc;
            std::string url = "https://api.vk.com/method/friends.get.xml?user_id="+utils::toStr(_vec_id[i])+"&v=5.25";
            doc.load(utils::HttpQuery(url).Query().Str().c_str());
            pugi::xpath_node error_code = doc.select_single_node("/error/error_code");
            pugi::xpath_node error_msg = doc.select_single_node("/error/error_msg");

            if (error_code || error_msg) {
                if(toInt(error_code.node().child_value()) != 15)
                    std::cerr << "TEST FAILED FOR ID: " << _vec_id[i] << " ERROR CODE != 15" << std::endl;
                //else
                    //std::cerr << "TEST PASSED FOR ID " << _vec_id[i] << std::endl;
            } else {
                std::cerr << "TEST FAILED FOR ID: " << _vec_id[i] << " ERROR CODE NOT FOUND" << std::endl;
            }
        }
    }
private:
    std::string _file;
    std::vector<int> _vec_id;
};

#ifdef VKTEST
int main(int argc, char *argv[]) {
    TestLostUser test_lost_user(argv[1]);
    test_lost_user.Run();
}
#else
int main() {
    const std::string pattern = "<a href=\"id";

    std::ifstream read_lost_id("lost_id.txt");
    std::set<int> set_id;
    int val;
    while(read_lost_id >> val)
        set_id.insert(val);
    read_lost_id.close();

    int max_val = 0;
    if (!set_id.empty()) {
        max_val = *set_id.rbegin();
    }

    int i_start = max_val / 1000000;
    int j_start = (max_val / 10000) % 100;

    auto itlow=set_id.lower_bound (1000000 * i_start + 10000 * j_start);

    if (itlow != set_id.end()) {
        std::ofstream write_lost_id("lost_id.txt");
        std::ostream_iterator<int> out_it (write_lost_id, " ");
        std::copy ( set_id.begin(), itlow, out_it );
        write_lost_id.close();
    }

    for(int i = i_start; i < 275; i++) {
        std::cout << "Пошел миллион(275): " << i << std::endl;
        for(int j = j_start; j < 100; j++) {
            std::cout << "Пошла десятитысечка(100): " << j << std::endl;
            #pragma omp parallel for num_threads(100)
            for(int k = 0; k < 100; k++) {
                std::cout << "Пошла сотка(100): " << k << std::endl;
                std::string url = "https://vk.com/catalog.php?selection="+toStr(i)+"-"+toStr(j)+"-"+toStr(k);
                std::string thread_res;

                try {
                    thread_res = VkCatalogRequest(url);
                } catch (const HttpError& e) {
                    std::cerr << e.what() << std::endl;
                    exit(1);
                } catch (const Error& e) {
                    std::cerr << e.what() << std::endl;
                    //Некоторые блоки по 100 полностью пустые
                    #pragma omp critical
                    {
                        int logical_id = (i * 1000000) + (j * 10000) + (k * 100) + 1;
                        for(int increment = logical_id; increment < 100+logical_id; increment++) {
                            std::ofstream lost_id_file("lost_id.txt", std::ios_base::app);
                            lost_id_file << increment << " ";
                        }
                    }
                    continue;
                }

                int first_id = (i * 1000000) + (j * 10000) + (k * 100) + 1;
                std::set<int> exist_id;
                size_t pos = thread_res.find(pattern);
                while(1) {
                    size_t quote_pos = thread_res.find('"', pos+pattern.length());
                    std::string str_id = thread_res.substr(pos+pattern.length(), quote_pos-(pos+pattern.length()));
                    exist_id.insert(toInt(str_id));
                    pos = thread_res.find(pattern, pos+1);
                    if (pos == thread_res.npos)
                        break;
                }
                for(int i = first_id; i < 100+first_id; i++) {
                    if (exist_id.find(i) == exist_id.end()) {
                        #pragma omp critical
                        {
                            std::ofstream lost_id_file("lost_id.txt", std::ios_base::app);
                            lost_id_file << i << " ";
                        }
                    }
                }
            }
        }
    }
}
#endif
