#include "net_discover.h"

void ThreadWorker::AddParams(Id id, const NetDiscover* parent)
{
    _process_id.push_back(id);
    _parent = parent;
}

void ThreadWorker::GzipFile(const std::string& from, const std::string& to) const {
    std::ofstream out_file(to.c_str());
    std::ifstream in_file(from.c_str());
    boost::iostreams::filtering_ostream graph_gzip;
    graph_gzip.push(boost::iostreams::gzip_compressor());
    graph_gzip.push(out_file);
    boost::iostreams::copy(in_file, graph_gzip);
}

void ThreadWorker::GzipFile(const std::string& from) const {
    using namespace utils;
    std::string archive_file = from + "_" + toStr(std::time(nullptr)) +"_"+toStr(_parent->_first_id)+"-"+toStr(_parent->_last_id)+".gz";
    GzipFile(from, archive_file);
}

std::pair<int,int> ThreadWorker::LastDoneEdge(const std::string& patch) const
{
    bool is_gzip_there = false;
    for( boost::filesystem::directory_iterator dir_iter(_parent->_work_dir); dir_iter != boost::filesystem::directory_iterator() ; ++dir_iter)
    {
        std::string name = dir_iter->path().c_str();
        if (name.find("gz") != std::string::npos) {
            is_gzip_there = true;
            break;
        }
    }

    std::ifstream graph_file(patch.c_str(), std::ios_base::in | std::ios_base::binary);
    if (is_gzip_there)
        if (!graph_file) throw utils::FileSysError("Не могу открыть "+patch+". Распакуйте последний архив этого потока");

    Id last_src = -1;
    Id last_done_dest = -1;
    while(graph_file.good()) {
        Id src, dest;
        graph_file.read(reinterpret_cast<char*>(&src), 4);
        graph_file.read(reinterpret_cast<char*>(&dest), 4);
        if (graph_file.fail()) break;
        last_src = src;
        last_done_dest = dest;
    }
    return std::pair<int,int>(last_src, last_done_dest);
}

void ThreadWorker::SetIndex(int i)
{
    _index = i;
}

void ThreadWorker::ContinueProcId(int src, int last_done_dest, const std::string& patch) const
{
    std::string url = "https://api.vk.com/method/friends.get.xml?user_id="+utils::toStr(src)+"&v=5.25";

    std::string response;
    try {
        response = utils::VkXmlRequest(url);
    } catch(const std::runtime_error& e) {
        std::cerr << e.what() << std::endl;
        exit(1);
    }
    pugi::xml_document doc;
    doc.load(response.c_str());

    pugi::xpath_node error_code = doc.select_single_node("/error/error_code");
    pugi::xpath_node error_msg = doc.select_single_node("/error/error_msg");
    if (error_code || error_msg) return;

    pugi::xpath_node_set users = doc.select_nodes("/response/items/user_id");

    std::ofstream graph_file(patch.c_str(), std::ios_base::out | std::ios_base::binary | std::ios_base::app);
    for (pugi::xpath_node_set::const_iterator it = users.begin(); it != users.end(); ++it)
    {
        pugi::xml_node node = it->node();
        //Иногда первыми в xml пустые несколько строк идут, пропускаем их
        if (std::string(node.child_value()).empty()) {
            std::cerr << "SKIPPED EMPTY CHILD_VALUE" << std::endl;
            continue;
        }
        Id dest = utils::toId(node.child_value());
        //Предполагается, что ответ от VK всегда отсортирован в возрастающей последовательности.
        //Поэтому все что до last_done_dest считается уже записанным
        if (dest < last_done_dest) continue;

        if (_parent->_lost_id_set.find(dest) != _parent->_lost_id_set.end()) {
            continue;
        }

        //Создаем связи только с вершинами, у которых ID больше чем у src, тем самым не будет дублированных связей. Only A->B, not A->B and B->A
        if (dest > src) {
            graph_file.write(reinterpret_cast<char*>(&src), 4);
            graph_file.write(reinterpret_cast<char*>(&dest), 4);
        }
    }
}

void ThreadWorker::operator()() const
{
    try {
        const std::string graph_path = _parent->_work_dir + "/graph" + utils::toStr(_index);
        std::pair<int,int> last_edge = LastDoneEdge(graph_path);

        std::vector<Id>::const_iterator start_process_id;
        if (last_edge.first == -1) {
            start_process_id = _process_id.begin();
        } else {
            ContinueProcId(last_edge.first, last_edge.second, graph_path);
            if ( (last_edge.first+_parent->_thread_num) > _process_id.back()) return;
            start_process_id = std::find(_process_id.begin(), _process_id.end(), last_edge.first+_parent->_thread_num);
            if (start_process_id == _process_id.end())
                throw utils::Error("Не могу найти последний записанный ID в контейнере _process_id");
        }


        for(auto id = start_process_id; id != _process_id.end(); ++id)
        {
            if (_parent->_lost_id_set.find(*id) != _parent->_lost_id_set.end()) {
                continue;
            }

            std::ofstream graph_file(graph_path.c_str(), std::ios_base::out | std::ios_base::binary | std::ios_base::app);

            std::string url = "https://api.vk.com/method/friends.get.xml?user_id="+utils::toStr(*id)+"&v=5.25";
            std::string response;
            try {
                response = utils::VkXmlRequest(url);
            } catch(const std::runtime_error& e) {
                std::cerr << e.what() << std::endl;
                exit(1);
            }
            pugi::xml_document doc;
            doc.load(response.c_str());

            pugi::xpath_node error_code = doc.select_single_node("/error/error_code");
            pugi::xpath_node error_msg = doc.select_single_node("/error/error_msg");
            if (error_code || error_msg) continue;

#ifdef VKDEBUG
            std::string save_path = _parent->_xml_dir+"/friends_get_"+utils::toStr(*id)+".xml";
            std::ifstream xml(save_path.c_str());
            if (xml.good()) {
                xml.close();
            } else {
                doc.save_file( save_path.c_str() );
            }
#endif
            pugi::xpath_node_set users = doc.select_nodes("/response/items/user_id");
            Id src = *id;

            if (users.size() == 0) {
                graph_file.write(reinterpret_cast<char*>(&src), 4);
                Id dest = (uint32_t)(-1);
                graph_file.write(reinterpret_cast<char*>(&dest), 4);
            } else {
                for (pugi::xpath_node_set::const_iterator it = users.begin(); it != users.end(); ++it)
                {
                    pugi::xml_node node = it->node();
                    //Иногда первыми в xml пустые несколько строк идут, пропускаем их
                    if (std::string(node.child_value()).empty()) {
                        std::cerr << "SKIPPED EMPTY CHILD_VALUE" << std::endl;
                        continue;
                    }
                    Id dest = utils::toId(node.child_value());
                    if (_parent->_lost_id_set.find(dest) != _parent->_lost_id_set.end()) {
                        continue;
                    }

                    //Создаем связи только с вершинами, у которых ID больше чем у src, тем самым не будет дублированных связей. Only A->B, not A->B and B->A
                    if (dest > src) {
                        graph_file.write(reinterpret_cast<char*>(&src), 4);
                        graph_file.write(reinterpret_cast<char*>(&dest), 4);
                    }
                }
            }
            graph_file.close();

            size_t size = boost::filesystem::file_size(graph_path.c_str()) / 1024 / 1024;
            if (size >= 1000) {
                GzipFile(graph_path);
                std::pair<int,int> last_edge = LastDoneEdge(graph_path);
                boost::filesystem::remove(graph_path);
                std::ofstream new_graph_file(graph_path.c_str(), std::ios_base::out | std::ios_base::binary);
                new_graph_file.write(reinterpret_cast<char*>(&last_edge.first), 4);
                new_graph_file.write(reinterpret_cast<char*>(&last_edge.second), 4);
            }
        }
        GzipFile(graph_path);
        boost::filesystem::remove(graph_path);
        std::cerr << "THREAD: " << _index << " WORKING DONE" << std::endl;
    } catch (const std::runtime_error& e) {
        std::cerr << e.what() << " THREAD: " <<  _index << std::endl;
        exit(1);
    } catch (...) {
        std::cerr << " UNEXPECTED ERROR. THREAD: " <<  _index << std::endl;
        exit(1);
    }
}

NetDiscover::NetDiscover(const Id first_id, const Id last_id, int thread_num, const std::string& work_dir, const std::set<int>& lost_id_set) :
    _first_id(first_id),
    _last_id(last_id),
    _thread_num(thread_num),
    _thread_worker(_thread_num),
    _work_dir(work_dir),
    _xml_dir(_work_dir+"/xml"),
    _lost_id_set(lost_id_set) {}


void NetDiscover::Discover()
{
    struct stat wd = {0};
    if (stat(_work_dir.c_str(), &wd) == -1) {
        mkdir(_work_dir.c_str(), 0755);
    }

    struct stat xd = {0};
    if (stat(_xml_dir.c_str(), &xd) == -1) {
        mkdir(_xml_dir.c_str(), 0755);
    }

    for(int i = 0; i < _thread_worker.size(); ++i)
        _thread_worker[i].SetIndex(i);

    auto current = _thread_worker.begin();
    for(int id = _first_id; id < _last_id; ++id) {
        current->AddParams(id, this);
        ++current;
        if (current == _thread_worker.end()) current = _thread_worker.begin();
    }

    int last_size = _thread_worker.begin()->ProcessIdSize();
    for(auto it = _thread_worker.begin(); it != _thread_worker.end(); it++)
        if(last_size != it->ProcessIdSize()) {
            throw utils::Error("У потоков разное число обрабатываемых ID");
        }

    std::vector<std::thread> threads;
    ForEachI(_thread_worker, iter) {
        threads.push_back( std::thread(*iter) );
    }

    ForEachI(threads, iter) {
        iter->join();
    }
}

int main(int argc, char** argv) {
    try {

        Id FIRST_ID = atoi(argv[1]);//1;
        Id LAST_ID = atoi(argv[2]);//275570000;
        int THREAD_NUM = atoi(argv[3]);//8;
        std::string WORK_DIR = argv[4];//"/root/vk/vk_discover";
        std::string LOST_ID_FILE = argv[5];
        std::cerr << "FIRST ID: " << FIRST_ID << std::endl;
        std::cerr << "LAST_ID: " << LAST_ID << std::endl;
        std::cerr << "THREAD_NUM: " << THREAD_NUM << std::endl;
        std::cerr << "WORK_DIR: " << WORK_DIR << std::endl;
        std::cerr << "LOST_ID_FILE: " << LOST_ID_FILE << std::endl;

        std::ifstream lost_id_f(LOST_ID_FILE.c_str());
        if (!lost_id_f) throw utils::FileSysError("Не могу открыть LOST_ID_FILE");
        int ld;
        std::set<int> LOST_ID_SET;
        while(lost_id_f >> ld)
            LOST_ID_SET.insert(ld);

        if (WORK_DIR.back() == '/')
            WORK_DIR.erase(WORK_DIR.size() - 1);

        if ( (LAST_ID-FIRST_ID) % THREAD_NUM != 0 )
            throw utils::Error("Допустимый диапазон должен быть кратным числу потоков");

        NetDiscover vk_discover(FIRST_ID, LAST_ID, THREAD_NUM, WORK_DIR, LOST_ID_SET);
        vk_discover.Discover();
    } catch (const std::runtime_error& e) {
        std::cerr << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknowd exceptions" << std::endl;
    }
}
