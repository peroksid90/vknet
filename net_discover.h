#ifndef NET_DISCOVER_H
#define NET_DISCOVER_H
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "pugixml.hpp"
#include "utils.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <set>
#include <ctime>

class ThreadWorker;
class NetDiscover
{
    friend class ThreadWorker;
public:
    NetDiscover(const Id first_id, const Id last_id, int thread_num, const std::string& work_dir, const std::set<int>& lost_id_set);
    void Discover();
private:
    const Id _first_id;
    const Id _last_id;
    const int _thread_num;
    const std::string _work_dir;
    const std::string _xml_dir;
    std::vector<ThreadWorker> _thread_worker;
    const std::set<int>& _lost_id_set;
};

class ThreadWorker
{
 public:
        void AddParams(Id id, const NetDiscover* parent);
        void operator()() const;
        void SetIndex(int i);
        size_t ProcessIdSize() {
            return _process_id.size();
        }
        void GzipFile(const std::string& from, const std::string& to) const;

        void GzipFile(const std::string& from) const;
private:
        std::vector<Id> _process_id;
        void ContinueProcId(int src, int last_done_dest, const std::string& path) const;
        std::pair<int,int> LastDoneEdge(const std::string& path) const;
        const NetDiscover* _parent;
        int _index;
};


#endif // NET_DISCOVER_H
