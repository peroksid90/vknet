#include <string>
#include <boost/unordered_set.hpp>
#include <graphlab.hpp>
#include <graphlab/ui/metrics_server.hpp>
#include <graphlab/macros_def.hpp>
#include <cstdlib>
#include<unistd.h>
#include <sys/wait.h>
#include<cstdlib>
#include<iostream>
#include<pugixml.hpp>
#include"utils.h"

struct vertex_data_type {
    vertex_data_type() {}
    boost::unordered_set<graphlab::vertex_id_type> vid_set;

    void save(graphlab::oarchive &oarc) const {
        oarc << vid_set;
    }

    void load(graphlab::iarchive &iarc) {
        iarc >> vid_set;
    }
};

typedef size_t edge_data_type;
typedef float message_type;

struct set_union_gather {
    boost::unordered_set<graphlab::vertex_id_type> vid_set;

    set_union_gather& operator+=(const set_union_gather& other) {
        foreach(graphlab::vertex_id_type othervid, other.vid_set) {
            vid_set.insert(othervid);
        }
        return *this;
    }

    void save(graphlab::oarchive& oarc) const {
        oarc << vid_set;
    }

    void load(graphlab::iarchive& iarc) {
        iarc >> vid_set;
    }
};

typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;


class neighboor_program :
    public graphlab::ivertex_program<graph_type, set_union_gather>,
    public graphlab::IS_POD_TYPE {
public:
    edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const {
        return graphlab::ALL_EDGES;
    }

    gather_type gather(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
        set_union_gather gather;
        vertex_id_type otherid = edge.source().id() == vertex.id() ? edge.target().id() : edge.source().id();
        gather.vid_set.insert(otherid);
        return gather;
    }

   void apply(icontext_type& context, vertex_type& vertex, const gather_type& neighborhood) {
        vertex.data().vid_set = neighborhood.vid_set;
   }

   edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const {
        return graphlab::NO_EDGES;
   }
};

int main(int argc, char** argv) {
    int first_id = 1;
    int last_id = 20000;

    std::set<int> skipped;
    std::ifstream lost_id_f(argv[1]);
    int ld;
    while(lost_id_f >> ld)
        skipped.insert(ld);

    std::cerr << "LOST ID SIZE: " << skipped.size() << std::endl;

    std::map<int,std::vector<int>> expected_friend_count;
    for(int id = first_id; id <= last_id; id++) {
        pugi::xml_document doc;
        std::string url = "/home/peroksid/vknet/xml/friends_get_"+utils::toStr(id)+".xml";
        if (skipped.find(id) != skipped.end()) continue;
        pugi::xml_parse_result result = doc.load_file(url.c_str());
        if (!result) {
            std::cerr << "Не могу загрузить документ: " << url << std::endl;
            //skipped.push_back(id);
            continue;
            //exit(1);
        }
        pugi::xpath_node error_code = doc.select_single_node("/error/error_code");
        pugi::xpath_node error_msg = doc.select_single_node("/error/error_msg");
        if (error_code || error_msg) {
                std::stringstream err_stream;
                err_stream << "CODE: " << error_code.node().child_value() << " REASON: " << error_msg.node().child_value() << std::endl;
                std::string msg = error_msg.node().child_value();
                if (msg.find("user deactivated") == msg.npos) throw;
                //std::cerr << err_stream.str();
                //skipped.push_back(id);
                continue;
        }
        pugi::xpath_node_set users = doc.select_nodes("/response/items/user_id");

        for (pugi::xpath_node_set::const_iterator it = users.begin(); it != users.end(); ++it)
        {
            pugi::xml_node node = it->node();
            if (skipped.find(utils::toInt(node.child_value())) == skipped.end())
                expected_friend_count[id].push_back(utils::toInt(node.child_value()));
        }
    }

    graphlab::mpi_tools::init(argc, argv);
    graphlab::distributed_control dc;
    graph_type graph(dc);
    graph.load_format("/home/peroksid/vknet/vk_discover/graph", "bintsv4");
    graph.finalize();

    graphlab::synchronous_engine<neighboor_program> engine(dc, graph);
    engine.signal_all();
    engine.start();

    for(int id = first_id; id <= last_id; id++) {
        if (skipped.find(id) != skipped.end())
            continue;
        if (!graph.has_vid(id)) {
            std::cout << "GraphLab: vertex " << id << " not found!" << std::endl;
            continue;
        }
        std::vector<int> actual_friends;
        for(auto iter = graph.vertex(id).data().vid_set.begin(); iter != graph.vertex(id).data().vid_set.end(); iter++)
            actual_friends.push_back(*iter);

        std::sort(expected_friend_count[id].begin(), expected_friend_count[id].end());
        std::sort(actual_friends.begin(), actual_friends.end());

        std::vector<int> diff(10000);
        auto it = std::set_difference (expected_friend_count[id].begin(), expected_friend_count[id].end()
                                       ,actual_friends.begin(), actual_friends.end()
                                       ,diff.begin());
        diff.resize(it-diff.begin());

        if (diff.size() > 0) {
            std::cout << "TEST FAIL. ID: " << id << " DIFF: ";
            for (auto iter=diff.begin(); iter!=diff.end(); ++iter)
              std::cout << ' ' << *iter;
            std::cout << std::endl;
        } else {
            std::cout << "TEST DONE" << std::endl;
        }
    }
}
