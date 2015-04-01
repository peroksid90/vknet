all:
	g++ -std=c++11 net_discover.cpp pugixml.cpp -o vk_disc -lcurl -lboost_iostreams -lboost_filesystem -lboost_system
	g++ -std=c++11 vk_catalog.cpp pugixml.cpp -o vk_catalog -lcurl -fopenmp -lboost_iostreams -lboost_filesystem -lboost_system
