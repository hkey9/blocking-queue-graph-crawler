#include <iostream>
#include <string>
#include <queue>
#include <unordered_set>
#include <cstdio>
#include <cstdlib>
#include <curl/curl.h>
#include <stdexcept>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>
#include <array>
#include <sstream>


using json = nlohmann::json;

template<typename T>
class BlockingQueue {
  std::queue<T> queue;
  std::mutex mtx;
  std::condition_variable cv;
  bool finished = false;
public:
  void push(const T& item) {
    std::unique_lock<std::mutex> lock(mtx);
    queue.push(item);
    cv.notify_one();
  }
  bool pop(T& item) {
    std::unique_lock<std::mutex> lock(mtx);
    while (queue.empty() && !finished) {
      cv.wait(lock);
    }
    if (queue.empty()) return false;
    item = queue.front();
    queue.pop();
    return true;
  }
  void set_finished() {
    std::unique_lock<std::mutex> lock(mtx);
    finished = true;
    cv.notify_all();
  }
  bool empty() {
    std::unique_lock<std::mutex> lock(mtx);
    return queue.empty();
  }
};

// Work item for BFS
struct WorkItem {
  std::string node;
  int depth;
};

// Fetch neighbors from the web API
std::vector<std::string> fetch_neighbors(const std::string& node) {
  std::string encoded_node = node;
  for (auto& c : encoded_node) {
    if (c == ' ') c = '_'; // API expects underscores for spaces
  }
  std::string cmd = "curl -s \"http://hollywood-graph-crawler.bridgesuncc.org/neighbors/" + encoded_node + "\"";
  std::array<char, 4096> buffer;
  std::string result;
  FILE* pipe = _popen(cmd.c_str(), "r");
  if (!pipe) return {};
  while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
    result += buffer.data();
  }
  _pclose(pipe);
  auto json_data = json::parse(result, nullptr, false);
  if (!json_data.is_object() || !json_data.contains("neighbors")) return {};
  return json_data["neighbors"].get<std::vector<std::string>>();
}

// Parallel BFS using blocking queue
void parallel_bfs(const std::string& start_node, int max_depth, int num_threads) {
  BlockingQueue<WorkItem> queue;
  std::unordered_set<std::string> visited;
  std::mutex visited_mtx;
  std::vector<std::string> result;
  std::atomic<int> active_workers(0);

  queue.push({start_node, 0});
  visited.insert(start_node);
  result.push_back(start_node);

  auto worker = [&]() {
    WorkItem item;
    while (queue.pop(item)) {
      active_workers++;
      if (item.depth >= max_depth) {
        active_workers--;
        continue;
      }
      auto neighbors = fetch_neighbors(item.node);
      {
        std::lock_guard<std::mutex> lock(visited_mtx);
        for (const auto& neighbor : neighbors) {
          if (visited.insert(neighbor).second) {
            queue.push({neighbor, item.depth + 1});
            result.push_back(neighbor);
          }
        }
      }
      active_workers--;
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i)
    threads.emplace_back(worker);

  // Wait for all work to finish
  while (true) {
    if (queue.empty() && active_workers == 0) {
      queue.set_finished();
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  for (auto& t : threads) t.join();

  // Output results
  std::cout << "Nodes reachable within depth " << max_depth << ":\n";
  for (const auto& node : result)
    std::cout << node << std::endl;
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cout << "Usage: " << argv[0] << " <start_node> <depth>\n";
    return 1;
  }
  std::string start_node = argv[1];
  int depth = std::stoi(argv[2]);
  int num_threads = 8; // You can change this for benchmarking
  parallel_bfs(start_node, depth, num_threads);
  return 0;
}


struct ParseException : std::runtime_error, rapidjson::ParseResult {
    ParseException(rapidjson::ParseErrorCode code, const char* msg, size_t offset) : 
        std::runtime_error(msg), 
        rapidjson::ParseResult(code, offset) {}
};

#define RAPIDJSON_PARSE_ERROR_NORETURN(code, offset) \
    throw ParseException(code, #code, offset)


#include <chrono>

using namespace std;
using namespace rapidjson;

bool debug = false;

// Updated service URL
const string SERVICE_URL = "http://hollywood-graph-crawler.bridgesuncc.org/neighbors/";

// Function to HTTP ecnode parts of URLs. for instance, replace spaces with '%20' for URLs
string url_encode(CURL* curl, string input) {
  char* out = curl_easy_escape(curl, input.c_str(), input.size());
  string s = out;
  curl_free(out);
  return s;
}

// Callback function for writing response data
size_t WriteCallback(void* contents, size_t size, size_t nmemb, string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

// Function to fetch neighbors using libcurl with debugging
string fetch_neighbors(CURL* curl, const string& node) {

    string url = SERVICE_URL + url_encode(curl, node);
    string response;

    if (debug)
      cout << "Sending request to: " << url << endl;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    // curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L); // Verbose Logging

    // Set a User-Agent header to avoid potential blocking by the server
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "User-Agent: C++-Client/1.0");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
        cerr << "CURL error: " << curl_easy_strerror(res) << endl;
    } else {
      if (debug)
        cout << "CURL request successful!" << endl;
    }

    // Cleanup
    curl_slist_free_all(headers);

    if (debug) 
      cout << "Response received: " << response << endl;  // Debug log

    return (res == CURLE_OK) ? response : "{}";
}

// Function to parse JSON and extract neighbors
vector<string> get_neighbors(const string& json_str) {
    vector<string> neighbors;
    try {
      Document doc;
      doc.Parse(json_str.c_str());
      
      if (doc.HasMember("neighbors") && doc["neighbors"].IsArray()) {
        for (const auto& neighbor : doc["neighbors"].GetArray())
	  neighbors.push_back(neighbor.GetString());
      }
    } catch (const ParseException& e) {
      std::cerr<<"Error while parsing JSON: "<<json_str<<std::endl;
      throw e;
    }
    return neighbors;
}

// BFS Traversal Function
vector<string> bfs(CURL* curl, const string& start, int depth) {
    queue<pair<string, int>> q;
    unordered_set<string> visited;
    vector<string> result;

    q.push({start, 0});
    visited.insert(start);

    while (!q.empty()) {
        auto [node, level] = q.front();
        q.pop();

        if (level <= depth) {
            result.push_back(node);
        }
        
        if (level < depth) {
	    try {
	      for (const auto& neighbor : get_neighbors(fetch_neighbors(curl, node))) {
                if (!visited.count(neighbor)) {
		  visited.insert(neighbor);
		  q.push({neighbor, level + 1});
                }
	      }
	    } catch (const ParseException& e) {
	      std::cerr<<"Error while fetching neighbors of: "<<node<<std::endl;
	      throw e;
	    }
        }
    }
    return result;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <node_name> <depth>\n";
        return 1;
    }

    string start_node = argv[1];     // example "Tom%20Hanks"
    int depth;
    try {
        depth = stoi(argv[2]);
    } catch (const exception& e) {
        cerr << "Error: Depth must be an integer.\n";
        return 1;
    }

    CURL* curl = curl_easy_init();
    if (!curl) {
        cerr << "Failed to initialize CURL" << endl;
        return -1;
    }


    const auto start{std::chrono::steady_clock::now()};
    
    
    for (const auto& node : bfs(curl, start_node, depth))
        cout << "- " << node << "\n";

    const auto finish{std::chrono::steady_clock::now()};
    const std::chrono::duration<double> elapsed_seconds{finish - start};
    std::cout << "Time to crawl: "<<elapsed_seconds.count() << "s\n";
    
    curl_easy_cleanup(curl);

    
    return 0;
}
