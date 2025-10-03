#include <iostream>
#include <string>
#include <queue>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <curl/curl.h>
#include <stdexcept>
#include "rapidjson/error/error.h"
#include "rapidjson/reader.h"
#include <rapidjson/document.h>
#include <chrono>

using namespace std;
using namespace rapidjson;

bool debug = false;

// Updated service URL
const string SERVICE_URL = "http://hollywood-graph-crawler.bridgesuncc.org/neighbors/";

// JSON parse exception wrapper from starter kit
struct ParseException : std::runtime_error, rapidjson::ParseResult {
    ParseException(rapidjson::ParseErrorCode code, const char* msg, size_t offset) :
        std::runtime_error(msg),
        rapidjson::ParseResult(code, offset) {}
};

#define RAPIDJSON_PARSE_ERROR_NORETURN(code, offset) \
    throw ParseException(code, #code, offset)

// URL encode helper (requires a CURL* handle)
string url_encode(CURL* curl, const string& input) {
    char* out = curl_easy_escape(curl, input.c_str(), input.size());
    if (!out) return input;
    string s = out;
    curl_free(out);
    return s;
}

// Write callback for libcurl
size_t WriteCallback(void* contents, size_t size, size_t nmemb, string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

// Fetch neighbors using provided CURL handle (per-thread)
string fetch_neighbors_with_curl(CURL* curl, const string& node) {
    string url = SERVICE_URL + url_encode(curl, node);
    string response;

    if (debug)
        cout << "Sending request to: " << url << endl;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    // Minimal headers (User-Agent)
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "User-Agent: C++-Client/1.0");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
        cerr << "CURL error (" << node << "): " << curl_easy_strerror(res) << endl;
    } else {
        if (debug)
            cout << "CURL request successful for: " << node << endl;
    }

    curl_slist_free_all(headers);

    return (res == CURLE_OK) ? response : "{}";
}

// Extract neighbors from JSON string using RapidJSON
vector<string> get_neighbors_from_json(const string& json_str) {
    vector<string> neighbors;
    try {
        Document doc;
        doc.Parse(json_str.c_str());

        if (doc.HasParseError()) {
            RAPIDJSON_PARSE_ERROR_NORETURN(doc.GetParseError(), doc.GetErrorOffset());
        }

        if (doc.HasMember("neighbors") && doc["neighbors"].IsArray()) {
            for (const auto& n : doc["neighbors"].GetArray()) {
                if (n.IsString())
                    neighbors.push_back(n.GetString());
            }
        }
    } catch (const ParseException& e) {
        cerr << "Error parsing JSON: " << json_str << endl;
        throw;
    }
    return neighbors;
}

// Thread-safe blocking queue for work items
template<typename T>
class BlockingQueue {
public:
    BlockingQueue() : done(false) {}

    void push(const T& item) {
        {
            lock_guard<mutex> lk(m);
            q.push(item);
        }
        cv.notify_one();
    }

    // Waits until either an item is available (returns true and sets 'item')
    // or queue is done and empty (returns false).
    bool wait_pop(T& item) {
        unique_lock<mutex> lk(m);
        cv.wait(lk, [&]() { return !q.empty() || done; });
        if (q.empty() && done) return false;
        item = move(q.front());
        q.pop();
        return true;
    }

    bool empty() {
        lock_guard<mutex> lk(m);
        return q.empty();
    }

    // Called to indicate no future push will occur (but in our design,
    // producers are the workers themselves so we'll control 'done' externally).
    void set_done() {
        {
            lock_guard<mutex> lk(m);
            done = true;
        }
        cv.notify_all();
    }

    // notify to wake waiting threads (used when pushing)
    void notify_all() {
        cv.notify_all();
    }

private:
    queue<T> q;
    mutex m;
    condition_variable cv;
    bool done;
};

//  BFS worker and orchestration 
vector<string> parallel_bfs(const string& start_node, int max_depth, int num_workers = 8) {
    // Work item is pair<node, depth>
    BlockingQueue<pair<string,int>> workq;
    unordered_set<string> visited;
    mutex visited_m;

    atomic<int> working_count{0};
    atomic<bool> done{false}; // global termination flag
    condition_variable termination_cv;
    mutex termination_m;

    // push initial
    {
        lock_guard<mutex> lk(visited_m);
        visited.insert(start_node);
    }
    workq.push({start_node, 0});

    // Worker lambda
    auto worker = [&](int id) {
        // Each worker gets its own CURL handle
        CURL* curl = curl_easy_init();
        if (!curl) {
            cerr << "Worker " << id << " failed to init curl\n";
            return;
        }
        // make reasonable timeout so threads don't hang too long
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

        while (true) {
            pair<string,int> item;
            bool got = workq.wait_pop(item);
            if (!got) {
                // queue is done and empty
                break;
            }

            // Mark this worker as actively processing
            working_count.fetch_add(1, memory_order_relaxed);

            const string node = item.first;
            const int depth = item.second;

            if (depth < max_depth) {
                try {
                    string json_resp = fetch_neighbors_with_curl(curl, node);
                    vector<string> neighbors = get_neighbors_from_json(json_resp);
                    for (const auto& nb : neighbors) {
                        bool should_push = false;
                        {
                            lock_guard<mutex> lk(visited_m);
                            if (!visited.count(nb)) {
                                visited.insert(nb);
                                should_push = true;
                            }
                        }
                        if (should_push) {
                            workq.push({nb, depth + 1});
                        }
                    }
                } catch (const ParseException& e) {
                    cerr << "Parse error for node: " << node << endl;
                    // continue processing other nodes (do not abort here)
                }
            }

            // Finished processing one item
            working_count.fetch_sub(1, memory_order_relaxed);

            // If queue is empty and nobody is working, signal termination:
            if (workq.empty() && working_count.load(memory_order_relaxed) == 0) {
                // set done & notify all workers (makes wait_pop return false)
                workq.set_done();
            }
        } // end while

        curl_easy_cleanup(curl);
    };

    // start worker threads
    vector<thread> threads;
    for (int i = 0; i < num_workers; ++i) {
        threads.emplace_back(worker, i);
    }

    // wait for all workers to complete
    for (auto &t : threads) {
        if (t.joinable()) t.join();
    }

    // Collect visited nodes into a vector for return
    vector<string> result;
    {
        lock_guard<mutex> lk(visited_m);
        result.reserve(visited.size());
        for (const auto &n : visited) result.push_back(n);
    }
    return result;
}

int main(int argc, char* argv[]) {
    if (argc < 3 || argc > 4) {
        cerr << "Usage: " << argv[0] << " <node_name> <depth> [num_workers]\n";
        cerr << "Example: " << argv[0] << " \"Tom Hanks\" 4 8\n";
        return 1;
    }

    string start_node = argv[1];
    int depth;
    int num_workers = 8;
    try {
        depth = stoi(argv[2]);
        if (argc == 4) num_workers = stoi(argv[3]);
    } catch (const exception& e) {
        cerr << "Error: depth and num_workers must be integers\n";
        return 1;
    }

    // init curl globally
    if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
        cerr << "curl_global_init failed\n";
        return 1;
    }

    const auto t0 = chrono::steady_clock::now();
    vector<string> nodes = parallel_bfs(start_node, depth, num_workers);
    const auto t1 = chrono::steady_clock::now();

    // Output
    for (const auto &n : nodes) {
        cout << "- " << n << "\n";
    }

    const chrono::duration<double> elapsed = t1 - t0;
    cout << "Time to crawl: " << elapsed.count() << "s\n";
    cout << "Nodes discovered: " << nodes.size() << "\n";

    curl_global_cleanup();
    return 0;
}