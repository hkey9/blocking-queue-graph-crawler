#include <iostream>
#include <string>
#include <queue>
#include <unordered_set>
#include <cstdio>
#include <cstdlib>
#include <curl/curl.h>
#include <stdexcept>
#include <thread>
#include <mutex>
#include <vector>
#include <algorithm>
#include <chrono>
#include "rapidjson/error/error.h"
#include "rapidjson/reader.h"
#include <rapidjson/document.h>


struct ParseException : std::runtime_error, rapidjson::ParseResult {
    ParseException(rapidjson::ParseErrorCode code, const char* msg, size_t offset) : 
        std::runtime_error(msg), 
        rapidjson::ParseResult(code, offset) {}
};

#define RAPIDJSON_PARSE_ERROR_NORETURN(code, offset) \
    throw ParseException(code, #code, offset)


bool debug = false;

// Updated service URL
const std::string SERVICE_URL = "http://hollywood-graph-crawler.bridgesuncc.org/neighbors/";

// Fixed maximum threads (per spec). You can change this constant if desired.
const unsigned int MAX_THREADS = 8;

// Function to HTTP ecnode parts of URLs. for instance, replace spaces with '%20' for URLs
std::string url_encode(CURL* curl, std::string input) {
  char* out = curl_easy_escape(curl, input.c_str(), input.size());
  std::string s = out;
  curl_free(out);
  return s;
}

// Callback function for writing response data
size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

// Function to fetch neighbors using libcurl with debugging
std::string fetch_neighbors(CURL* curl, const std::string& node) {

  std::string url = SERVICE_URL + url_encode(curl, node);
  std::string response;

    if (debug)
      std::cout << "Sending request to: " << url << std::endl;

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
      std::cerr << "CURL error: " << curl_easy_strerror(res) << std::endl;
    } else {
      if (debug)
        std::cout << "CURL request successful!" << std::endl;
    }

    // Cleanup
    curl_slist_free_all(headers);

    if (debug) 
      std::cout << "Response received: " << response << std::endl;  // Debug log

    return (res == CURLE_OK) ? response : "{}";
}

// Function to parse JSON and extract neighbors
std::vector<std::string> get_neighbors(const std::string& json_str) {
    std::vector<std::string> neighbors;
    rapidjson::Document doc;
    doc.Parse(json_str.c_str());

    if (doc.HasParseError()) {
        RAPIDJSON_PARSE_ERROR_NORETURN(doc.GetParseError(), doc.GetErrorOffset());
    }

    if (doc.HasMember("neighbors") && doc["neighbors"].IsArray()) {
        for (const auto& neighbor : doc["neighbors"].GetArray()) {
            if (neighbor.IsString())
                neighbors.push_back(neighbor.GetString());
        }
    }
    return neighbors;
}

// Parallel BFS
std::vector<std::vector<std::string>> bfs_parallel(const std::string& start, int depth) {
    std::vector<std::vector<std::string>> levels;
    std::unordered_set<std::string> visited;
    std::mutex visited_mtx;
    std::mutex nextlevel_mtx;

    levels.push_back({start});
    visited.insert(start);

    for (int d = 0; d < depth; ++d) {
        if (debug)
            std::cout << "Starting level " << d << " with " << levels[d].size() << " nodes.\n";

        std::vector<std::string> next_level;
        size_t level_size = levels[d].size();
        if (level_size == 0) {
            levels.push_back({});
            continue;
        }

        // Decide number of threads for this level
        unsigned int threads_for_level = static_cast<unsigned int>(std::min<size_t>(MAX_THREADS, level_size));
        // If fewer nodes than max threads, we'll give one node per thread (threads_for_level == level_size).
        // If more nodes, threads_for_level == MAX_THREADS and nodes are split among them.

        if (debug)
            std::cout << "Using " << threads_for_level << " threads for level " << d << ".\n";

        // Partition nodes among threads as evenly as possible
        std::vector<std::pair<size_t,size_t>> assignments; // startIdx, endIdx (end exclusive)
        assignments.reserve(threads_for_level);

        size_t base = level_size / threads_for_level;
        size_t rem = level_size % threads_for_level;
        size_t idx = 0;
        for (unsigned int t = 0; t < threads_for_level; ++t) {
            size_t chunk = base + (t < rem ? 1 : 0);
            size_t startIdx = idx;
            size_t endIdx = idx + chunk;
            assignments.emplace_back(startIdx, endIdx);
            idx = endIdx;
        }

        // Worker lambda for a thread to process assigned nodes
        auto worker = [&](size_t startIdx, size_t endIdx) {
            // Each thread creates its own CURL easy handle
            CURL* curl = curl_easy_init();
            if (!curl) {
                std::cerr << "Thread failed to initialize CURL\n";
                return;
            }

            for (size_t i = startIdx; i < endIdx; ++i) {
                const std::string& node = levels[d][i];
                try {
                    std::string resp = fetch_neighbors(curl, node);
                    std::vector<std::string> neigh = get_neighbors(resp);

                    for (const auto& nb : neigh) {
                        // Lock visited set when checking/inserting
                        std::lock_guard<std::mutex> lock(visited_mtx);
                        if (visited.find(nb) == visited.end()) {
                            // Mark visited and push to next level safely
                            visited.insert(nb);
                            // Do not hold visited mutex while pushing to next_level to reduce contention:
                            // use a separate mutex for next_level.
                            {
                                std::lock_guard<std::mutex> nl_lock(nextlevel_mtx);
                                next_level.push_back(nb);
                            }
                        }
                    }
                } catch (const ParseException& e) {
                    std::cerr << "JSON parse error while expanding node '" << node << "' : " << e.what() << std::endl;
                    // Continue; don't abort entire traversal on one bad node.
                } catch (const std::exception& e) {
                    std::cerr << "Error while expanding node '" << node << "' : " << e.what() << std::endl;
                }
            }

            curl_easy_cleanup(curl);
        };

        // Launch threads
        std::vector<std::thread> workers;
        workers.reserve(threads_for_level);
        for (unsigned int t = 0; t < threads_for_level; ++t) {
            size_t s = assignments[t].first;
            size_t e = assignments[t].second;
            if (s >= e) continue; // no work
            workers.emplace_back(worker, s, e);
        }

        // Wait for threads to finish
        for (auto& th : workers) {
            if (th.joinable()) th.join();
        }

        levels.push_back(std::move(next_level));
        if (debug)
            std::cout << "Finished level " << d << ". Next level size: " << levels.back().size() << "\n";
    }

    return levels;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <node_name> <depth>\n";
        return 1;
    }

    std::string start_node = argv[1];     // example "Tom%20Hanks"
    int depth;
    try {
        depth = std::stoi(argv[2]);
        if (depth < 0) throw std::invalid_argument("negative depth");
    } catch (const std::exception& e) {
        std::cerr << "Error: Depth must be a non-negative integer.\n";
        return 1;
    }

    // Initialize libcurl globally
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != 0) {
        std::cerr << "curl_global_init() failed\n";
        return 1;
    }

    const auto start_time = std::chrono::steady_clock::now();

    std::vector<std::vector<std::string>> result = bfs_parallel(start_node, depth);

    const auto finish_time = std::chrono::steady_clock::now();
    const std::chrono::duration<double> elapsed_seconds = finish_time - start_time;

    // Output results level by level
    for (size_t lvl = 0; lvl < result.size(); ++lvl) {
        std::cout << "Level " << lvl << " (" << result[lvl].size() << " nodes):\n";
        for (const auto& node : result[lvl]) {
            std::cout << "- " << node << "\n";
        }
        std::cout << std::endl;
    }

    std::cout << "Time to crawl: " << elapsed_seconds.count() << "s\n";

    curl_global_cleanup();
    return 0;
}
