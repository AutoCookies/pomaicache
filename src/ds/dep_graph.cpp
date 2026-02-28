#include "pomai_cache/dep_graph.hpp"

namespace pomai_cache {

void DepGraph::add_edge(const std::string &parent, const std::string &child) {
  children_[parent].insert(child);
  parents_[child].insert(parent);
}

void DepGraph::remove_node(const std::string &key) {
  if (auto it = children_.find(key); it != children_.end()) {
    for (const auto &child : it->second) {
      if (auto pit = parents_.find(child); pit != parents_.end())
        pit->second.erase(key);
    }
    children_.erase(it);
  }

  if (auto it = parents_.find(key); it != parents_.end()) {
    for (const auto &parent : it->second) {
      if (auto cit = children_.find(parent); cit != children_.end())
        cit->second.erase(key);
    }
    parents_.erase(it);
  }
}

void DepGraph::collect_descendants(const std::string &key,
                                   std::unordered_set<std::string> &out) const {
  auto it = children_.find(key);
  if (it == children_.end())
    return;
  for (const auto &child : it->second) {
    if (out.insert(child).second)
      collect_descendants(child, out);
  }
}

std::unordered_set<std::string>
DepGraph::descendants(const std::string &key) const {
  std::unordered_set<std::string> result;
  collect_descendants(key, result);
  return result;
}

std::vector<std::string> DepGraph::parents(const std::string &key) const {
  auto it = parents_.find(key);
  if (it == parents_.end())
    return {};
  return {it->second.begin(), it->second.end()};
}

std::vector<std::string> DepGraph::children(const std::string &key) const {
  auto it = children_.find(key);
  if (it == children_.end())
    return {};
  return {it->second.begin(), it->second.end()};
}

bool DepGraph::has_node(const std::string &key) const {
  return children_.contains(key) || parents_.contains(key);
}

std::size_t DepGraph::edge_count() const {
  std::size_t total = 0;
  for (const auto &[_, set] : children_)
    total += set.size();
  return total;
}

} // namespace pomai_cache
