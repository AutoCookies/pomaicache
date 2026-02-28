#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace pomai_cache {

/// Directed acyclic dependency graph for pipeline-aware cascade invalidation.
/// Edges: parent -> child. Invalidating a parent cascades to all descendants.
class DepGraph {
public:
  void add_edge(const std::string &parent, const std::string &child);
  void remove_node(const std::string &key);

  std::unordered_set<std::string> descendants(const std::string &key) const;
  std::vector<std::string> parents(const std::string &key) const;
  std::vector<std::string> children(const std::string &key) const;

  bool has_node(const std::string &key) const;
  std::size_t size() const { return children_.size(); }
  std::size_t edge_count() const;

private:
  void collect_descendants(const std::string &key,
                           std::unordered_set<std::string> &out) const;

  std::unordered_map<std::string, std::unordered_set<std::string>> children_;
  std::unordered_map<std::string, std::unordered_set<std::string>> parents_;
};

} // namespace pomai_cache
