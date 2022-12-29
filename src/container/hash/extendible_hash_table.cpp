//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  for (int i = 0; i < num_buckets_; ++i) {
    dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // 查询key在hash 表中是否存在，如果存在就返回true 否则返回false
  std::lock_guard<std::mutex> lock(latch_);
  size_t bucked_idx = IndexOf(key);
  auto bucket = dir_[bucked_idx];
  if (bucket == nullptr) {
    return false;
  }
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  size_t bucked_idx = IndexOf(key);
  auto bucket = dir_[bucked_idx];
  if (bucket == nullptr) {
    return false;
  }
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lock(latch_);

  size_t index = IndexOf(key);
  V val;
  if (dir_[index]->Find(key, val)) {
    dir_[index]->Insert(key, value);
    return;
  }
  while (dir_[index]->IsFull()) {
    int local_depth = dir_[index]->GetDepth();
    if (local_depth == global_depth_) {
      size_t dir_size = dir_.size();
      dir_.reserve(2 * dir_size);
      std::copy_n(dir_.begin(), dir_size, std::back_inserter(dir_));
      ++global_depth_;
    }
    auto b0 = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    auto b1 = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    ++num_buckets_;
    int local_mask = 1 << local_depth;
    for (const auto &[k, v] : dir_[index]->GetItems()) {
      size_t item_hash = std::hash<K>()(k);
      if (static_cast<bool>(item_hash & local_mask)) {
        b1->Insert(k, v);
      } else {
        b0->Insert(k, v);
      }
    }
    for (size_t i = (std::hash<K>()(key) & (local_mask - 1)); i < dir_.size(); i += local_mask) {
      if (static_cast<bool>(i & local_mask)) {
        dir_[i] = b1;
      } else {
        dir_[i] = b0;
      }
    }
    index = IndexOf(key);
  }
  dir_[index]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &item : list_) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }
  if (!IsFull()) {
    list_.push_back({key, value});
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
