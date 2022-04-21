//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  size_ = 0;
  capacity_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!size_) return false;
  *frame_id = lru_list_.back();
  lru_map_.erase(*frame_id);
  lru_list_.pop_back();
  --size_;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) { LRU_Remove(frame_id); }

void LRUReplacer::Unpin(frame_id_t frame_id) { LRU_Append(frame_id); }

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(mutex_);
  int ret = size_;
  return ret;
}

void LRUReplacer::LRU_Append(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  // frame is already in lru replacer
  if (lru_map_.count(frame_id)) {
    return;
  }
  lru_list_.push_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
  ++size_;
}

void LRUReplacer::LRU_Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!lru_map_.count(frame_id)) {
    return;
  }
  lru_list_.erase(lru_map_[frame_id]);
  lru_map_.erase(frame_id);
  --size_;
}

void LRUReplacer::MoveToHead(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  lru_list_.erase(lru_map_[frame_id]);
  lru_list_.push_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
}

}  // namespace bustub
