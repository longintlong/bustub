//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id id of page to be flushed cannot be -1.");
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; ++i) {
    disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
    pages_[i].is_dirty_ = false;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id_t new_pg_id = AllocatePage();
  Page *r = nullptr;
  frame_id_t frame_id = -1;
  std::lock_guard<std::mutex> lock(latch_);

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    r = pages_ + frame_id;
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
    if (frame_id >= 0) {
      r = pages_ + frame_id;
      // r is dirty, write back to the disk.
      if (r->IsDirty()) {
        disk_manager_->WritePage(r->page_id_, r->data_);
      }
      page_table_.erase(r->page_id_);
      r->ResetMemory();
      r->page_id_ = new_pg_id;
      r->pin_count_ = 0;
      r->is_dirty_ = false;
    }
  }
  if (r != nullptr) {
    page_table_[new_pg_id] = frame_id;
    *page_id = new_pg_id;
    r->page_id_ = new_pg_id;
    ++r->pin_count_;
    replacer_->Pin(frame_id);
  }
  return r;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // return NULL if no page is available in the free list and all other pages are currently pinned
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  Page *r = nullptr;
  // P exists
  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];
    r = pages_ + frame_id;
    return r;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    r = pages_ + frame_id;
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
    if (frame_id >= 0) {
      r = pages_ + frame_id;
      // r is dirty, write back to the disk.
      if (r->IsDirty()) {
        disk_manager_->WritePage(r->page_id_, r->data_);
      }
      page_table_.erase(r->page_id_);

      // Update P's metadata
      r->ResetMemory();
      r->page_id_ = page_id;
      r->pin_count_ = 0;
      r->is_dirty_ = false;
    }
  }
  if (r != nullptr) {
    page_table_[page_id] = frame_id;
    ++r->pin_count_;
    replacer_->Pin(frame_id);
    disk_manager_->ReadPage(page_id, r->data_);
  }

  return r;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  DeallocatePage(page_id);
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *r = pages_ + frame_id;
  if (r->pin_count_ == 0) {
    if (!r->IsDirty()) {
      disk_manager_->WritePage(page_id, r->data_);
    }
    page_table_.erase(page_id);
    r->ResetMemory();
    r->page_id_ = page_id;
    r->pin_count_ = 0;
    r->is_dirty_ = false;
    free_list_.push_back(frame_id);
    return true;
  }
  return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_table_.count(page_id) != 0, "try unpin a page not int Buffer Pool.");

  frame_id_t frame_id = page_table_[page_id];
  Page *r = pages_ + frame_id;

  if (is_dirty) {
    r->is_dirty_ = true;
  }
  int pin_count = r->pin_count_;
  --r->pin_count_;
  if (r->pin_count_ <= 0) {
    replacer_->Unpin(frame_id);
  }
  return pin_count > 0;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
