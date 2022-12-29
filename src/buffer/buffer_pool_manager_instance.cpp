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

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  auto frame_id = -1;
  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }
  auto page = &pages_[frame_id];

  page->page_id_ = AllocatePage();

  *page_id = page->page_id_;
  page_table_->Insert(*page_id, frame_id);
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = &pages_[frame_id];
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page->pin_count_++;
    return page;
  }
  if (!GetAvailableFrame(&frame_id)) {
    return nullptr;
  }
  Page *page = &pages_[frame_id];
  if (page_id != INVALID_PAGE_ID) {
    page_table_->Insert(page_id, frame_id);
    page->ResetMemory();
    page->page_id_ = page_id;
    disk_manager_->ReadPage(page->page_id_, page->data_);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    return page;
  }
  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *page = &pages_[frame_id];
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  if (page->pin_count_ <= 0) {
    return false;
  }
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *page = &pages_[frame_id];

  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    if (page->page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    return false;
  }
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
  }

  page_table_->Remove(page_id);       // 页表里删除该页
  free_list_.emplace_back(frame_id);  // 添加到空闲页表list中
  replacer_->Remove(frame_id);

  // 重置元数据
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }
auto BufferPoolManagerInstance::GetAvailableFrame(frame_id_t *ft) -> bool {
  if (!free_list_.empty()) {
    *ft = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  if (!replacer_->Evict(ft)) {
    return false;
  }
  Page *page = &pages_[*ft];
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
  page_table_->Remove(page->GetPageId());
  return true;
}

}  // namespace bustub
