#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/common/exception.h"

namespace onebase {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : max_frames_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  bool found = false;
  frame_id_t victim = INVALID_FRAME_ID;
  bool victim_has_infinite_distance = false;
  size_t victim_metric = 0;
  size_t victim_earliest_timestamp = 0;

  for (const auto &[candidate_frame_id, entry] : entries_) {
    if (!entry.is_evictable_) {
      continue;
    }

    const bool has_infinite_distance = entry.history_.size() < k_;
    const size_t earliest_timestamp = entry.history_.front();
    const size_t metric = has_infinite_distance ? earliest_timestamp : current_timestamp_ - earliest_timestamp;

    if (!found) {
      found = true;
      victim = candidate_frame_id;
      victim_has_infinite_distance = has_infinite_distance;
      victim_metric = metric;
      victim_earliest_timestamp = earliest_timestamp;
      continue;
    }

    if (has_infinite_distance != victim_has_infinite_distance) {
      if (has_infinite_distance) {
        victim = candidate_frame_id;
        victim_has_infinite_distance = true;
        victim_metric = metric;
        victim_earliest_timestamp = earliest_timestamp;
      }
      continue;
    }

    if (has_infinite_distance) {
      if (earliest_timestamp < victim_earliest_timestamp) {
        victim = candidate_frame_id;
        victim_metric = metric;
        victim_earliest_timestamp = earliest_timestamp;
      }
      continue;
    }

    if (metric > victim_metric || (metric == victim_metric && earliest_timestamp < victim_earliest_timestamp)) {
      victim = candidate_frame_id;
      victim_metric = metric;
      victim_earliest_timestamp = earliest_timestamp;
    }
  }

  if (!found) {
    return false;
  }

  entries_.erase(victim);
  curr_size_--;
  *frame_id = victim;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= max_frames_) {
    throw OneBaseException("frame id out of range", ExceptionType::OUT_OF_RANGE);
  }

  std::scoped_lock<std::mutex> lock(latch_);
  auto &entry = entries_[frame_id];
  entry.history_.push_back(current_timestamp_++);
  if (entry.history_.size() > k_) {
    entry.history_.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= max_frames_) {
    throw OneBaseException("frame id out of range", ExceptionType::OUT_OF_RANGE);
  }

  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = entries_.find(frame_id);
  if (iter == entries_.end() || iter->second.is_evictable_ == set_evictable) {
    return;
  }

  iter->second.is_evictable_ = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= max_frames_) {
    throw OneBaseException("frame id out of range", ExceptionType::OUT_OF_RANGE);
  }

  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = entries_.find(frame_id);
  if (iter == entries_.end()) {
    return;
  }
  if (!iter->second.is_evictable_) {
    throw OneBaseException("cannot remove a non-evictable frame", ExceptionType::BUFFER_FULL);
  }

  entries_.erase(iter);
  curr_size_--;
}

auto LRUKReplacer::Size() const -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace onebase
