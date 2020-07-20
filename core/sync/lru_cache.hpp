/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_SYNC_LRU_CACHE_HPP
#define CPP_FILECOIN_SYNC_LRU_CACHE_HPP

#include <cassert>
#include <functional>
#include <list>
#include <memory>
#include <map>

namespace fc::sync {

  template <typename Key, typename Value>
  class LRUCache {
   public:
    using ExtractKey = std::function<const Key &(const Value &)>;

    LRUCache(size_t size_limit, ExtractKey extract_key_fn)
        : size_limit_(size_limit), extract_key_(std::move(extract_key_fn)) {
      assert(size_limit_ >= 1);
      assert(extract_key_);
    }

    std::shared_ptr<const Value> get(const Key &key) {
      auto it = items_.find(key);
      if (it == items_.end()) {
        return std::shared_ptr<const Value>{};
      }
      auto &item = it->second;
      bring_to_front(item);
      return item.value;
    }

    std::shared_ptr<Value> get_mutable(const Key &key) {
      auto it = items_.find(key);
      if (it == items_.end()) {
        return std::shared_ptr<Value>{};
      }
      auto &item = it->second;
      if (item.value.use_count() > 1) {
        item.value = std::make_shared<Value>(*item.value);
        *item.lru_it = std::weak_ptr<Value>(item.value);
      }
      return item.value;
    }

    void modifyValues(std::function<void(Value& value)> cb) {
      for (auto& [_, item] : items_) {
        cb(*(item.value));
      }
    }

    void put(std::shared_ptr<Value> value, bool update_if_exists) {
      assert(value);
      assert(items_.size() == lru_.size());

      const auto &key = extract_key_(*value);
      auto it = items_.find(key);
      if (it == items_.end()) {
        typename LRUList::iterator lru_it;
        if (items_.size() >= size_limit_) {
          lru_it = lru_.rbegin().base();
          auto sptr = lru_it->lock();

          assert(sptr);

          if (sptr) {
            items_.erase(extract_key_(*sptr));
          }
          lru_.splice(lru_.begin(), lru_, lru_it);
          *lru_it = value;
        } else {
          lru_.push_front(std::weak_ptr<Value>(value));
          lru_it = lru_.begin();
        }
        items_[extract_key_(*value)] = {std::move(value), lru_it};
      } else {
        auto &item = it->second;
        bring_to_front(item);
        if (update_if_exists) {
          item.value = std::move(value);
        }
      }
    }

   private:
    using LRUList = std::list<std::weak_ptr<Value>>;
    struct Item {
      std::shared_ptr<Value> value;
      typename LRUList::iterator lru_it;
    };

    // TODO (artem): make std::hash out of TipsetHash and use unordered
    using Items = std::map<Key, Item>;

    void bring_to_front(Item &item) {
      if (item.lru_it != lru_.begin()) {
        lru_.splice(lru_.begin(), lru_, item.lru_it);
      }
    }

    const size_t size_limit_;
    ExtractKey extract_key_;
    Items items_;
    LRUList lru_;
  };

}  // namespace fc::sync

#endif  // CPP_FILECOIN_SYNC_LRU_CACHE_HPP