/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef FILECOIN_GENERIC_ITERATOR_HPP
#define FILECOIN_GENERIC_ITERATOR_HPP

#include <memory>

namespace fc::face {

  /**
   * An interface for an iterator
   * @tparam Container over which the iterator would iterate
   */
  template <typename Container>
  class GenericIterator {
   public:
    using value_type = typename Container::value_type;

    virtual ~GenericIterator() = default;

    // needed as there's no simple way to copy an object by a pointer to its
    // abstract interface
    virtual std::unique_ptr<GenericIterator> clone() const = 0;

    virtual value_type *get() = 0;
    virtual value_type const *get() const = 0;

    virtual value_type &operator*() = 0;
    virtual value_type const &operator*() const = 0;

    virtual GenericIterator<Container> &operator++() = 0;

    value_type &operator->() {
      return **this;
    }

    virtual bool operator!=(const GenericIterator<Container> &other) const {
      return get() != other.get();
    }

    bool operator==(const GenericIterator<Container> &other) {
      return get() == other.get();
    }
  };

}  // namespace fc::face

#endif  // FILECOIN_GENERIC_ITERATOR_HPP
