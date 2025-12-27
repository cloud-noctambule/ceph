// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_DPDKMOSDOP_H
#define CEPH_DPDKMOSDOP_H

#include "msg/DPDKMessage.h"
#include "MOSDFastDispatchOp.h"
#include "include/ceph_features.h"
#include "common/hobject.h"

namespace _dpdkmosdop {
class DPDKMOSDOp final : public MOSDFastDispatchOp { // Note: Not inheriting from MOSDOp directly
private:
  static constexpr int HEAD_VERSION = 8;
  static constexpr int COMPAT_VERSION = 3;

private:
  uint32_t client_inc = 0;
  __u32 osdmap_epoch = 0;
  __u32 flags = 0;
  utime_t mtime;
  int32_t retry_attempt = -1;   // 0 is first attempt.  -1 if we don't know.

  hobject_t hobj;
  spg_t pgid;
  // Decoding flags. Decoding is only needed for messages caught by pipe reader.
  std::atomic<bool> partial_decode_needed{true};
  std::atomic<bool> final_decode_needed{true};

public:
  // Placeholder for ops - will need to define the type
  // V ops;
  uint64_t features = 0;
  bool bdata_encode = false;
  osd_reqid_t reqid; // reqid explicitly set by sender

private:
  snapid_t snap_seq;
  std::vector<snapid_t> snaps;

public:
  DPDKMOSDOp() : MOSDFastDispatchOp(CEPH_MSG_OSD_OP, HEAD_VERSION, COMPAT_VERSION) {}
  ~DPDKMOSDOp() final {}

  void decode_payload() override { 
    // TODO: Implement payload decoding
  }
  void encode_payload(uint64_t features) override { 
    // TODO: Implement payload encoding
  }
  std::string_view get_type_name() const override { return "osd_op"; }

  // Basic getters and setters
  pg_t get_pg() const {
    ceph_assert(!partial_decode_needed);
    return pgid.pgid;
  }
  spg_t get_spg() const override {
    ceph_assert(!partial_decode_needed);
    return pgid;
  }
  epoch_t get_map_epoch() const override {
    return osdmap_epoch;
  }
};
} // namespace _dpdkmosdop

// For use in DPDKMessage context
class DPDKMOSDOp final : public DPDKMessage {
public:
  DPDKMOSDOp() : DPDKMessage{CEPH_MSG_OSD_OP} {}
private:
  ~DPDKMOSDOp() final {}

public:
  void decode_payload() override { 
    // TODO: Implement payload decoding from Packet
  }
  void encode_payload(uint64_t features) override { 
    // TODO: Implement payload encoding to Packet
  }
  std::string_view get_type_name() const override { return "osd_op"; }
};

#endif
