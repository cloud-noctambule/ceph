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

#ifndef CEPH_DPDKMSTATFS_H
#define CEPH_DPDKMSTATFS_H

#include "msg/DPDKMessage.h"

class DPDKMStatfs final : public DPDKMessage {
public:
  DPDKMStatfs() : DPDKMessage{CEPH_MSG_STATFS} {}
private:
  ~DPDKMStatfs() final {}

public:
  void decode_payload() override { }
  void encode_payload(uint64_t features) override { }
  std::string_view get_type_name() const override { return "statfs"; }
};

class DPDKMStatfsReply final : public DPDKMessage {
public:
  DPDKMStatfsReply() : DPDKMessage{CEPH_MSG_STATFS_REPLY} {}
private:
  ~DPDKMStatfsReply() final {}

public:
  void decode_payload() override { }
  void encode_payload(uint64_t features) override { }
  std::string_view get_type_name() const override { return "statfs_reply"; }
};

#endif
