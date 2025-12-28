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

#ifndef CEPH_MSG_DPDKMESSAGE_H
#define CEPH_MSG_DPDKMESSAGE_H

#include <concepts>
#include <cstdlib>
#include <ostream>
#include <string_view>

#include <boost/intrusive/list.hpp>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "include/Context.h"
#include "common/RefCountedObj.h"
#include "common/ThrottleInterface.h"
#include "common/config.h"
#include "common/ref.h"
#include "common/debug.h"
#include "common/zipkin_trace.h"
#include "include/ceph_assert.h"
#include "include/types.h"
#include "msg/Connection.h"
#include "msg/MessageRef.h"
#include "msg/async/dpdk/Packet.h"
#include "msg_types.h"

// Message::encode() crcflags bits
#define MSG_CRC_DATA           (1 << 0)
#define MSG_CRC_HEADER         (1 << 1)
#define MSG_CRC_ALL            (MSG_CRC_DATA | MSG_CRC_HEADER)

// abstract DPDKMessage class

class DPDKMessage : public RefCountedObject {
public:
#ifdef WITH_SEASTAR
  // In crimson, conn is independently maintained outside Message.
  using ConnectionRef = void*;
#else
  using ConnectionRef = ::ConnectionRef;
#endif

protected:
  ceph_msg_header  header;      // headerelope
  ceph_msg_footer  footer;
  std::optional<Packet> payload;  // "front" unaligned blob
  std::optional<Packet> middle;   // "middle" unaligned blob
  std::optional<Packet> data;     // data payload (page-alignment will be preserved where possible)
  bool packet_compacked = false;
  std::optional<Packet> packed_packet;
  /* recv_stamp is set when the Messenger starts reading the
   * Message off the wire */
  utime_t recv_stamp;
  /* dispatch_stamp is set when the Messenger starts calling dispatch() on
   * its endpoints */
  utime_t dispatch_stamp;
  /* throttle_stamp is the point at which we got throttle */
  utime_t throttle_stamp;
  /* time at which message was fully read */
  utime_t recv_complete_stamp;

  ConnectionRef connection;

  uint32_t magic = 0;

  boost::intrusive::list_member_hook<> dispatch_q;

public:
  // zipkin tracing
  ZTracer::Trace trace;
  void encode_trace(Packet& packet, uint64_t features) const;
  void decode_trace(Packet& packet, bool create = false);

  class CompletionHook : public Context {
  protected:
    DPDKMessage *m;
    friend class DPDKMessage;
  public:
    explicit CompletionHook(DPDKMessage *_m) : m(_m) {}
    virtual void set_message(DPDKMessage *_m) { m = _m; }
  };

  typedef boost::intrusive::list<DPDKMessage,
				 boost::intrusive::member_hook<
				   DPDKMessage,
				   boost::intrusive::list_member_hook<>,
				   &DPDKMessage::dispatch_q>> Queue;

  ceph::mono_time queue_start;
protected:
  CompletionHook* completion_hook = nullptr; // owned by Messenger

  // release our size in bytes back to this throttler when our payload
  // is adjusted or when we are destroyed.
  ThrottleInterface *byte_throttler = nullptr;

  // release a count back to this throttler when we are destroyed
  ThrottleInterface *msg_throttler = nullptr;

  // keep track of how big this message was when we reserved space in
  // the msgr dispatch_throttler, so that we can properly release it
  // later.  this is necessary because messages can enter the dispatch
  // queue locally (not via read_message()), and those are not
  // currently throttled.
  uint64_t dispatch_throttle_size = 0;

  friend class DPDKMessenger;

public:
  DPDKMessage() {
    memset(&header, 0, sizeof(header));
    memset(&footer, 0, sizeof(footer));
  }
  DPDKMessage(int t, int version=1, int compat_version=0) {
    memset(&header, 0, sizeof(header));
    header.type = t;
    header.version = version;
    header.compat_version = compat_version;
    memset(&footer, 0, sizeof(footer));
  }

  DPDKMessage *get() {
    return static_cast<DPDKMessage *>(RefCountedObject::get());
  }
  void compack_packet_set_header(fragment& frag) {
    packet_compacked = true;
    if(payload.has_value()){
      packed_packet = std::move(payload.value());
      if(middle.has_value())
        packed_packet->append(std::move(middle.value()));
      if(data.has_value())
        packed_packet->append(std::move(data.value()));
    }
    else if(middle.has_value()){
      packed_packet = std::move(middle.value());
      if(data.has_value())
        packed_packet->append(std::move(data.value()));
    }
    else if(data.has_value())
      packed_packet = std::move(data.value());
    else{
      packed_packet = std::move(Packet());
    }
    packed_packet->set_protocol_header(frag);
  }
  Packet* get_compacked_packet() {
    return packet_compacked ? &packed_packet.value() : nullptr;
  }
protected:
  ~DPDKMessage() override {
    if (byte_throttler) {
      if(payload.has_value())
        byte_throttler->put(payload.value().len());
      if(middle.has_value())
        byte_throttler->put(middle.value().len());
      if(data.has_value())
        byte_throttler->put(data.value().len());
    }
    release_message_throttle();
    trace.event("message destructed");
    /* call completion hooks (if any) */
    if (completion_hook)
      completion_hook->complete(0);
  }
public:
  const ConnectionRef& get_connection() const {
#ifdef WITH_SEASTAR
    ceph_abort("In crimson, conn is independently maintained outside Message");
#endif
    return connection;
  }
  void set_connection(ConnectionRef c) {
#ifdef WITH_SEASTAR
    // In crimson, conn is independently maintained outside Message.
    ceph_assert(c == nullptr);
#endif
    connection = std::move(c);
  }
  CompletionHook* get_completion_hook() { return completion_hook; }
  void set_completion_hook(CompletionHook *hook) { completion_hook = hook; }
  void set_byte_throttler(ThrottleInterface *t) {
    byte_throttler = t;
  }
  void set_message_throttler(ThrottleInterface *t) {
    msg_throttler = t;
  }

  void set_dispatch_throttle_size(uint64_t s) { dispatch_throttle_size = s; }
  uint64_t get_dispatch_throttle_size() const { return dispatch_throttle_size; }

  const ceph_msg_header &get_header() const { return header; }
  ceph_msg_header &get_header() { return header; }
  void set_header(const ceph_msg_header &e) { header = e; }
  void set_footer(const ceph_msg_footer &e) { footer = e; }
  const ceph_msg_footer &get_footer() const { return footer; }
  ceph_msg_footer &get_footer() { return footer; }
  void set_src(const entity_name_t& src) { header.src = src; }

  uint32_t get_magic() const { return magic; }
  void set_magic(int _magic) { magic = _magic; }

  /*
   * If you use get_[data, middle, payload] you shouldn't
   * use it to change those Packets unless you KNOW
   * there is no throttle being used. The other
   * functions are throttling-aware as appropriate.
   */

  void clear_payload() {
    if (byte_throttler) {
      if(payload.has_value())
        byte_throttler->put(payload.value().len());
      if(middle.has_value())
        byte_throttler->put(middle.value().len());
    }
    payload = std::nullopt;
    middle = std::nullopt;
  }

  virtual void clear_buffers() {}
  void clear_data() {
    if (byte_throttler && data.has_value())
      byte_throttler->put(data.value().len());
    data = std::nullopt;
    clear_buffers(); // let subclass drop buffers as well
  }
  void release_message_throttle() {
    if (msg_throttler)
      msg_throttler->put();
    msg_throttler = nullptr;
  }

  bool empty_payload() const { return payload.has_value() && payload.value().len() == 0; }
  Packet& get_payload() { return payload.value(); }
  const Packet& get_payload() const { return payload.value(); }
  void set_payload(Packet&& pkt) {
    if (byte_throttler)
      byte_throttler->put(payload.value().len());
    payload = std::move(pkt);
    if (byte_throttler)
      byte_throttler->take(payload.value().len());
  }

  void set_middle(Packet&& pkt) {
    if (byte_throttler)
      byte_throttler->put(middle.value().len());
    middle = std::move(pkt);
    if (byte_throttler)
      byte_throttler->take(middle.value().len());
  }
  Packet& get_middle() { return middle.value(); }

  bool has_data() const { return data.has_value(); }
  bool has_payload() const { return payload.has_value(); }
  bool has_middle() const { return middle.has_value(); }
  void set_data(Packet pkt) {
    if (byte_throttler)
      byte_throttler->put(data.value().len());
    data = std::move(pkt);
    if (byte_throttler)
      byte_throttler->take(data.value().len());
  }

  const Packet& get_data() const { return data.value(); }
  Packet& get_data() { return data.value(); }
  void claim_data(Packet& pkt) {
    if (byte_throttler)
      byte_throttler->put(data.value().len());
    pkt = std::move(data.value());
  }
  uint32_t get_data_len() const { 
    return data.has_value() ? data.value().len() : 0; }
  uint32_t get_middle_len() const { return middle.has_value() ? middle.value().len() : 0; }
  uint32_t get_payload_len() const { return payload.has_value() ? payload.value().len() : 0; }
  uint32_t get_payload_crc32c() const { return payload.has_value() ? payload.value().crc32c() : 0; }
  uint32_t get_middle_crc32c() const { return middle.has_value() ? middle.value().crc32c() : 0; }
  uint32_t get_data_crc32c() const { return data.has_value() ? data.value().crc32c() : 0; }

  void set_recv_stamp(utime_t t) { recv_stamp = t; }
  const utime_t& get_recv_stamp() const { return recv_stamp; }
  void set_dispatch_stamp(utime_t t) { dispatch_stamp = t; }
  const utime_t& get_dispatch_stamp() const { return dispatch_stamp; }
  void set_throttle_stamp(utime_t t) { throttle_stamp = t; }
  const utime_t& get_throttle_stamp() const { return throttle_stamp; }
  void set_recv_complete_stamp(utime_t t) { recv_complete_stamp = t; }
  const utime_t& get_recv_complete_stamp() const { return recv_complete_stamp; }

  void calc_header_crc() {
    header.crc = ceph_crc32c(0, (unsigned char*)&header,
			     sizeof(header) - sizeof(header.crc));
  }
  void calc_front_crc();
  void calc_data_crc();

  virtual int get_cost() const {
    return data.value().len();
  }

  // type
  int get_type() const { return header.type; }
  void set_type(int t) { header.type = t; }

  uint64_t get_tid() const { return header.tid; }
  void set_tid(uint64_t t) { header.tid = t; }

  uint64_t get_seq() const { return header.seq; }
  void set_seq(uint64_t s) { header.seq = s; }

  unsigned get_priority() const { return header.priority; }
  void set_priority(__s16 p) { header.priority = p; }

  // source/dest
  entity_inst_t get_source_inst() const {
    return entity_inst_t(get_source(), get_source_addr());
  }
  entity_name_t get_source() const {
    return entity_name_t(header.src);
  }
  entity_addr_t get_source_addr() const {
#ifdef WITH_SEASTAR
    ceph_abort("In crimson, conn is independently maintained outside Message");
#else
    if (connection)
      return connection->get_peer_addr();
#endif
    return entity_addr_t();
  }
  entity_addrvec_t get_source_addrs() const {
#ifdef WITH_SEASTAR
    ceph_abort("In crimson, conn is independently maintained outside Message");
#else
    if (connection)
      return connection->get_peer_addrs();
#endif
    return entity_addrvec_t();
  }

  // forwarded?
  entity_inst_t get_orig_source_inst() const {
    return get_source_inst();
  }
  entity_name_t get_orig_source() const {
    return get_source();
  }
  entity_addr_t get_orig_source_addr() const {
    return get_source_addr();
  }
  entity_addrvec_t get_orig_source_addrs() const {
    return get_source_addrs();
  }

  // virtual bits
  virtual void decode_payload(){};
  virtual void encode_payload(uint64_t features){};
  virtual std::string_view get_type_name() const { return "DPDKMessage"; }
  virtual void print(std::ostream& out) const {
    out << get_type_name() << " magic: " << magic;
  }

  virtual void dump(ceph::Formatter *f) const;

  void encode(uint64_t features, int crcflags, bool skip_header_crc = false);
};

extern DPDKMessage *decode_dpdk_message(CephContext *cct,
                               int crcflags,
                               ceph_msg_header& header,
                               ceph_msg_footer& footer,
                               Packet& front,
                               Packet& middle,
                               Packet& data,
                               DPDKMessage::ConnectionRef conn);
inline std::ostream& operator<<(std::ostream& out, const DPDKMessage& m) {
  m.print(out);
  if (m.get_header().version)
    out << " v" << m.get_header().version;
  return out;
}

extern void encode_dpdk_message(DPDKMessage *m, uint64_t features, Packet& pkt);
extern DPDKMessage *decode_dpdk_message(CephContext *cct, int crcflags,
                               Packet& pkt);

#endif /* CEPH_MSG_DPDKMESSAGE_H */