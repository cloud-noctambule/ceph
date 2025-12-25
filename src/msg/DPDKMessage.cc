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

#include "DPDKMessage.h"
#include "common/Formatter.h"

#define dout_subsys ceph_subsys_ms

// Helper function to calculate CRC32C for a Packet
static uint32_t packet_crc32c(uint32_t crc, const Packet& pkt) {
  for (unsigned i = 0; i < pkt.nr_frags(); ++i) {
    fragment f = pkt.frag(i);
    crc = ceph_crc32c(crc, reinterpret_cast<const unsigned char*>(f.base), f.size);
  }
  return crc;
}

// Include DPDKMessage subclass implementations
#include "messages/DPDKMPing.h"
#include "messages/DPDKMStatfs.h"
#include "messages/DPDKMOSDOp.h"

// Forward declaration for remaining subclass
class DPDKMOSDOpReply;

void DPDKMessage::calc_front_crc() {
  footer.front_crc = packet_crc32c(0, payload);
  footer.middle_crc = packet_crc32c(0, middle);
}

void DPDKMessage::calc_data_crc() {
  footer.data_crc = packet_crc32c(0, data);
}

void DPDKMessage::encode_trace(Packet& pkt, uint64_t features) const {
  using ceph::encode;
  auto p = trace.get_info();
  static const blkin_trace_info empty = { 0, 0, 0 };
  if (!p) {
    p = &empty;
  }
  
  // Encode trace info directly to Packet
  char* buf = pkt.prepend_uninitialized_header(sizeof(blkin_trace_info));
  memcpy(buf, p, sizeof(blkin_trace_info));
}

void DPDKMessage::decode_trace(Packet& pkt, bool create) {
  blkin_trace_info info = {};
  
  // Decode trace info from Packet
  char* buf = pkt.get_header(0, sizeof(blkin_trace_info));
  if (buf) {
    memcpy(&info, buf, sizeof(blkin_trace_info));
  }

#ifdef WITH_BLKIN
  if (!connection) {
    return;
  }

  const auto msgr = connection->get_messenger();
  const auto endpoint = msgr->get_trace_endpoint();
  if (info.trace_id) {
    trace.init(get_type_name().data(), endpoint, &info, true);
    trace.event("decoded trace");
  } else if (create || (msgr->get_myname().is_osd() &&
                        msgr->cct->_conf->osd_blkin_trace_all)) {
    // create a trace even if we didn't get one on the wire
    trace.init(get_type_name().data(), endpoint);
    trace.event("created trace");
  }
  trace.keyval("tid", get_tid());
  trace.keyval("entity type", get_source().type_str());
  trace.keyval("entity num", get_source().num());
#endif
}

void DPDKMessage::encode(uint64_t features, int crcflags, bool skip_header_crc) {
  // encode and copy out of *m
  if (empty_payload()) {
    ceph_assert(middle.len() == 0);
    encode_payload(features);

    if (byte_throttler) {
      byte_throttler->take(payload.len() + middle.len());
    }

    // if the encoder didn't specify past compatibility, we assume it
    // is incompatible.
    if (header.compat_version == 0)
      header.compat_version = header.version;
  }
  if (crcflags & MSG_CRC_HEADER)
    calc_front_crc();

  // update envelope
  header.front_len = get_payload().len();
  header.middle_len = get_middle().len();
  header.data_len = get_data().len();
  if (!skip_header_crc && (crcflags & MSG_CRC_HEADER))
    calc_header_crc();

  footer.flags = CEPH_MSG_FOOTER_COMPLETE;

  if (crcflags & MSG_CRC_DATA) {
    calc_data_crc();
  } else {
    footer.flags = (unsigned)footer.flags | CEPH_MSG_FOOTER_NOCRC;
  }
}

void DPDKMessage::dump(ceph::Formatter *f) const {
  std::stringstream ss;
  print(ss);
  f->dump_string("summary", ss.str());
}

DPDKMessage *decode_dpdk_message(CephContext *cct,
                               int crcflags,
                               ceph_msg_header& header,
                               ceph_msg_footer& footer,
                               Packet& front,
                               Packet& middle,
                               Packet& data,
                               DPDKMessage::ConnectionRef conn) {
  // verify crc
  if (crcflags & MSG_CRC_HEADER) {
    __u32 front_crc = packet_crc32c(0, front);
    __u32 middle_crc = packet_crc32c(0, middle);

    if (front_crc != footer.front_crc) {
      if (cct) {
        ldout(cct, 0) << "bad crc in front " << front_crc << " != exp " << footer.front_crc
#ifndef WITH_SEASTAR
                      << " from " << conn->get_peer_addr()
#endif
                      << dendl;
      }
      return nullptr;
    }
    if (middle_crc != footer.middle_crc) {
      if (cct) {
        ldout(cct, 0) << "bad crc in middle " << middle_crc << " != exp " << footer.middle_crc
#ifndef WITH_SEASTAR
                      << " from " << conn->get_peer_addr()
#endif
                      << dendl;
      }
      return nullptr;
    }
  }
  if (crcflags & MSG_CRC_DATA) {
    if ((footer.flags & CEPH_MSG_FOOTER_NOCRC) == 0) {
      __u32 data_crc = packet_crc32c(0, data);
      if (data_crc != footer.data_crc) {
        if (cct) {
          ldout(cct, 0) << "bad crc in data " << data_crc << " != exp " << footer.data_crc
#ifndef WITH_SEASTAR
                        << " from " << conn->get_peer_addr()
#endif
                        << dendl;
        }
        return nullptr;
      }
    }
  }

  // Create message based on type
  DPDKMessage *m = nullptr;
  
  switch (header.type) {
  case CEPH_MSG_PING:
    m = new DPDKMPing();
    break;
  case CEPH_MSG_STATFS:
    m = new DPDKMStatfs();
    break;
  case CEPH_MSG_STATFS_REPLY:
    m = new DPDKMStatfsReply();
    break;
  case CEPH_MSG_OSD_OP:
    m = new DPDKMOSDOp();
    break;
  case CEPH_MSG_OSD_OPREPLY:
    // m = new DPDKMOSDOpReply(); // TODO: Implement this class
    break;
  default:
    if (cct) {
      ldout(cct, 0) << "unknown message type " << header.type << dendl;
    }
    return nullptr;
  }
  
  if (!m) {
    if (cct) {
      ldout(cct, 0) << "message type " << header.type << " not implemented for DPDKMessage" << dendl;
    }
    return nullptr;
  }
  
  // Set the header, footer, and connection
  m->set_header(header);
  m->set_footer(footer);
  m->set_connection(conn);
  
  // Set the payload, middle, and data
  m->set_payload(std::move(front));
  m->set_middle(std::move(middle));
  m->set_data(data); // Note: not using move here as data might be used elsewhere
  
  // Decode the payload
  m->decode_payload();
  
  return m;
}

void encode_dpdk_message(DPDKMessage *m, uint64_t features, Packet& pkt) {
  // First encode the message internally (this will call encode_payload)
  m->encode(features, MSG_CRC_ALL);
  
  // Now write the encoded message to the Packet
  
  // 1. Write the header
  char* header_buf = pkt.prepend_uninitialized_header(sizeof(ceph_msg_header));
  memcpy(header_buf, &m->get_header(), sizeof(ceph_msg_header));
  
  // 2. Write the footer
  char* footer_buf = pkt.prepend_uninitialized_header(sizeof(ceph_msg_footer));
  memcpy(footer_buf, &m->get_footer(), sizeof(ceph_msg_footer));
  
  // 3. Write the payload (front)
  Packet& payload = m->get_payload();
  // Assuming Packet has a method to append to another Packet
  pkt.append(std::move(payload));
  
  // 4. Write the middle
  Packet& middle = m->get_middle();
  pkt.append(std::move(middle));
  
  // 5. Write the data
  Packet& data = m->get_data();
  pkt.append(std::move(data));
  
  // Note: The order above might need to be adjusted based on how the Packet is structured
  // and how the network stack expects to process the data.
}

DPDKMessage *decode_dpdk_message(CephContext *cct, int crcflags, Packet& pkt) {
  // First, linearize the packet to make it easier to extract components
  pkt.linearize();
  
  // 1. Extract the header
  ceph_msg_header header;
  char* header_ptr = pkt.get_header(0, sizeof(ceph_msg_header));
  if (!header_ptr) {
    if (cct) {
      ldout(cct, 0) << "Failed to extract header from packet" << dendl;
    }
    return nullptr;
  }
  memcpy(&header, header_ptr, sizeof(ceph_msg_header));
  
  // 2. Calculate the position of footer
  size_t footer_pos = pkt.len() - sizeof(ceph_msg_footer);
  if (footer_pos < sizeof(ceph_msg_header)) {
    if (cct) {
      ldout(cct, 0) << "Packet too small for footer" << dendl;
    }
    return nullptr;
  }
  
  // 3. Extract the footer
  ceph_msg_footer footer;
  char* footer_ptr = pkt.get_header(footer_pos, sizeof(ceph_msg_footer));
  if (!footer_ptr) {
    if (cct) {
      ldout(cct, 0) << "Failed to extract footer from packet" << dendl;
    }
    return nullptr;
  }
  memcpy(&footer, footer_ptr, sizeof(ceph_msg_footer));
  
  // 4. Extract front, middle, and data based on header lengths
  size_t current_pos = sizeof(ceph_msg_header);
  
  // Extract front
  Packet front;
  if (header.front_len > 0) {
    if (current_pos + header.front_len > footer_pos) {
      if (cct) {
        ldout(cct, 0) << "Invalid front_len in header" << dendl;
      }
      return nullptr;
    }
    // Create a new Packet for front by sharing the relevant part
    front = pkt.share(current_pos, header.front_len);
    current_pos += header.front_len;
  }
  
  // Extract middle
  Packet middle;
  if (header.middle_len > 0) {
    if (current_pos + header.middle_len > footer_pos) {
      if (cct) {
        ldout(cct, 0) << "Invalid middle_len in header" << dendl;
      }
      return nullptr;
    }
    // Create a new Packet for middle by sharing the relevant part
    middle = pkt.share(current_pos, header.middle_len);
    current_pos += header.middle_len;
  }
  
  // Extract data
  Packet data;
  if (header.data_len > 0) {
    if (current_pos + header.data_len > footer_pos) {
      if (cct) {
        ldout(cct, 0) << "Invalid data_len in header" << dendl;
      }
      return nullptr;
    }
    // Create a new Packet for data by sharing the relevant part
    data = pkt.share(current_pos, header.data_len);
    current_pos += header.data_len;
  }
  
  // Verify that we've extracted all payload and only footer remains
  if (current_pos != footer_pos) {
    if (cct) {
      ldout(cct, 0) << "Packet structure mismatch: expected pos " << footer_pos << ", got " << current_pos << dendl;
    }
    return nullptr;
  }
  
  // Now call the existing decode_dpdk_message function with the extracted components
  return decode_dpdk_message(cct, crcflags, header, footer, front, middle, data, nullptr);
}