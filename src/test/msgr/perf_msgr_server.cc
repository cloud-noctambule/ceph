// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <iostream>

using namespace std;

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/WorkQueue.h"
#include "global/global_init.h"
#include "msg/Messenger.h"
#include "messages/MOSDOp.h"
#include "msg/DPDKMessage.h"
#include "messages/MOSDOpReply.h"
#include "auth/DummyAuth.h"
static bool receive_dpdk_message = false;
class ServerDispatcher : public Dispatcher {
  uint64_t think_time;
  ThreadPool op_tp;
  class OpWQ : public ThreadPool::WorkQueue<Message> {
    list<Message*> messages;

   public:
    OpWQ(ceph::timespan timeout, ceph::timespan suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<Message>("ServerDispatcher::OpWQ", timeout, suicide_timeout, tp) {}

    bool _enqueue(Message *m) override {
      messages.push_back(m);
      return true;
    }
    void _dequeue(Message *m) override {
      ceph_abort();
    }
    bool _empty() override {
      return messages.empty();
    }
    Message *_dequeue() override {
      if (messages.empty())
	  return NULL;
      Message *m = messages.front();
      messages.pop_front();
      return m;
    }
    void _process(Message *m, ThreadPool::TPHandle &handle) override {
      MOSDOp *osd_op = static_cast<MOSDOp*>(m);
      if(osd_op->is_dpdk_message_wrapper && receive_dpdk_message) {
        DPDKMessage *dpdk_msg = new DPDKMessage();
        dpdk_msg->set_tid(osd_op->get_tid());
        m->get_connection()->send_dpdk_message(dpdk_msg);
      }
      else{
        MOSDOpReply *reply = new MOSDOpReply(osd_op, 0, 0, 0, false);
        m->get_connection()->send_message(reply);
        m->put();
      }

    }
    void _process_finish(Message *m) override { }
    void _clear() override {
      ceph_assert(messages.empty());
    }
  } op_wq;

 public:
  ServerDispatcher(int threads, uint64_t delay): Dispatcher(g_ceph_context), think_time(delay),
    op_tp(g_ceph_context, "ServerDispatcher::op_tp", "tp_serv_disp", threads, "serverdispatcher_op_threads"),
    op_wq(ceph::make_timespan(30), ceph::make_timespan(30), &op_tp) {
    op_tp.start();
  }
  ~ServerDispatcher() override {
    op_tp.stop();
  }
  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OP:
      return true;
    default:
      return false;
    }
  }

  void ms_handle_fast_connect(Connection *con) override {}
  void ms_handle_fast_accept(Connection *con) override {}
  bool ms_dispatch(Message *m) override { return true; }
  bool ms_handle_reset(Connection *con) override { return true; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }
  void ms_fast_dispatch(Message *m) override {
    // usleep(think_time);
    //cerr << __func__ << " reply message=" << m << std::endl;
    op_wq.queue(m);
  }
  int ms_handle_fast_authentication(Connection *con) override {
    return 1;
  }
};

class MessengerServer {
  Messenger *msgr;
  string type;
  string bindaddr;
  ServerDispatcher dispatcher;
  DummyAuthClientServer dummy_auth;

 public:
  MessengerServer(const string &t, const string &addr, int threads, int delay):
    msgr(NULL), type(t), bindaddr(addr), dispatcher(threads, delay),
    dummy_auth(g_ceph_context) {
    msgr = Messenger::create(g_ceph_context, type, entity_name_t::OSD(0), "server", 0);
    msgr->set_default_policy(Messenger::Policy::stateless_server(0));
    dummy_auth.auth_registry.refresh_config();
    msgr->set_auth_server(&dummy_auth);
  }
  ~MessengerServer() {
    msgr->shutdown();
    msgr->wait();
  }
  void start() {
    entity_addr_t addr;
    addr.parse(bindaddr.c_str());
    msgr->bind(addr);
    msgr->add_dispatcher_head(&dispatcher);
    msgr->start();
    msgr->wait();
  }
};

void usage(const string &name) {
  cerr << "Usage: " << name << " [bind ip:port] [server worker threads] [thinktime us]" << std::endl;
  cerr << "       [bind ip:port]: The ip:port pair to bind, client need to specify this pair to connect" << std::endl;
  cerr << "       [server worker threads]: threads will process incoming messages and reply(matching pg threads)" << std::endl;
  cerr << "       [thinktime]: sleep time when do dispatching(match fast dispatch logic in OSD.cc)" << std::endl;
}

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  if(args.size()>3&&(args[3][0] == 'r' || args[3][0] == 'R')){
    std::stringstream cout_ss; // 创建一个stringstream对象
    g_ceph_context->_conf.set_val("ms_type", "async+rdma",&cout_ss);
    cout<<cout_ss.str()<<std::endl;
  }
  if(args.size()>3&&(args[3][0] == 'd' || args[3][0] == 'D')){
    std::stringstream cout_ss; // 创建一个stringstream对象
    bool force_zero_copy_in_stack = false;
    if(args.size()>4&&(args[4][0] == 'z' || args[4][0] == 'Z')){
      force_zero_copy_in_stack = true;
    }
    if(args.size()>5&&(args[5][0] == 'd' || args[5][0] == 'D')){
      receive_dpdk_message = true;
      g_ceph_context->_conf.set_val("ms_dpdk_with_dpdk_message", "true",&cout_ss);
      g_ceph_context->_conf.set_val("ms_dpdk_work_throught_encode", "true",&cout_ss);
    }
    else{
      g_ceph_context->_conf.set_val("ms_dpdk_with_dpdk_message", "false",&cout_ss);
    }
    g_ceph_context->_conf.set_val("ms_type", "async+dpdk",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_memory_channel", "2",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_hw_queue_weight", "1",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_coremask", "0xf0",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_hw_queue_weight", "1",&cout_ss);
    g_ceph_context->_conf.set_val("ms_async_op_threads", "1",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_gateway_ipv4_addr", "192.168.0.53",&cout_ss);
    // g_ceph_context->f_con.set_val("ms_dpdk_gateway_ipv4_addr", "10.10.10.1",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_devs_allowlist", "--allow=0000:cb:00.1",&cout_ss);
    // g_ceph_context->_conf.set_val("ms_dpdk_devs_allowlist", "--allow=0000:ca:00.3",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_netmask_ipv4_addr", "255.255.255.0",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_host_ipv4_addr", "192.168.0.93",&cout_ss);
    if(force_zero_copy_in_stack){
      g_ceph_context->_conf.set_val("ms_dpdk_force_zero_copy", "true",&cout_ss);
    }
    else{
      g_ceph_context->_conf.set_val("ms_dpdk_force_zero_copy", "false",&cout_ss);
    }
    g_ceph_context->_conf.set_val("debug_dpdk", "6/6", &cout_ss);
    g_ceph_context->_conf.set_val("debug_ms", "6/6", &cout_ss);
    cout<<cout_ss.str()<<std::endl;
  }
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.apply_changes(nullptr);

  if (args.size() < 3) {
    usage(argv[0]);
    return 1;
  }

  int worker_threads = atoi(args[1]);
  int think_time = atoi(args[2]);
  std::string public_msgr_type = g_ceph_context->_conf->ms_public_type.empty() ? g_ceph_context->_conf.get_val<std::string>("ms_type") : g_ceph_context->_conf->ms_public_type;

  cerr << " This tool won't handle connection error alike things, " << std::endl;
  cerr << "please ensure the proper network environment to test." << std::endl;
  cerr << " Or ctrl+c when meeting error and restart tests" << std::endl;
  cerr << " using ms-public-type " << public_msgr_type << std::endl;
  cerr << "       bind ip:port " << args[0] << std::endl;
  cerr << "       worker threads " << worker_threads << std::endl;
  cerr << "       thinktime(us) " << think_time << std::endl;

  MessengerServer server(public_msgr_type, args[0], worker_threads, think_time);
  server.start();

  return 0;
}
