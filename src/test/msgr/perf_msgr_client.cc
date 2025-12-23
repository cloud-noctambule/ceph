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
#include "common/Cycles.h"
#include "global/global_init.h"
#include "msg/Messenger.h"
#include "messages/MOSDOp.h"
#include "auth/DummyAuth.h"

#include <atomic>
#define NUM_CLIENT_THREAD_MODELS 3
const char* ClientThreadModelNames[] = {
    "ONE_THREAD_ONE_SERVER",//一个一个messenger，一个连接，一个服务器，一个线程
    "ONE_THREAD_ALL_SERVER",//一个messenger，多个连接，多个服务器，一个线程
    "MULTI_THREAD_SHARE_SERVER"//一个messenger，一个连接，一个服务器，多个线程
};
enum ClientThreadModel {
    ONE_THREAD_ONE_SERVER, // 单线程单服务器
    ONE_THREAD_ALL_SERVER,     // 单线程多服务器
    MULTI_THREAD_SHARE_SERVER    // 多线程单服务器
};
class MessengerClient {
  class ClientThread;
  class ClientDispatcher : public Dispatcher {
    uint64_t think_time;
    ClientThread *thread;

   public:
    ClientDispatcher(uint64_t delay, ClientThread *t): Dispatcher(g_ceph_context), think_time(delay), thread(t) {}
    bool ms_can_fast_dispatch_any() const override { return true; }
    bool ms_can_fast_dispatch(const Message *m) const override {
      switch (m->get_type()) {
      case CEPH_MSG_OSD_OPREPLY:
        return thread->check_cid(m->get_tid());
      default:
        return false;
      }
    }

    void ms_handle_fast_connect(Connection *con) override {}
    void ms_handle_fast_accept(Connection *con) override {}
    bool ms_dispatch(Message *m) override { return true; }
    void ms_fast_dispatch(Message *m) override;
    bool ms_handle_reset(Connection *con) override { return true; }
    void ms_handle_remote_reset(Connection *con) override {}
    bool ms_handle_refused(Connection *con) override { return false; }
    int ms_handle_fast_authentication(Connection *con) override {
      return 1;
    }
  };

  class ClientThread : public Thread {
    Messenger *msgr;
    int concurrent;
    std::vector<ConnectionRef> *conns;
    std::vector<uint64_t> *sid;
    std::atomic<unsigned> client_inc = { 0 };
    object_t oid;
    object_locator_t oloc;
    pg_t pgid;
    int msg_len;
    bufferlist data;
    int ops;
    uint64_t cid;//cid已经左移48位，避免后面每次移动
    int mode;
    ClientDispatcher dispatcher;

    inline uint64_t make_tid(int op,uint64_t sid_index){
        uint64_t tid=0;
        tid|=cid;
        tid|=((*sid)[sid_index]<<32);
        tid|=(uint64_t(op));
        return tid;
    }
   public:
    static vector<bool>* client_completion;//用于MULTI_THREAD_SHARE_SERVER
    static ceph::mutex client_completion_lock;
    ceph::mutex lock = ceph::make_mutex("MessengerBenchmark::ClientThread::lock");
    ceph::condition_variable cond;
    uint64_t inflight;
    vector<vector<uint64_t>> &record;
    vector<vector<bool>> &op_received;
    vector<uint32_t> record_start_pos;
    ClientThread(Messenger *m, int c, std::vector<ConnectionRef> *conns ,std::vector<uint64_t> *sid , int len, int ops, int think_time_us,\
    uint64_t cid,int mode,vector<vector<uint64_t>> &record,vector<vector<bool>> &op_received):
        msgr(m), concurrent(c), conns(conns), sid(sid), oid("object-name"), oloc(1, 1), msg_len(len), ops(ops),mode(mode),
        dispatcher(think_time_us, this), inflight(0),record(record),op_received(op_received) {
      m->add_dispatcher_head(&dispatcher);
      bufferptr ptr(msg_len);
      memset(ptr.c_str(), 0, msg_len);
      data.append(ptr);
      this->cid=cid;
      this->cid<<=48;
      record_start_pos.resize(record.size(),cid*ops);
    }
    void delete_vec(){
      delete conns;
      delete sid;
    }
    void *entry() override {
      std::unique_lock locker{lock};
      for (int i = 0; i < ops; ++i) {
        if (inflight > uint64_t(concurrent)) {
	        cond.wait(locker);
        }
	      hobject_t hobj(oid, oloc.key, CEPH_NOSNAP, pgid.ps(), pgid.pool(),
		       oloc.nspace);
	      spg_t spgid(pgid);
        MOSDOp *m = new MOSDOp(client_inc, 0, hobj, spgid, 0, 0, 0);
        bufferlist msg_data(data);
        int sid_index = i % sid->size();
        m->write(0, msg_len, msg_data);
        m->set_tid(make_tid(record_start_pos[sid_index],sid_index));
        inflight++;
        record[(*sid)[sid_index]][record_start_pos[sid_index]++]=Cycles::rdtsc();//可能有点误差
        (*conns)[sid_index]->send_message(m);
        //cerr << __func__ << " send m=" << m << std::endl;
      }
      locker.unlock();
      uint64_t ori_cid = cid>>48;
      if(mode == ClientThreadModel::ONE_THREAD_ALL_SERVER || mode == ClientThreadModel::ONE_THREAD_ONE_SERVER)
        msgr->shutdown();
      if(mode == ClientThreadModel::MULTI_THREAD_SHARE_SERVER){
        uint32_t server_num = record_start_pos.size();
        uint32_t sid_index = ori_cid%server_num;
        (*client_completion)[ori_cid]=true;
        bool can_shutdown = true;
        for(uint32_t i=0;i<client_completion->size();i++){
          if((i%server_num==sid_index)&&((*client_completion)[i]==false)){
            can_shutdown = false;
          }
        }
        if(can_shutdown){
          // cout<<"msgr" <<sid_index<<" shutdown"<<std::endl;
          msgr->shutdown();
        }
      }
      cout<<(cid>>48)<<" client done!"<<std::endl;
      return 0;
    }
    inline void set_record(uint64_t tid, uint64_t recv_time){
      uint64_t cid = 0xFFFF000000000000 & tid;
      uint64_t sid = (0x0000FFFF00000000 & tid) >>32;
      uint64_t op =  0x00000000FFFFFFFF & tid;
      if(cid!=this->cid){
        std::cout<<"get a strange msseage, got:"<<cid<<" this:"<<this->cid<<std::endl;
      }
      if(op_received[sid][op]){
        std::cout<<"get a strange msseage, have received"<<std::endl;
      }
      record[sid][op]=(Cycles::to_microseconds(recv_time - record[sid][op]));
      op_received[sid][op] = true;
    }
    inline bool check_cid(uint64_t tid){
      return (0xFFFF000000000000 & tid) == this->cid;
    }
  };

  string type;
  string serveraddr;
  int think_time_us;
  vector<Messenger*> msgrs;
  vector<ClientThread*> clients;
  vector<string> server_addr_splited;
  DummyAuthClientServer dummy_auth;
  vector<vector<uint64_t>> gathered_result_every_server;
  vector<vector<bool>> op_received_every_server;
  vector<uint64_t> avarege_latency;
  vector<int> total_ops;
  int ops;
  int mode;
 public:
  MessengerClient(const string &t, const string &addr, int delay):
      type(t), serveraddr(addr), think_time_us(delay),
      dummy_auth(g_ceph_context) {
        parseIPPorts(serveraddr,server_addr_splited);
  }
  ~MessengerClient() {
    if(mode == ClientThreadModel::MULTI_THREAD_SHARE_SERVER){
      delete MessengerClient::ClientThread::client_completion;
    }

    for (uint64_t i = 0; i < msgrs.size(); ++i) {
      msgrs[i]->shutdown();
      msgrs[i]->wait();
    }
    // 先 join 所有 ClientThread，确保线程安全退出
    for (uint64_t i = 0; i < clients.size(); ++i) {
      if (clients[i]->is_started()) {
        clients[i]->join(nullptr);
      }
    }
      // 再 delete
    for (uint64_t i = 0; i < clients.size(); ++i) {
      clients[i]->delete_vec();
      delete clients[i];
    }
  }
  void ready(int c, int jobs, int ops, int msg_len, int mode) {
    int server_num = server_addr_splited.size();
    this->ops = ops;
    this->mode = mode;
    vector<entity_addr_t> _addrs(server_num);
    for(int i=0; i<server_num;i++){
      _addrs[i].parse(server_addr_splited[i].c_str());
      _addrs[i].set_nonce(0);
    }
    this->gathered_result_every_server.resize(server_num);//直接将MessengerClient的vector传到线程内部，分段使用，避免后续的拷贝
    this->op_received_every_server.resize(server_num);
    for(int i=0; i<server_num; i++){
      this->gathered_result_every_server[i].resize(ops*jobs); 
      this->op_received_every_server[i].resize(ops*jobs);
    }
    dummy_auth.auth_registry.refresh_config();
    switch (mode)
    {
    case ClientThreadModel::ONE_THREAD_ONE_SERVER:
      for (int i = 0; i < jobs; ++i) {
        Messenger *msgr = Messenger::create(g_ceph_context, type, entity_name_t::CLIENT(0), "client", getpid()+i);
        msgr->set_default_policy(Messenger::Policy::lossless_client(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->start();
        entity_addrvec_t addrs(_addrs[i%server_num]);
        std::vector<ConnectionRef> *conns = new std::vector<ConnectionRef>;
        std::vector<uint64_t> *sid = new std::vector<uint64_t>;
        conns->emplace_back(msgr->connect_to_osd(addrs));//这个地方是可以直接批量建立连接的，但是好像实际上传入多个addrs后只会从中找到一个去建立连接
        sid->push_back(i%server_num);
        ClientThread *t = new ClientThread(msgr, c, conns,sid,msg_len, ops, think_time_us, i, mode,this->gathered_result_every_server,this->op_received_every_server);
        msgrs.push_back(msgr);
        clients.push_back(t);
      }
      break;
    case ClientThreadModel::ONE_THREAD_ALL_SERVER:
      for (int i = 0; i < jobs; ++i) {
        Messenger *msgr = Messenger::create(g_ceph_context, type, entity_name_t::CLIENT(0), "client", getpid()+i);
        msgr->set_default_policy(Messenger::Policy::lossless_client(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->start();
        std::vector<ConnectionRef> *conns = new std::vector<ConnectionRef>;
        std::vector<uint64_t> *sid = new std::vector<uint64_t>;
        for(int j=0; j<server_num;j++){
          entity_addrvec_t addrs(_addrs[j]);
          conns->emplace_back(msgr->connect_to_osd(addrs));
          sid->push_back(j);
        }
        ClientThread *t = new ClientThread(msgr, c, conns,sid,msg_len, ops, think_time_us, i,mode,this->gathered_result_every_server,this->op_received_every_server);
        msgrs.push_back(msgr);
        clients.push_back(t);
      }
      break;
    case ClientThreadModel::MULTI_THREAD_SHARE_SERVER:
      std::vector<ConnectionRef> shared_conns;
      for(int i =0 ; i<server_num; i++){
        Messenger *msgr = Messenger::create(g_ceph_context, type, entity_name_t::CLIENT(0), "client", getpid()+i);
        msgr->set_default_policy(Messenger::Policy::lossless_client(0));
        msgr->set_auth_client(&dummy_auth);
        msgr->start();
        entity_addrvec_t addrs(_addrs[i]);
        shared_conns.emplace_back(msgr->connect_to_osd(addrs));
        msgrs.push_back(msgr);
      }
      MessengerClient::ClientThread::client_completion = new vector<bool>(jobs,false);
      for (int i = 0; i < jobs; i++) {
        std::vector<ConnectionRef> *conns = new std::vector<ConnectionRef>;
        std::vector<uint64_t> *sid = new std::vector<uint64_t>;
        int sid_index = i%server_num;
        conns->emplace_back(shared_conns[sid_index]);
        sid->push_back(sid_index);
        ClientThread *t = new ClientThread(msgrs[sid_index], c, conns,sid,msg_len, ops, think_time_us, i, mode,this->gathered_result_every_server,this->op_received_every_server);
        clients.push_back(t);
      }
      break;
    }
    std::cout<<"ready done!"<<std::endl;
    usleep(1000*1000);
  }
  void start() {
    for (uint64_t i = 0; i < clients.size(); ++i)
      clients[i]->create("client");
    for (uint64_t i = 0; i < msgrs.size(); ++i)
      msgrs[i]->wait();
  }
  void parseIPPorts(const string & input, vector<string>& result) {
    if(input[0]!='{'){
      result.push_back(input);
      return;
    }
    int size = input.size();
    for(int i=1;i<size;i++){
      int j=i+1;
      while(input[j]!='}'&&input[j]!=',') j++;
      result.push_back(string(input.begin() + i, input.begin() + j));
      i=j;
    }
  }
  void gather_record(){
    int server_num = server_addr_splited.size();
    avarege_latency.resize(server_num);
    total_ops.resize(server_num);
    int size= gathered_result_every_server[0].size();
    for(int i=0; i<server_num;i++){
      for(int j=0;j<size;j++){
        if(op_received_every_server[i][j]){
          avarege_latency[i]+=gathered_result_every_server[i][j];
          total_ops[i]++;
        }
        else{
          gathered_result_every_server[i][j] = 0xFFFFFFFFFFFFFFFF;
        }
      }
    }
    for(int i=0;i<server_num;i++){
      if(total_ops[i]==0){
        total_ops[i]++;
        continue;
      }
      avarege_latency[i]/=total_ops[i];
      sort(gathered_result_every_server[i].begin(),gathered_result_every_server[i].end());
    }
  }
  void print_record(){
    int server_num = server_addr_splited.size();
    for(int i = 0; i < server_num; i++){
      cout<<"server:"<<server_addr_splited[i]<<" total_op:"<<total_ops[i]\
      <<" avar latency:"<<avarege_latency[i]<<" min latency:"<<gathered_result_every_server[i][0]\
      <<" max latency:"<<gathered_result_every_server[i][total_ops[i]-1]<<std::endl;
    }
  }
};
vector<bool>* MessengerClient::ClientThread::client_completion = nullptr;
ceph::mutex MessengerClient::ClientThread::client_completion_lock = ceph::make_mutex("MessengerClient::client_completion_lock");
void MessengerClient::ClientDispatcher::ms_fast_dispatch(Message *m) {
  usleep(think_time);
  m->put();
  uint64_t recv_time = Cycles::rdtsc();
  std::lock_guard l{thread->lock};
  thread->set_record(m->get_tid(),recv_time);
  thread->inflight--;
  thread->cond.notify_all();
}


void usage(const string &name) {
  cout << "Usage: " << name << " [\"{server ip:port,server ip:port}\"] [numjobs] [concurrency] [ios] [thinktime us] [msg length] [mode] [netstack]" << std::endl;
  cout << "       [server ip:port]: connect to the ip:port pair, {} to include multi servers" << std::endl;
  cout << "       [numjobs]: how much client threads spawned and do benchmark" << std::endl;
  cout << "       [concurrency]: the max inflight messages(like iodepth in fio)" << std::endl;
  cout << "       [ios]: how much messages sent for each client" << std::endl;
  cout << "       [thinktime]: sleep time when do fast dispatching(match client logic)" << std::endl;
  cout << "       [msg length]: message data bytes" << std::endl;
  cout << "       [mode]: the way that thread uses connection, starts from 0 to " <<NUM_CLIENT_THREAD_MODELS - 1<< std::endl;
  cout << "       [netstack]: optional, if setten with R or r, we use RDMA， D or d for DPDK"<< std::endl;
}

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  std::cout<<args.size()<<std::endl;
  if(args.size()>7&&(args[7][0] == 'r' || args[7][0] == 'R')){
    std::stringstream cout_ss; // 创建一个stringstream对象
    g_ceph_context->_conf.set_val("ms_type", "async+rdma",&cout_ss);
    cout<<cout_ss.str()<<std::endl;
  }
  if(args.size()>7&&(args[7][0] == 'd' || args[7][0] == 'D')){
    std::stringstream cout_ss; // 创建一个stringstream对象
    g_ceph_context->_conf.set_val("ms_type", "async+dpdk",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_memory_channel", "2",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_hw_queue_weight", "1",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_coremask", "0x0f0",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_hw_queue_weight", "1",&cout_ss);
    g_ceph_context->_conf.set_val("ms_async_op_threads", "1",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_devs_allowlist", "--allow=0000:ca:00.3",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_gateway_ipv4_addr", "192.168.0.53",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_netmask_ipv4_addr", "255.255.255.0",&cout_ss);
    g_ceph_context->_conf.set_val("ms_dpdk_host_ipv4_addr", "192.168.0.94",&cout_ss);
    cout<<cout_ss.str()<<std::endl;
  }
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.apply_changes(nullptr);    
  if (args.size() < 7) {
    usage(argv[0]);
    return 1;
  }
  
  int numjobs = atoi(args[1]);
  int concurrent = atoi(args[2]);
  int ios = atoi(args[3]);
  int think_time = atoi(args[4]);
  int len = atoi(args[5]);
  int mode = atoi(args[6]);
  if(mode >= NUM_CLIENT_THREAD_MODELS){
    usage(argv[0]);
    return 1;
  }
  std::string public_msgr_type = g_ceph_context->_conf->ms_public_type.empty() ? g_ceph_context->_conf.get_val<std::string>("ms_type") : g_ceph_context->_conf->ms_public_type;
  cout << " using ms-public-type " << public_msgr_type << std::endl;
  cout << "       server ip:port " << args[0] << std::endl;
  cout << "       numjobs " << numjobs << std::endl;
  cout << "       concurrency " << concurrent << std::endl;
  cout << "       ios " << ios << std::endl;
  cout << "       thinktime(us) " << think_time << std::endl;
  cout << "       message data bytes " << len << std::endl;
  cout << "       client thread mode "<<ClientThreadModelNames[mode]<<std::endl;
  
  MessengerClient client(public_msgr_type, args[0], think_time);

  client.ready(concurrent, numjobs, ios, len,mode);
  Cycles::init();
  uint64_t start = Cycles::rdtsc();
  client.start();
  uint64_t stop = Cycles::rdtsc();
  cout << " Total op " << (ios * numjobs) << " run time " << Cycles::to_microseconds(stop - start) << "us." << std::endl;
  client.gather_record();
  client.print_record();
  return 0;
}
