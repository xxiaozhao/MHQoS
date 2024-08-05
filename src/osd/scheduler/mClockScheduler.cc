// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <memory>
#include <functional>

//咯咯哒
#include <ostream>
#include <fstream>
#include <string>
#include <chrono>
#include <ctime>
#include <chrono>
#include <cmath>


#include "osd/scheduler/mClockScheduler.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;

#define dout_context cct
#define dout_subsys ceph_subsys_mclock
#undef dout_prefix
#define dout_prefix *_dout << "mClockScheduler: "


namespace ceph::osd::scheduler {


//咯咯哒
std::unordered_map<std::string, int> mClockScheduler::user_priorities;


mClockScheduler::mClockScheduler(CephContext *cct,
  int whoami,
  uint32_t num_shards,
  int shard_id,
  bool is_rotational,
  MonClient *monc)
  : cct(cct),
    whoami(whoami),
    num_shards(num_shards),
    shard_id(shard_id),
    is_rotational(is_rotational),
    monc(monc),
    scheduler(
      std::bind(&mClockScheduler::ClientRegistry::get_info,
                &client_registry,
                _1),
      dmc::AtLimit::Wait,       //咯咯哒
      cct->_conf.get_val<double>("osd_mclock_scheduler_anticipation_timeout"))
{
  cct->_conf.add_observer(this);
  ceph_assert(num_shards > 0);
  set_osd_capacity_params_from_config();


  // //初始化上次冻结时间
  // OpScheduler::last_frozen_time = Clock::now()- std::chrono::minutes(1);

  //初始化用户优先级
  // std::unordered_map<std::string, int> mClockScheduler::user_priorities;
  initUserPriorities();

  // 详细了解下面两个函数
  set_config_defaults_from_profile();
  client_registry.update_from_config(
    cct->_conf, osd_bandwidth_capacity_per_shard, num_shards);

  //咯咯哒绑定清理函数
    cleaning_job =
	  std::unique_ptr<crimson::RunEvery>(
	    new crimson::RunEvery(init_check_time,
			 std::bind(&mClockScheduler::do_clean, this)));

}

/* ClientRegistry holds the dmclock::ClientInfo configuration parameters
 * (reservation (bytes/second), weight (unitless), limit (bytes/second))
 * for each IO class in the OSD (client, background_recovery,
 * background_best_effort).
 *
 * mclock expects limit and reservation to have units of <cost>/second
 * (bytes/second), but osd_mclock_scheduler_client_(lim|res) are provided
 * as ratios of the OSD's capacity.  We convert from the one to the other
 * using the capacity_per_shard parameter.
 *
 * Note, mclock profile information will already have been set as a default
 * for the osd_mclock_scheduler_client_* parameters prior to calling
 * update_from_config -- see set_config_defaults_from_profile().
 */



// 更新客户端注册表中的QoS配置参数，这些参数包括预留量、权重和限制。它根据OSD的容量和Ceph配置来计算这些值。——————延迟处理
void mClockScheduler::ClientRegistry::update_from_config(
  const ConfigProxy &conf,
  const double capacity_per_shard,
  const uint32_t num_shards)
{
  auto get_res = [&](double res, double bandwidth) {
//    if(bandwidth){
//      return bandwidth/num_shards;
//    }
    if (res) {
      return res * capacity_per_shard;
    } else {
      return default_min; // min reservation
    }
  };

  auto get_lim = [&](double lim, double bandwidth) {
    if(bandwidth){
      return bandwidth/num_shards;
    }
    if (lim) {
      return lim * capacity_per_shard;
    } else {
      return default_max; // high limit
    }
  };


   auto get_delay = [&](double delay) {
    if (delay) {
      return delay;
    } else {
      return default_min; // high delay
    }
  };
        

  // Set external client infos
  double res_bandwidth = conf.get_val<double>("osd_mclock_scheduler_client_res_bandwidth");
  double lim_bandwidth = conf.get_val<double>("osd_mclock_scheduler_client_lim_bandwidth");
  double delay = conf.get_val<double>(
    "osd_mclock_scheduler_client_delay");
  double res = conf.get_val<double>(
    "osd_mclock_scheduler_client_res");
  double lim = conf.get_val<double>(
    "osd_mclock_scheduler_client_lim");
  uint64_t wgt = conf.get_val<uint64_t>(
    "osd_mclock_scheduler_client_wgt");
  default_external_client_info.update(
    get_res(res, res_bandwidth),
    wgt,
    get_lim(lim, lim_bandwidth),
    get_delay(delay));

  // Set background recovery client infos
  res_bandwidth = conf.get_val<double>("osd_mclock_scheduler_background_recovery_res_bandwidth");
  lim_bandwidth = conf.get_val<double>("osd_mclock_scheduler_background_recovery_lim_bandwidth");
  delay = conf.get_val<double>(
    "osd_mclock_scheduler_background_recovery_delay");
  res = conf.get_val<double>(
    "osd_mclock_scheduler_background_recovery_res");
  lim = conf.get_val<double>(
    "osd_mclock_scheduler_background_recovery_lim");
  wgt = conf.get_val<uint64_t>(
    "osd_mclock_scheduler_background_recovery_wgt");
  internal_client_infos[
    static_cast<size_t>(op_scheduler_class::background_recovery)].update(
      get_res(res, res_bandwidth),
      wgt,
      get_lim(lim, lim_bandwidth),
      get_delay(delay));

   //咯咯哒——background_medium
   // Set background_medium client infos
   res_bandwidth = conf.get_val<double>("osd_mclock_scheduler_background_medium_res_bandwidth");
   lim_bandwidth = conf.get_val<double>("osd_mclock_scheduler_background_medium_lim_bandwidth");
   delay = conf.get_val<double>(
    "osd_mclock_scheduler_background_medium_delay");
  res = conf.get_val<double>(
    "osd_mclock_scheduler_background_medium_res");
  lim = conf.get_val<double>(
    "osd_mclock_scheduler_background_medium_lim");
  wgt = conf.get_val<uint64_t>(
    "osd_mclock_scheduler_background_medium_wgt");
  internal_client_infos[
    static_cast<size_t>(op_scheduler_class::background_medium)].update(
      get_res(res, res_bandwidth),
      wgt,
      get_lim(lim, lim_bandwidth),
      get_delay(delay));

  // Set background best effort client infos
  res_bandwidth = conf.get_val<double>("osd_mclock_scheduler_background_best_effort_res_bandwidth");
  lim_bandwidth = conf.get_val<double>("osd_mclock_scheduler_background_best_effort_lim_bandwidth");
  delay = conf.get_val<double>(
    "osd_mclock_scheduler_background_best_effort_delay");
  res = conf.get_val<double>(
    "osd_mclock_scheduler_background_best_effort_res");
  lim = conf.get_val<double>(
    "osd_mclock_scheduler_background_best_effort_lim");
  wgt = conf.get_val<uint64_t>(
    "osd_mclock_scheduler_background_best_effort_wgt");
  internal_client_infos[
    static_cast<size_t>(op_scheduler_class::background_best_effort)].update(
      get_res(res, res_bandwidth),
      wgt,
      get_lim(lim, lim_bandwidth),
      get_delay(delay));

  // 咯咯哒（根据切片capacity_per_shard信息更新qos模板）
  // high
  res_bandwidth = conf.get_val<double>("osd_mclock_scheduler_high_res_bandwidth");
  lim_bandwidth = conf.get_val<double>("osd_mclock_scheduler_high_lim_bandwidth");
  delay = conf.get_val<double>(
    "osd_mclock_scheduler_high_delay");
  res = conf.get_val<double>(
    "osd_mclock_scheduler_high_res");
  lim = conf.get_val<double>(
    "osd_mclock_scheduler_high_lim");
  wgt = conf.get_val<uint64_t>(
    "osd_mclock_scheduler_high_wgt");
  default_external_high_info.update(
  get_res(res, res_bandwidth),
  wgt,
  get_lim(lim, lim_bandwidth),
  get_delay(delay));

  //low
  res_bandwidth = conf.get_val<double>("osd_mclock_scheduler_low_res_bandwidth");
  lim_bandwidth = conf.get_val<double>("osd_mclock_scheduler_low_lim_bandwidth");
  delay = conf.get_val<double>(
    "osd_mclock_scheduler_low_delay");
  res = conf.get_val<double>(
    "osd_mclock_scheduler_low_res");
  lim = conf.get_val<double>(
    "osd_mclock_scheduler_low_lim");
  wgt = conf.get_val<uint64_t>(
    "osd_mclock_scheduler_low_wgt");
  default_external_low_info.update(
  get_res(res, res_bandwidth),
  wgt,
  get_lim(lim, lim_bandwidth),
  get_delay(delay));


//咯咯哒
  std::ofstream out("/home/mxqh/software/ceph/a_client_infos.txt", std::ios_base::app);

  // Check if the file is open before trying to write to it
  if (out.is_open()) {

    out << "capacity_per_shard: " << capacity_per_shard  << "\n";


    // Client
    out << "Client\tR: " << default_external_client_info.reservation
        << ", W: " << default_external_client_info.weight
        << ", L: " << default_external_client_info.limit 
        << ", D: " << default_external_client_info.delay<< "\n";

    // HIGH
    out << "high\tR: " << default_external_high_info.reservation
        << ", W: " << default_external_high_info.weight
        << ", L: " << default_external_high_info.limit 
        << ", D: " << default_external_high_info.delay<< "\n";

    // LOW
    out << "low\tR: " << default_external_low_info.reservation
        << ", W: " << default_external_low_info.weight
        << ", L: " << default_external_low_info.limit 
        << ", D: " << default_external_low_info.delay <<"\n";

    // 恢复
    auto& bg_recovery_info = internal_client_infos[static_cast<size_t>(op_scheduler_class::background_recovery)];
    out << "Recovery\tR: " << bg_recovery_info.reservation
        << ", W: " << bg_recovery_info.weight
        << ", L: " << bg_recovery_info.limit 
        << ", D: " << bg_recovery_info.delay<< "\n";
    
    // background_medium
    auto& bg_medium_info = internal_client_infos[static_cast<size_t>(op_scheduler_class::background_medium)];
    out << "Recovery\tR: " << bg_medium_info.reservation
        << ", W: " << bg_medium_info.weight
        << ", L: " << bg_medium_info.limit 
        << ", D: " << bg_medium_info.delay<< "\n";

    // 尽力而为
    auto& bg_best_effort_info = internal_client_infos[static_cast<size_t>(op_scheduler_class::background_best_effort)];
    out << "effort\tR: " << bg_best_effort_info.reservation
        << ", W: " << bg_best_effort_info.weight
        << ", L: " << bg_best_effort_info.limit 
        << ", D: " << bg_best_effort_info.delay<< "\n";

    out << "\n";

    out.close();

  } else {
    std::cerr << "Unable to open file for writing: /home/mxqh/software/ceph/a_client_infos.txt" << std::endl;
  }


}


//只更新权重参数
void mClockScheduler::ClientRegistry::update_wgt(uint64_t high, uint64_t client, uint64_t low, uint64_t background_recovery, uint64_t background_medium, uint64_t background_best_effort)
{
  default_external_high_info.update_wgt(high);
  default_external_client_info.update_wgt(client);
  default_external_low_info.update_wgt(low);
  internal_client_infos[static_cast<size_t>(op_scheduler_class::background_recovery)].update_wgt(background_recovery);
  internal_client_infos[static_cast<size_t>(op_scheduler_class::background_medium)].update_wgt(background_medium);
  internal_client_infos[static_cast<size_t>(op_scheduler_class::background_best_effort)].update_wgt(background_best_effort);
}


// 根据给定的客户端配置ID，返回相应的ClientInfo对象。如果找不到特定的客户端，则返回默认的ClientInfo。
const dmc::ClientInfo *mClockScheduler::ClientRegistry::get_external_client(
  const client_profile_id_t &client) const
{
  auto ret = external_client_infos.find(client);
  if (ret == external_client_infos.end())
    return &default_external_client_info;
  else
    return &(ret->second);
}


// 根据操作的调度器ID返回对应的ClientInfo对象。这是根据操作的类别（如即时、客户端、背景恢复等）来决定的。
const dmc::ClientInfo *mClockScheduler::ClientRegistry::get_info(
  const scheduler_id_t &id) const {
  switch (id.class_id) {
  case op_scheduler_class::immediate:
    ceph_assert(0 == "Cannot schedule immediate");
    return (dmc::ClientInfo*)nullptr;
  case op_scheduler_class::client:
    return get_external_client(id.client_profile_id);

  case op_scheduler_class::high: // 添加对 high 类型的处理
    return &default_external_high_info;
    // return get_external_client(id.client_profile_id); // 或者根据实际情况返回不同的客户端信息
  case op_scheduler_class::low: // 添加对 mxqh 类型的处理
    return &default_external_low_info;
    // return get_external_client(id.client_profile_id); // 或者根据实际情况返回不同的客户端信息
  // 咯咯哒
  default:
    ceph_assert(static_cast<size_t>(id.class_id) < internal_client_infos.size());
    return &internal_client_infos[static_cast<size_t>(id.class_id)];
  }
}


// 设置OSD容量参数，这些参数基于配置文件中的值和OSD的属性（如是否为旋转磁盘）。这些参数对于计算QoS配置至关重要。
void mClockScheduler::set_osd_capacity_params_from_config()
{
  uint64_t osd_bandwidth_capacity;
  double osd_iop_capacity;
  std::tie(osd_bandwidth_capacity, osd_iop_capacity) = [&, this] {
    if (is_rotational) {
      return std::make_tuple(
        cct->_conf.get_val<Option::size_t>(
          "osd_mclock_max_sequential_bandwidth_hdd"),
        cct->_conf.get_val<double>("osd_mclock_max_capacity_iops_hdd"));
    } else {
      return std::make_tuple(
        cct->_conf.get_val<Option::size_t>(
          "osd_mclock_max_sequential_bandwidth_ssd"),
        cct->_conf.get_val<double>("osd_mclock_max_capacity_iops_ssd"));
    }
  }();

  osd_bandwidth_capacity = std::max<uint64_t>(1, osd_bandwidth_capacity);
  osd_iop_capacity = std::max<double>(1.0, osd_iop_capacity);

  osd_bandwidth_cost_per_io =
    static_cast<double>(osd_bandwidth_capacity) / osd_iop_capacity;
  osd_bandwidth_capacity_per_shard = static_cast<double>(osd_bandwidth_capacity)
    / static_cast<double>(num_shards);

    // 咯咯哒，打开文件进行持久化操作，使用 std::ios::trunc 模式覆盖写入
  std::ofstream files("/home/mxqh/software/ceph/a_capacity_per_shard.txt", std::ios::app);
  if (!files.is_open()) {
      std::cerr << "Error: Unable to open file for writing." << std::endl;
      return;
  }

  files << "OSD:" << whoami << "." << shard_id<< ":" << osd_bandwidth_capacity_per_shard <<std::endl;

  // 关闭文件
  files.close();


  dout(1) << __func__ << ": osd_bandwidth_cost_per_io: "
          << std::fixed << std::setprecision(2)
          << osd_bandwidth_cost_per_io << " bytes/io"
          << ", osd_bandwidth_capacity_per_shard "
          << osd_bandwidth_capacity_per_shard << " bytes/second"
          << dendl;
}



/**
 * profile_t
 *
 * mclock profile -- 3 params for each of 3 client classes
 * 0 (min): specifies no minimum reservation
 * 0 (max): specifies no upper limit
 */

//  延迟处理

struct profile_t {
  struct client_config_t {
    double reservation;
    uint64_t weight;
    double limit;
    double delay;
  };
  client_config_t client;
  client_config_t background_recovery;
  client_config_t background_medium;        //咯咯哒
  client_config_t background_best_effort;

  //咯咯哒
  client_config_t high;
  client_config_t low;
};

// 输出某个客户端的参数值——————延迟处理
static std::ostream &operator<<(
  std::ostream &lhs, const profile_t::client_config_t &rhs)
{
  return lhs << "{res: " << rhs.reservation
             << ", wgt: " << rhs.weight
             << ", lim: " << rhs.limit
             << ", delay: " << rhs.delay
             << "}";
}

// 输出每种客户的参数值——————延迟处理
static std::ostream &operator<<(std::ostream &lhs, const profile_t &rhs)
{
  return lhs << "[client: " << rhs.client
             << ", background_recovery: " << rhs.background_recovery
             << ", background_medium: " << rhs.background_medium
             << ", background_best_effort: " << rhs.background_best_effort
             << ", high: " << rhs.high
             << ", low: " << rhs.low
             << "]";
}



// 根据mClock配置文件设置QoS参数的默认值。这些配置文件定义了不同操作类别的默认预留、权重和限制。——————延迟处理
void mClockScheduler::set_config_defaults_from_profile()
{
  
	// Let only a single osd shard (id:0) set the profile config
//  if (shard_id > 0) {
//    return;
//  }

  /**
   * high_client_ops
   *
   * Client Allocation:
   *   reservation: 60% | weight: 2 | limit: 0 (max) |
   * Background Recovery Allocation:
   *   reservation: 40% | weight: 1 | limit: 0 (max) |
   * Background Best Effort Allocation:
   *   reservation: 0 (min) | weight: 1 | limit: 70% |
   */

  // 咯咯哒
  static constexpr profile_t high_client_ops_profile{
    { 0,  138,  0,  0 },        //client
    { 0,  240,  0,  0 },	  //background_recovery
    { 0,  160,  0,  0 },        //background_medium
    { 0,    1,  0,  0 },	  //background_best_effort
    { 0,  415, .7,  0 },	  //high
    { 0,   46,  0,  0 }	  //low

  };



  /**
   * high_recovery_ops
   *
   * Client Allocation:
   *   reservation: 30% | weight: 1 | limit: 0 (max) |
   * Background Recovery Allocation:
   *   reservation: 70% | weight: 2 | limit: 0 (max) |
   * Background Best Effort Allocation:
   *   reservation: 0 (min) | weight: 1 | limit: 0 (max) |
   */
  static constexpr profile_t high_recovery_ops_profile{
    { 0,  69,  0,  0 },	   //client
    { 0, 420,  0,  0 },	   //background_recovery
    { 0, 280,  0,  0 },    //background_medium
    { 0,   1,  0,  0 },	   //background_best_effort
    { 0, 208,  0,  0 },	   //high
    { 0,  23,  0,  0 }	   //low
  };



  /**
   * balanced
   *
   * Client Allocation:
   *   reservation: 50% | weight: 1 | limit: 0 (max) |
   * Background Recovery Allocation:
   *   reservation: 50% | weight: 1 | limit: 0 (max) |
   * Background Best Effort Allocation:
   *   reservation: 0 (min) | weight: 1 | limit: 90% |
   */
  // static constexpr profile_t balanced_profile{
  //   { .5, 1, 0 },       //client
  //   { .5, 1, 0 },       //background_recovery
  //   {  0, 1, 0 },       //background_medium
  //   {  0, 1, .9},       //background_best_effort
  //   { .5, 1, 0 },       //high
  //   {  0, 1, .9}        //low
  // };


    static constexpr profile_t balanced_profile{
    { 0, 115,   0,  0 },       //client
    { 0, 300,   0,  0 },       //background_recovery
    { 0, 200,   0,  0 },       //background_medium
    { 0,   1,  .9,  0 },       //background_best_effort
    { 0, 346,   0,  0 },       //high
    { 0,  39,   0,  0 }        //low
  };


  const profile_t *profile = nullptr;
  auto mclock_profile = cct->_conf.get_val<std::string>("osd_mclock_profile");

    // 更新模板成员变量
    if(mclock_profile != cur_mclock_profile){
        pre_mclock_profile = cur_mclock_profile;
        cur_mclock_profile = mclock_profile;
    }


  if (mclock_profile == "high_client_ops") {
    profile = &high_client_ops_profile;
    dout(10) << "Setting high_client_ops profile " << *profile << dendl;
  } else if (mclock_profile == "high_recovery_ops") {
    profile = &high_recovery_ops_profile;
    dout(10) << "Setting high_recovery_ops profile " << *profile << dendl;
  } else if (mclock_profile == "balanced") {
    profile = &balanced_profile;
    dout(10) << "Setting balanced profile " << *profile << dendl;
  } else if (mclock_profile == "custom") {
    dout(10) << "Profile set to custom, not setting defaults" << dendl;
    return;
  } else {
    derr << "Invalid mclock profile: " << mclock_profile << dendl;
    ceph_assert("Invalid choice of mclock profile" == 0);
    return;
  }


  //咯咯哒，如果
   if (shard_id > 0) {
    return;
  }


  ceph_assert(nullptr != profile);

  auto set_config = [&conf = cct->_conf](const char *key, auto val) {
    conf.set_val_default(key, std::to_string(val));
  };

  set_config("osd_mclock_scheduler_client_res", profile->client.reservation);
  set_config("osd_mclock_scheduler_client_wgt", profile->client.weight);
  set_config("osd_mclock_scheduler_client_lim", profile->client.limit);
  set_config("osd_mclock_scheduler_client_delay", profile->client.limit);

  set_config(
    "osd_mclock_scheduler_background_recovery_res",
    profile->background_recovery.reservation);
  set_config(
    "osd_mclock_scheduler_background_recovery_wgt",
    profile->background_recovery.weight);
  set_config(
    "osd_mclock_scheduler_background_recovery_lim",
    profile->background_recovery.limit);
    set_config(
    "osd_mclock_scheduler_background_recovery_delay",
    profile->background_recovery.delay);

    set_config(
    "osd_mclock_scheduler_background_medium_res",
    profile->background_medium.reservation);
  set_config(
    "osd_mclock_scheduler_background_medium_wgt",
    profile->background_medium.weight);
  set_config(
    "osd_mclock_scheduler_background_medium_lim",
    profile->background_medium.limit);
  set_config(
    "osd_mclock_scheduler_background_medium_delay",
    profile->background_medium.delay);

  set_config(
    "osd_mclock_scheduler_background_best_effort_res",
    profile->background_best_effort.reservation);
  set_config(
    "osd_mclock_scheduler_background_best_effort_wgt",
    profile->background_best_effort.weight);
  set_config(
    "osd_mclock_scheduler_background_best_effort_lim",
    profile->background_best_effort.limit);
  set_config(
    "osd_mclock_scheduler_background_best_effort_delay",
    profile->background_best_effort.delay);

  // 咯咯哒
  set_config("osd_mclock_scheduler_high_res", profile->high.reservation);
  set_config("osd_mclock_scheduler_high_wgt", profile->high.weight);
  set_config("osd_mclock_scheduler_high_lim", profile->high.limit);
  set_config("osd_mclock_scheduler_high_delay", profile->high.delay);

  set_config("osd_mclock_scheduler_low_res", profile->low.reservation);
  set_config("osd_mclock_scheduler_low_wgt", profile->low.weight);
  set_config("osd_mclock_scheduler_low_lim", profile->low.limit);
  set_config("osd_mclock_scheduler_low_delay", profile->low.delay);




  cct->_conf.apply_changes(nullptr);
}



// 动态更新配置文件
void  mClockScheduler::dynamic_adjustment_config(){

	    // 获取各客户端的优先级
    double wgt_high = cct->_conf.get_val<uint64_t>(
      "osd_mclock_scheduler_high_wgt");
    double wgt_client = cct->_conf.get_val<uint64_t>(
      "osd_mclock_scheduler_client_wgt");
    double wgt_low = cct->_conf.get_val<uint64_t>(
      "osd_mclock_scheduler_low_wgt");

	LoadWeight high(((request_count.high.last_count*osd_bandwidth_cost_per_io)/osd_bandwidth_capacity_per_shard),wgt_high);
	LoadWeight client(((request_count.medium.last_count*osd_bandwidth_cost_per_io)/osd_bandwidth_capacity_per_shard),wgt_client);
	LoadWeight low(((request_count.low.last_count*osd_bandwidth_cost_per_io)/osd_bandwidth_capacity_per_shard),wgt_low);
	LoadWeight background_recovery(((request_count.background_recovery.last_count*osd_bandwidth_cost_per_io)/osd_bandwidth_capacity_per_shard),1);
	LoadWeight background_medium(((request_count.background_medium.last_count*osd_bandwidth_cost_per_io)/osd_bandwidth_capacity_per_shard),1);
	LoadWeight background_best_effort(((request_count.background_best_effort.last_count*osd_bandwidth_cost_per_io)/osd_bandwidth_capacity_per_shard),1);

       // 定义一个向量来按顺序存储指针
       std::vector<LoadWeight*> loadWeightList;

    double recovery_client_ratio = 1;

    // 如果是用户自定义模式则按照前一个模板设置比例
    std::string mclock_profile;
    if(cur_mclock_profile == "custom"){
          mclock_profile = pre_mclock_profile;
    }else{
          mclock_profile = cur_mclock_profile;
    }


    if (mclock_profile == "high_client_ops") {
        recovery_client_ratio = 4.0/6.0;
	// 将变量的指针存储到向量中
            loadWeightList.push_back(&high);
            loadWeightList.push_back(&background_recovery);
            loadWeightList.push_back(&client);
            loadWeightList.push_back(&low);
            loadWeightList.push_back(&background_medium);
            loadWeightList.push_back(&background_best_effort);	
    } else if (mclock_profile == "high_recovery_ops") {
        recovery_client_ratio = 7.0/3.0;
	// 将变量的指针存储到向量中
            loadWeightList.push_back(&high);
            loadWeightList.push_back(&background_recovery);
            loadWeightList.push_back(&background_medium);
            loadWeightList.push_back(&client);
            loadWeightList.push_back(&low);
            loadWeightList.push_back(&background_best_effort);
    } else if (mclock_profile == "balanced") {
        recovery_client_ratio = 1;
	// 将变量的指针存储到向量中
            loadWeightList.push_back(&high);
            loadWeightList.push_back(&background_recovery);
            loadWeightList.push_back(&client);
            loadWeightList.push_back(&background_medium);
            loadWeightList.push_back(&low);
            loadWeightList.push_back(&background_best_effort);
    } else {
        return;
    }


    //初始化负载的值



    double background_recovery_ratio = 0.6;
    double background_medium_ratio = 0.4;
    double background_best_effort_ratio = 0;



    if(request_count.background_recovery.last_count+request_count.background_medium.last_count)
    {
	  //  double load = (request_count.getTotalCount()*osd_bandwidth_cost_per_io)/osd_bandwidth_capacity_per_shard;


	    background_recovery_ratio = request_count.background_recovery.last_count/(request_count.background_recovery.last_count+request_count.background_medium.last_count);
	    background_medium_ratio = request_count.background_medium.last_count/(request_count.background_recovery.last_count+request_count.background_medium.last_count);
	    background_best_effort_ratio = 0;


    }





    // 计算恢复、尽力而为权重参数
    double wgt_client_sum = wgt_high*client_count.high+wgt_client*client_count.client+wgt_low*client_count.low;
   
    double wgt_recovery = wgt_client_sum * recovery_client_ratio;

    double wgt_background_recovery = wgt_recovery * background_recovery_ratio;
    double wgt_background_medium = wgt_recovery * background_medium_ratio;
    double wgt_background_best_effort = wgt_recovery * background_best_effort_ratio;



    //更新基础权重（只按比例不考虑负载变化）
    background_recovery.wgt = wgt_background_recovery>=1?wgt_background_recovery:1;
    background_medium.wgt = wgt_background_medium>=1?wgt_background_medium:1;
    background_best_effort.wgt = wgt_background_best_effort>=1?wgt_background_best_effort:1;




    //咯咯哒 打印当前模板信息
    std::ofstream log_file("/home/mxqh/software/ceph/a_profile.txt", std::ios::app); // 打开日志文件进行追加
    if (log_file.is_open()) {

        log_file << "client_wgt:" << client.wgt << "*" << client_count.client <<std::endl;
        log_file << "high_wgt:" << high.wgt << "*" << client_count.high << std::endl;
        log_file << "low_wgt:" << low.wgt << "*" << client_count.low << std::endl;
        log_file << "wgt_background_recovery:" << background_recovery.wgt << std::endl;
  log_file << "wgt_background_medium:" << background_medium.wgt << std::endl;
        log_file << "wgt_background_best_effort:" << background_best_effort.wgt << std::endl;
        // 获取并打印每个客户端模板信息

    }




   // 动态调整权重的值（考虑负载）————整体负载小于70%的时候没必要进行调整
	double total_load = (request_count.getTotalCount() * osd_bandwidth_cost_per_io) / osd_bandwidth_capacity_per_shard;
	// high, background_recovery 两个优先级不进行限速
  double sum_load = loadWeightList[0]->load + loadWeightList[1]->load;
	if (total_load > 0.7)
	{
	    //// high, background_recovery 两个优先级不进行限速
	    //double sum_load = loadWeightList[0]->load + loadWeightList[1]->load;
	    bool flag_1 = false; // 负载累加和是否超过1
	    if (sum_load > 1.0)
	    {
	        flag_1 = true;
	    }
	
	    // 从第三个元素开始遍历，检查是否需要进行限速
	    for (size_t i = 2; i < loadWeightList.size(); ++i) {
	        if (!flag_1) {
	            sum_load += loadWeightList[i]->load;
	            if (sum_load > 1.0) {
	                loadWeightList[i]->wgt = loadWeightList[i]->wgt * std::exp(-6 * (total_load - 0.7));
	                flag_1 = true;
	            }
	        } else {
	            loadWeightList[i]->wgt = 1;
	        }
	    }

	}


	//打印负载信息
	if (log_file.is_open()) {
                log_file << std::endl;
                log_file << "total_load"<< total_load << std::endl;
                log_file << "sum_load:" << sum_load <<std::endl;
                log_file << std::endl;
            }



   //更新权重参数
    client_registry.update_wgt(static_cast<uint64_t>(high.wgt>=1?high.wgt:1),
   static_cast<uint64_t>(client.wgt>=1?client.wgt:1),
   static_cast<uint64_t>(low.wgt>=1?low.wgt:1),
   static_cast<uint64_t>(background_recovery.wgt>=1?background_recovery.wgt:1),
   static_cast<uint64_t>(background_medium.wgt>=1?background_medium.wgt:1),
   static_cast<uint64_t>(background_best_effort.wgt>=1?background_best_effort.wgt:1));



    // 更新mclock中的模板参数(只需动态两个即可*******************)
    scheduler.update_client_infos();

    //获取当前时间
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::tm* now_tm = std::localtime(&now_c);



    //咯咯哒 打印当前模板信息
    if (log_file.is_open()) {

        //打印客户端数量
	//wgt_client_sum = wgt_high*client_count.high+wgt_client*client_count.client+wgt_low*client_count.low;
	log_file << std::endl;


        log_file << cur_mclock_profile <<":" << (now_tm->tm_hour) << ":" << (now_tm->tm_min) << ":" << (now_tm->tm_sec)  << std::endl;
        log_file << "client_wgt:" << client.wgt << "*" << client_count.client <<std::endl;
        log_file << "high_wgt:" << high.wgt << "*" << client_count.high << std::endl;
	log_file << "low_wgt:" << low.wgt << "*" << client_count.low << std::endl;
	log_file << "wgt_background_recovery:" << background_recovery.wgt << std::endl;
  log_file << "wgt_background_medium:" << background_medium.wgt << std::endl;
	log_file << "wgt_background_best_effort:" << background_best_effort.wgt << std::endl;
        // 获取并打印每个客户端模板信息
	

	log_file << std::endl;
	log_file << std::endl;
	log_file << std::endl;

        log_file.close(); // 关闭日志文件
    }


}








// 设置用户优先级
void mClockScheduler::set_user_priority(const std::string& username, int priority) {
    user_priorities[username] = priority;
}




// 咯咯哒
void mClockScheduler::do_clean() {
    auto now = Clock::now();
    DataGuard g(data_mtx);

    int erased_count = 0;
    idle_count = 0;

    for (auto it = current_users.begin(); it != current_users.end(); /* empty */) {
        auto& user = it->second;
        auto it2 = it++;

        auto user_last_access = user.last_access_time;

        if (user_last_access <= now - erase_age) {
            current_users.erase(it2);
            erased_count++;
//            client_count--;
            switch(user.priority){
                case 1 : client_count.high--; break;
                case 2 : client_count.client--; break;
                case 3 : client_count.low--; break;
              }

        } else if (user_last_access <= now - idle_age) {
            idle_count++;
        }
    }


    cleaning_job->try_update(aggressive_check_time);
}

// 计算一个操作的缩放成本，这是一个根据操作大小和I/O类型（顺序或随机）动态计算的值。
uint32_t mClockScheduler::calc_scaled_cost(int item_cost)
{
  auto cost = static_cast<uint32_t>(
    std::max<int>(
      1, // ensure cost is non-zero and positive
      item_cost));
  auto cost_per_io = static_cast<uint32_t>(osd_bandwidth_cost_per_io);

  return std::max<uint32_t>(cost, cost_per_io);
}


// 应用配置更改。这通常在配置更新后调用，以确保mClockScheduler使用最新的配置。
void mClockScheduler::update_configuration()
{
  // Apply configuration change. The expectation is that
  // at least one of the tracked mclock config option keys
  // is modified before calling this method.
  cct->_conf.apply_changes(nullptr);
}


// 将调度器的当前状态输出到给定的格式化程序中，用于调试和监控。
void mClockScheduler::dump(ceph::Formatter &f) const
{
  // Display queue sizes
  f.open_object_section("queue_sizes");
  f.dump_int("immediate", immediate.size());
  f.dump_int("scheduler", scheduler.request_count());
  f.close_section();

  // client map and queue tops (res, wgt, lim)
  std::ostringstream out;
  f.open_object_section("mClockClients");
  f.dump_int("client_count", scheduler.client_count());
  out << scheduler;
  f.dump_string("clients", out.str());
  f.close_section();

  // Display sorted queues (res, wgt, lim)
  f.open_object_section("mClockQueues");
  f.dump_string("queues", display_queues());
  f.close_section();
}



// 将一个操作项加入到调度队列中。如果操作是即时的，则放入即时队列；否则，根据其QoS成本加入mClock调度队列。
void mClockScheduler::enqueue(OpSchedulerItem&& item)
{
  auto id = get_scheduler_id(item);

  // TODO: move this check into OpSchedulerItem, handle backwards compat
  if (op_scheduler_class::immediate == id.class_id) {
    immediate.push_front(std::move(item));
  } else {
    auto cost = calc_scaled_cost(item.get_cost());
    item.set_qos_cost(cost);
    dout(20) << __func__ << " " << id
             << " item_cost: " << item.get_cost()
             << " scaled_cost: " << cost
             << dendl;

    


    // 加入当前客户端队列
std::string name = item.get_entityName();

if(current_users.find(name) == current_users.end()){
    int priority = mClockScheduler::get_user_priority(name);
    UserInfo user(name, priority);
    current_users[name] = user;
//    client_count++;
    switch(priority){
            case 1 : client_count.high++;break;
            case 2 : client_count.client++;break;
            case 3 : client_count.low++;break;
          }

} else {
    current_users[name].last_access_time = Clock::now(); // 使用 steady_clock::now()
}


// 统计各类型的数量
    switch(id.class_id){
        case op_scheduler_class::background_recovery:
	    request_count.add("background_recovery", this);
            break;
        case op_scheduler_class::background_medium:
	    request_count.add("background_medium", this);
            break;
        case op_scheduler_class::background_best_effort:
	    request_count.add("background_best_effort", this);
            break;
        case op_scheduler_class::immediate:
	    request_count.add("immediate", this);
            break;
        case op_scheduler_class::client:
	    request_count.add("client", this);
            break;
        case op_scheduler_class::high:
	    request_count.add("high", this);
            break;
        case op_scheduler_class::low:
	    request_count.add("low", this);
            break;
        default:
            break;
    }

    // Add item to scheduler queue
    scheduler.add_request(
      std::move(item),
      id,
      cost);
  }

 dout(20) << __func__ << " client_count: " << scheduler.client_count()
          << " queue_sizes: [ imm: " << immediate.size()
          << " sched: " << scheduler.request_count() << " ]"
          << dendl;
 dout(30) << __func__ << " mClockClients: "
          << scheduler
          << dendl;
 dout(30) << __func__ << " mClockQueues: { "
          << display_queues() << " }"
          << dendl;
}


// 将一个操作项加入到队列的前面。这通常用于高优先级或需要立即处理的操作。
void mClockScheduler::enqueue_front(OpSchedulerItem&& item)
{
  immediate.push_back(std::move(item));
  // TODO: item may not be immediate, update mclock machinery to permit
  // putting the item back in the queue
}


// 从队列中取出并返回下一个要处理的操作项。这个方法会首先检查即时队列，然后是mClock调度队列。
WorkItem mClockScheduler::dequeue()
{
  if (!immediate.empty()) {
    WorkItem work_item{std::move(immediate.back())};
    immediate.pop_back();
    return work_item;
  } else {


    mclock_queue_t::PullReq result = scheduler.pull_request();
    if (result.is_future()) {
      return result.getTime();
    } else if (result.is_none()) {
      ceph_assert(
	0 == "Impossible, must have checked empty() first");
      return {};
    } else {
      ceph_assert(result.is_retn());

      auto &retn = result.get_retn();
      return std::move(*retn.request);
    }
  }
}


// 返回一个表示当前队列状态的字符串，用于调试和监控。
std::string mClockScheduler::display_queues() const
{
  std::ostringstream out;
  scheduler.display_queues(out);
  return out.str();
}



// 返回一个字符串数组，包含mClockScheduler关注的配置项键名。这用于配置更新机制。——————延迟处理
const char** mClockScheduler::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_mclock_scheduler_client_res",
    "osd_mclock_scheduler_client_wgt",
    "osd_mclock_scheduler_client_lim",
    "osd_mclock_scheduler_client_delay",
    "osd_mclock_scheduler_client_res_bandwidth",
    "osd_mclock_scheduler_client_lim_bandwidth",
    "osd_mclock_scheduler_background_recovery_res",
    "osd_mclock_scheduler_background_recovery_wgt",
    "osd_mclock_scheduler_background_recovery_lim",
    "osd_mclock_scheduler_background_recovery_delay",
    "osd_mclock_scheduler_background_recovery_res_bandwidth",
    "osd_mclock_scheduler_background_recovery_lim_bandwidth",
    "osd_mclock_scheduler_background_medium_res",
    "osd_mclock_scheduler_background_medium_wgt",
    "osd_mclock_scheduler_background_medium_lim",
    "osd_mclock_scheduler_background_medium_delay",
    "osd_mclock_scheduler_background_medium_res_bandwidth",
    "osd_mclock_scheduler_background_medium_lim_bandwidth",
    "osd_mclock_scheduler_background_best_effort_res",
    "osd_mclock_scheduler_background_best_effort_wgt",
    "osd_mclock_scheduler_background_best_effort_lim",
    "osd_mclock_scheduler_background_best_effort_delay",
    "osd_mclock_scheduler_background_best_effort_res_bandwidth",
    "osd_mclock_scheduler_background_best_effort_lim_bandwidth",
   
    // 咯咯哒
    "osd_mclock_scheduler_high_res",
    "osd_mclock_scheduler_high_wgt",
    "osd_mclock_scheduler_high_lim",
    "osd_mclock_scheduler_high_delay",
    "osd_mclock_scheduler_high_res_bandwidth",
    "osd_mclock_scheduler_high_lim_bandwidth",

    "osd_mclock_scheduler_low_res",
    "osd_mclock_scheduler_low_wgt",
    "osd_mclock_scheduler_low_lim",
    "osd_mclock_scheduler_low_delay",
    "osd_mclock_scheduler_low_wgt_bandwidth",
    "osd_mclock_scheduler_low_delay_bandwidth",

    "osd_mclock_max_capacity_iops_hdd",
    "osd_mclock_max_capacity_iops_ssd",
    "osd_mclock_max_sequential_bandwidth_hdd",
    "osd_mclock_max_sequential_bandwidth_ssd",
    "osd_mclock_profile",
    NULL
  };
  return KEYS;
}




// 处理配置变更事件。这个方法在配置发生变更时被调用，以更新内部状态和参数。——————延迟处理
void mClockScheduler::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed)
{
  if (changed.count("osd_mclock_max_capacity_iops_hdd") ||
      changed.count("osd_mclock_max_capacity_iops_ssd")) {
    set_osd_capacity_params_from_config();
    client_registry.update_from_config(
      conf, osd_bandwidth_capacity_per_shard, num_shards);
    mClockScheduler::dynamic_adjustment_config();
  }
  if (changed.count("osd_mclock_max_sequential_bandwidth_hdd") ||
      changed.count("osd_mclock_max_sequential_bandwidth_ssd")) {
    set_osd_capacity_params_from_config();
    client_registry.update_from_config(
      conf, osd_bandwidth_capacity_per_shard, num_shards);
    mClockScheduler::dynamic_adjustment_config();
  }
  if (changed.count("osd_mclock_profile")) {
    set_config_defaults_from_profile();
    client_registry.update_from_config(
      conf, osd_bandwidth_capacity_per_shard, num_shards);
    mClockScheduler::dynamic_adjustment_config();
  }

  auto get_changed_key = [&changed]() -> std::optional<std::string> {
    static const std::vector<std::string> qos_params = {
      "osd_mclock_scheduler_client_res",
      "osd_mclock_scheduler_client_wgt",
      "osd_mclock_scheduler_client_lim",
      "osd_mclock_scheduler_client_delay",
      "osd_mclock_scheduler_client_res_bandwidth",
      "osd_mclock_scheduler_client_lim_bandwidth",
      "osd_mclock_scheduler_background_recovery_res",
      "osd_mclock_scheduler_background_recovery_wgt",
      "osd_mclock_scheduler_background_recovery_lim",
      "osd_mclock_scheduler_background_recovery_delay",
      "osd_mclock_scheduler_background_recovery_res_bandwidth",
      "osd_mclock_scheduler_background_recovery_lim_bandwidth",
      "osd_mclock_scheduler_background_medium_res",
      "osd_mclock_scheduler_background_medium_wgt",
      "osd_mclock_scheduler_background_medium_lim",
      "osd_mclock_scheduler_background_medium_delay",
      "osd_mclock_scheduler_background_medium_res_bandwidth",
      "osd_mclock_scheduler_background_medium_lim_bandwidth",
      "osd_mclock_scheduler_background_best_effort_res",
      "osd_mclock_scheduler_background_best_effort_wgt",
      "osd_mclock_scheduler_background_best_effort_lim",
      "osd_mclock_scheduler_background_best_effort_delay",
      "osd_mclock_scheduler_background_best_effort_res_bandwidth",
      "osd_mclock_scheduler_background_best_effort_lim_bandwidth",

      // 咯咯哒
      "osd_mclock_scheduler_high_res",
      "osd_mclock_scheduler_high_wgt",
      "osd_mclock_scheduler_high_lim",
      "osd_mclock_scheduler_high_delay",
      "osd_mclock_scheduler_high_res_bandwidth",
      "osd_mclock_scheduler_high_lim_bandwidth",

      "osd_mclock_scheduler_low_res",
      "osd_mclock_scheduler_low_wgt",
      "osd_mclock_scheduler_low_lim",
      "osd_mclock_scheduler_low_delay",
      "osd_mclock_scheduler_low_res_bandwidth",
      "osd_mclock_scheduler_low_lim_bandwidth",
    };

    for (auto &qp : qos_params) {
      if (changed.count(qp)) {
        return qp;
      }
    }
    return std::nullopt;
  };

  if (auto key = get_changed_key(); key.has_value()) {
    auto mclock_profile = cct->_conf.get_val<std::string>("osd_mclock_profile");
    if (mclock_profile == "custom") {
      client_registry.update_from_config(
        conf, osd_bandwidth_capacity_per_shard, num_shards);
      mClockScheduler::dynamic_adjustment_config();
    } else {
      // Attempt to change QoS parameter for a built-in profile. Restore the
      // profile defaults by making one of the OSD shards remove the key from
      // config monitor store. Note: monc is included in the check since the
      // mock unit test currently doesn't initialize it.
      if (shard_id == 0 && monc) {
        static const std::vector<std::string> osds = {
          "osd",
          "osd." + std::to_string(whoami)
        };

        for (auto osd : osds) {
          std::string cmd =
            "{"
              "\"prefix\": \"config rm\", "
              "\"who\": \"" + osd + "\", "
              "\"name\": \"" + *key + "\""
            "}";
          std::vector<std::string> vcmd{cmd};

          dout(10) << __func__ << " Removing Key: " << *key
                   << " for " << osd << " from Mon db" << dendl;
          monc->start_mon_command(vcmd, {}, nullptr, nullptr, nullptr);
        }
      }
    }
    // Alternatively, the QoS parameter, if set ephemerally for this OSD via
    // the 'daemon' or 'tell' interfaces must be removed.
    if (!cct->_conf.rm_val(*key)) {
      dout(10) << __func__ << " Restored " << *key << " to default" << dendl;
      cct->_conf.apply_changes(nullptr);
    }
  }
}



// 析构函数进行清理工作，包括从配置观察者列表中移除当前对象。
mClockScheduler::~mClockScheduler()
{
  cct->_conf.remove_observer(this);
}

}
