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

// 定义了在 OSD 中进行操作调度所需的类和枚举类型.
// 定义了在 OSD 中进行操作调度所需的类和枚举类型，提供了针对不同操作的抽象和具体实现，并提供了相关的功能和方法。
#pragma once

#include <ostream>
#include <iostream>
#include <string>
#include <fstream>

#include "include/types.h"
#include "include/utime.h"
#include "osd/OpRequest.h"
#include "osd/PG.h"
#include "osd/PGPeeringEvent.h"
#include "messages/MOSDOp.h"


class OSD;
class OSDShard;

namespace ceph::osd::scheduler {

// 操作调度的类别
enum class op_scheduler_class : uint8_t {
  background_recovery = 0,
  background_medium,
  background_best_effort, 
  immediate,
  client,
  high,
  low,
//  zjz,
//  mxqh,
};

std::ostream& operator<<(std::ostream& out, const op_scheduler_class& class_id);



// 操作调度项————操作队列中的操作项，并提供了一些功能来获取和操作操作项的信息。
// OpSchedulerItem 类代表了一个请求的必要信息，包括优先级、成本等。每个 OpSchedulerItem 实例都代表着一个特定的请求，它包含了这个请求需要的所有信息。
class OpSchedulerItem {
public:
  // Abstraction for operations queueable in the op queue
  // 表示可以在操作队列中排队的操作
  // 定义了操作队列中可排队操作的基本接口，并且提供了一些默认实现。子类可以根据需要覆盖这些虚函数以实现特定类型的操作。
  class OpQueueable {
  public:
    std::string entityName;
    using Ref = std::unique_ptr<OpQueueable>;

    /// Items with the same queue token will end up in the same shard
    // 确定操作所属队列的令牌。相同令牌的操作将最终进入同一个分片。
    virtual uint32_t get_queue_token() const = 0;

    /* Items will be dequeued and locked atomically w.r.t. other items with the
       * same ordering token */
      // 用于确定操作顺序的令牌。具有相同排序令牌的操作将按照令牌的顺序进行处理。
    virtual const spg_t& get_ordering_token() const = 0;

    // 用于获取操作请求的可选指针。默认情况下，它返回一个空指针，表示操作请求不可用。
    virtual std::optional<OpRequestRef> maybe_get_op() const {
      return std::nullopt;
    }

    // 返回操作请求保留的推送次数。默认情况下，它返回0，表示没有保留的推送。
    virtual uint64_t get_reserved_pushes() const {
      return 0;
    }

    // 用于判断操作是否为peering操作。默认情况下，它返回false。
    virtual bool is_peering() const {
      return false;
    }

    // 用于判断操作是否需要 PG（原语组）。默认情况下，它会终止程序。
    virtual bool peering_requires_pg() const {
      ceph_abort();
    }

    // 返回操作创建的 PGCreateInfo 对象指针。默认情况下，它返回空指针。
    virtual const PGCreateInfo *creates_pg() const {
      return nullptr;
    }

    // 操作信息打印到给定的输出流中
    virtual std::ostream &print(std::ostream &rhs) const = 0;

    // 用于执行操作
    virtual void run(OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) = 0;

    // 返回操作所属的调度类别
    virtual op_scheduler_class get_scheduler_class() const = 0;


    virtual ~OpQueueable() {}
    friend std::ostream& operator<<(std::ostream& out, const OpQueueable& q) {
      return q.print(out);
    }

  };





private:
  OpQueueable::Ref qitem;
  int cost;
  unsigned priority;
  utime_t start_time;
  uint64_t owner;  ///< global id (e.g., client.XXX)  
  epoch_t map_epoch;    ///< an epoch we expect the PG to exist in

  /**
   * qos_cost
   *
   * Set by mClockScheduler iff queued into mclock proper and not the high/immediate queues.  
   * Represents mClockScheduler's adjusted cost value.
   */
  uint32_t qos_cost = 0;

  /// True iff queued via mclock proper, not the high/immediate queues
  // 检查操作是否通过 mClock 正确地排队，而不是通过高优先级或立即执行队列。
  bool was_queued_via_mclock() const {
    return qos_cost > 0;
  }

public:
  OpSchedulerItem(
    OpQueueable::Ref &&item,
    int cost,
    unsigned priority,
    utime_t start_time,
    uint64_t owner,
    epoch_t e)
    : qitem(std::move(item)),
      cost(cost),
      priority(priority),
      start_time(start_time),
      owner(owner),
      map_epoch(e) {}

  // 定义了移动构造函数和移动赋值运算符，并禁用了复制构造函数和复制赋值运算符。
  OpSchedulerItem(OpSchedulerItem &&) = default;
  OpSchedulerItem(const OpSchedulerItem &) = delete;
  OpSchedulerItem &operator=(OpSchedulerItem &&) = default;
  OpSchedulerItem &operator=(const OpSchedulerItem &) = delete;

  uint32_t get_queue_token() const {
    return qitem->get_queue_token();
  }
  const spg_t& get_ordering_token() const {
    return qitem->get_ordering_token();
  }
  std::optional<OpRequestRef> maybe_get_op() const {
    return qitem->maybe_get_op();
  }
  uint64_t get_reserved_pushes() const {
    return qitem->get_reserved_pushes();
  }
  void run(OSD *osd, OSDShard *sdata,PGRef& pg, ThreadPool::TPHandle &handle) {
    qitem->run(osd, sdata, pg, handle);
  }
  unsigned get_priority() const { return priority; }
  int get_cost() const { return cost; }

  //咯咯哒
  std::string get_entityName()const { return qitem->entityName; }


  utime_t get_start_time() const { return start_time; }
  uint64_t get_owner() const { return owner; }
  epoch_t get_map_epoch() const { return map_epoch; }

  bool is_peering() const {
    return qitem->is_peering();
  }

  const PGCreateInfo *creates_pg() const {
    return qitem->creates_pg();
  }

  bool peering_requires_pg() const {
    return qitem->peering_requires_pg();
  }

  op_scheduler_class get_scheduler_class() const {
    return qitem->get_scheduler_class();
  }

// 设置操作的服务质量成本
  void set_qos_cost(uint32_t scaled_cost) {
    qos_cost = scaled_cost;
  }

  friend std::ostream& operator<<(std::ostream& out, const OpSchedulerItem& item) {
    out << "OpSchedulerItem("
        << item.get_ordering_token() << " " << *item.qitem;

    out << " class_id " << item.get_scheduler_class();

    out << " prio " << item.get_priority();

    if (item.was_queued_via_mclock()) {
      out << " qos_cost " << item.qos_cost;
    }

    out << " cost " << item.get_cost()
        << " e" << item.get_map_epoch();

    if (item.get_reserved_pushes()) {
      out << " reserved_pushes " << item.get_reserved_pushes();
    }

    return out << ")";
  }
}; // class OpSchedulerItem










/// Implements boilerplate for operations queued for the pg lock
// 实现针对 PG 锁定的操作的基本操作
class PGOpQueueable : public OpSchedulerItem::OpQueueable {
  spg_t pgid;
protected:
  const spg_t& get_pgid() const {
    return pgid;
  }

// 用于根据操作的优先级确定操作的调度器类别。
// 如果优先级高于或等于 CEPH_MSG_PRIO_HIGH，则返回 immediate 类型；
// 如果优先级高于或等于 PeeringState::recovery_msg_priority_t::FORCED， 则返回 background_recovery 类型；
// 如果优先级高于或等于 PeeringState::recovery_msg_priority_t::DEGRADED， 则返回 background_medium 类型；
// 否则返回 background_best_effort 类型。
  static op_scheduler_class priority_to_scheduler_class(int priority) {
    if (priority >= CEPH_MSG_PRIO_HIGH) {
      return op_scheduler_class::immediate;
    } else if (priority >= PeeringState::recovery_msg_priority_t::FORCED) {
      return op_scheduler_class::background_recovery;
    } else if (priority >= PeeringState::recovery_msg_priority_t::DEGRADED) {
      return op_scheduler_class::background_medium;
    } else {
      return op_scheduler_class::background_best_effort;
    }
  }

public:
  explicit PGOpQueueable(spg_t pg) : pgid(pg) {}
  uint32_t get_queue_token() const final {
    return get_pgid().ps();
  }

  const spg_t& get_ordering_token() const final {
    return get_pgid();
  }
};




int get_priorities(const std::string& username);


// 表示针对特定 PG 的操作项，并提供了打印操作信息、获取操作请求、确定操作调度器类别的功能。
class PGOpItem : public PGOpQueueable {
  OpRequestRef op;

public:
  PGOpItem(spg_t pg, OpRequestRef op) : PGOpQueueable(pg), op(std::move(op)) {
  
  updateAndGetEntityName();
  
  }

  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGOpItem(op=" << *(op->get_req()) << ")";
  }

 // 返回存储在 op 成员变量中的操作请求。
  std::optional<OpRequestRef> maybe_get_op() const final {
    return op;
  }


  std::string updateAndGetEntityName(){
  const Message *msg = op->get_req();
	entityName = msg->get_connection()->get_peer_entity_name().to_str();

  return entityName;
}

  op_scheduler_class get_scheduler_class() const final {
    auto type = op->get_req()->get_type();
    if (type == CEPH_MSG_OSD_OP ||
	type == CEPH_MSG_OSD_BACKOFF) {
    
	
	// 用于测试恢复是否可以正常工作
	    if(entityName == "client.recovery")
	    	return op_scheduler_class::background_recovery;
	    else if(entityName == "client.effort")
	    	return op_scheduler_class::background_best_effort;

	int priority=get_priorities(entityName);

           switch (priority){
              case 1: return op_scheduler_class::high;
              case 2: return op_scheduler_class::client;
              case 3: return op_scheduler_class::low;
              default: return op_scheduler_class::client;
              }

    } else {
      return op_scheduler_class::immediate;
    }
  }

  void run(OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
};




// 用于表示针对特定 PG 的对等事件，并提供了打印事件信息、确定事件是否为对等事件、获取对等事件是否需要特定的 PG、获取创建 PG 的信息等功能。
class PGPeeringItem : public PGOpQueueable {
  PGPeeringEventRef evt;
public:
  PGPeeringItem(spg_t pg, PGPeeringEventRef e) : PGOpQueueable(pg), evt(e) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGPeeringEvent(" << evt->get_desc() << ")";
  }
  void run(OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  bool is_peering() const override {
    return true;
  }
  bool peering_requires_pg() const override {
    return evt->requires_pg;
  }
  const PGCreateInfo *creates_pg() const override {
    return evt->create_info.get();
  }
  op_scheduler_class get_scheduler_class() const final {
    return op_scheduler_class::immediate;
  }
};



// 针对特定 PG 的快照修剪操作，并提供了打印操作信息的功能。
class PGSnapTrim : public PGOpQueueable {
  epoch_t epoch_queued;
public:
  PGSnapTrim(
    spg_t pg,
    epoch_t epoch_queued)
    : PGOpQueueable(pg), epoch_queued(epoch_queued) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGSnapTrim(pgid=" << get_pgid()
	       << " epoch_queued=" << epoch_queued
	       << ")";
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return op_scheduler_class::background_best_effort;
  }
};


// 用于表示针对特定 PG 的扫描操作
class PGScrub : public PGOpQueueable {
  epoch_t epoch_queued;
public:
  PGScrub(
    spg_t pg,
    epoch_t epoch_queued)
    : PGOpQueueable(pg), epoch_queued(epoch_queued) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGScrub(pgid=" << get_pgid()
	       << "epoch_queued=" << epoch_queued
	       << ")";
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return op_scheduler_class::background_best_effort;
  }
};





// 用于表示 PG 的扫描操作的具体项。
class PGScrubItem : public PGOpQueueable {
 protected:
  epoch_t epoch_queued;
  Scrub::act_token_t activation_index;
  std::string_view message_name;
  PGScrubItem(spg_t pg, epoch_t epoch_queued, std::string_view derivative_name)
      : PGOpQueueable{pg}
      , epoch_queued{epoch_queued}
      , activation_index{0}
      , message_name{derivative_name}
  {}
  PGScrubItem(spg_t pg,
	      epoch_t epoch_queued,
	      Scrub::act_token_t op_index,
	      std::string_view derivative_name)
      : PGOpQueueable{pg}
      , epoch_queued{epoch_queued}
      , activation_index{op_index}
      , message_name{derivative_name}
  {}
  std::ostream& print(std::ostream& rhs) const final
  {
    return rhs << message_name << "(pgid=" << get_pgid()
	       << "epoch_queued=" << epoch_queued
	       << " scrub-token=" << activation_index << ")";
  }
  void run(OSD* osd,
	   OSDShard* sdata,
	   PGRef& pg,
	   ThreadPool::TPHandle& handle) override = 0;
  op_scheduler_class get_scheduler_class() const final
  {
    return op_scheduler_class::background_best_effort;
  }
};








// 重新调度 PG 的扫描操作的具体项，它提供了具体的实现来处理重新调度扫描操作的逻辑。
class PGScrubResched : public PGScrubItem {
 public:
  PGScrubResched(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubResched"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

/**
 *  all replicas have granted our scrub resources request
 */
// 所有副本已经授予了对扫描资源的请求的确认项，它提供了具体的实现来处理这种确认消息的逻辑。
class PGScrubResourcesOK : public PGScrubItem {
 public:
  PGScrubResourcesOK(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubResourcesOK"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

/**
 *  scrub resources requests denied by replica(s)
 */
class PGScrubDenied : public PGScrubItem {
 public:
  PGScrubDenied(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubDenied"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

/**
 *  called when a repair process completes, to initiate scrubbing. No local/remote
 *  resources are allocated.
 */
class PGScrubAfterRepair : public PGScrubItem {
 public:
  PGScrubAfterRepair(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubAfterRepair"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubPushesUpdate : public PGScrubItem {
 public:
  PGScrubPushesUpdate(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubPushesUpdate"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubAppliedUpdate : public PGScrubItem {
 public:
  PGScrubAppliedUpdate(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubAppliedUpdate"}
  {}
  void run(OSD* osd,
	   OSDShard* sdata,
	   PGRef& pg,
	   [[maybe_unused]] ThreadPool::TPHandle& handle) final;
};

class PGScrubUnblocked : public PGScrubItem {
 public:
  PGScrubUnblocked(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubUnblocked"}
  {}
  void run(OSD* osd,
	   OSDShard* sdata,
	   PGRef& pg,
	   [[maybe_unused]] ThreadPool::TPHandle& handle) final;
};

class PGScrubDigestUpdate : public PGScrubItem {
 public:
  PGScrubDigestUpdate(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubDigestUpdate"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubGotLocalMap : public PGScrubItem {
 public:
  PGScrubGotLocalMap(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubGotLocalMap"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubGotReplMaps : public PGScrubItem {
 public:
  PGScrubGotReplMaps(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubGotReplMaps"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubMapsCompared : public PGScrubItem {
 public:
  PGScrubMapsCompared(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubMapsCompared"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGRepScrub : public PGScrubItem {
 public:
  PGRepScrub(spg_t pg, epoch_t epoch_queued, Scrub::act_token_t op_token)
      : PGScrubItem{pg, epoch_queued, op_token, "PGRepScrub"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGRepScrubResched : public PGScrubItem {
 public:
  PGRepScrubResched(spg_t pg, epoch_t epoch_queued, Scrub::act_token_t op_token)
      : PGScrubItem{pg, epoch_queued, op_token, "PGRepScrubResched"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubReplicaPushes : public PGScrubItem {
 public:
  PGScrubReplicaPushes(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubReplicaPushes"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubScrubFinished : public PGScrubItem {
 public:
  PGScrubScrubFinished(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubScrubFinished"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubGetNextChunk : public PGScrubItem {
 public:
  PGScrubGetNextChunk(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubGetNextChunk"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubChunkIsBusy : public PGScrubItem {
 public:
  PGScrubChunkIsBusy(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubChunkIsBusy"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubChunkIsFree : public PGScrubItem {
 public:
  PGScrubChunkIsFree(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubChunkIsFree"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGRecovery : public PGOpQueueable {
  utime_t time_queued;
  epoch_t epoch_queued;
  uint64_t reserved_pushes;
  int priority;
public:
  PGRecovery(
    spg_t pg,
    epoch_t epoch_queued,
    uint64_t reserved_pushes,
    int priority)
    : PGOpQueueable(pg),
      time_queued(ceph_clock_now()),
      epoch_queued(epoch_queued),
      reserved_pushes(reserved_pushes),
      priority(priority) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGRecovery(pgid=" << get_pgid()
	       << " epoch_queued=" << epoch_queued
	       << " reserved_pushes=" << reserved_pushes
	       << ")";
  }
  uint64_t get_reserved_pushes() const final {
    return reserved_pushes;
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return priority_to_scheduler_class(priority);
  }
};

class PGRecoveryContext : public PGOpQueueable {
  utime_t time_queued;
  std::unique_ptr<GenContext<ThreadPool::TPHandle&>> c;
  epoch_t epoch;
  int priority;
public:
  PGRecoveryContext(spg_t pgid,
		    GenContext<ThreadPool::TPHandle&> *c, epoch_t epoch,
		    int priority)
    : PGOpQueueable(pgid),
      time_queued(ceph_clock_now()),
      c(c), epoch(epoch), priority(priority) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGRecoveryContext(pgid=" << get_pgid()
	       << " c=" << c.get() << " epoch=" << epoch
	       << ")";
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return priority_to_scheduler_class(priority);
  }
};

class PGDelete : public PGOpQueueable {
  epoch_t epoch_queued;
public:
  PGDelete(
    spg_t pg,
    epoch_t epoch_queued)
    : PGOpQueueable(pg),
      epoch_queued(epoch_queued) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGDelete(" << get_pgid()
	       << " e" << epoch_queued
	       << ")";
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return op_scheduler_class::background_best_effort;
  }
};

class PGRecoveryMsg : public PGOpQueueable {
  utime_t time_queued;
  OpRequestRef op;

public:
  PGRecoveryMsg(spg_t pg, OpRequestRef op)
    : PGOpQueueable(pg), time_queued(ceph_clock_now()), op(std::move(op)) {}

  static bool is_recovery_msg(OpRequestRef &op) {
    switch (op->get_req()->get_type()) {
    case MSG_OSD_PG_PUSH:
    case MSG_OSD_PG_PUSH_REPLY:
    case MSG_OSD_PG_PULL:
    case MSG_OSD_PG_BACKFILL:
    case MSG_OSD_PG_BACKFILL_REMOVE:
    case MSG_OSD_PG_SCAN:
      return true;
    default:
      return false;
    }
  }

  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGRecoveryMsg(op=" << *(op->get_req()) << ")";
  }

  std::optional<OpRequestRef> maybe_get_op() const final {
    return op;
  }

  op_scheduler_class get_scheduler_class() const final {
    return priority_to_scheduler_class(op->get_req()->get_priority());
  }

  void run(OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
};

}
