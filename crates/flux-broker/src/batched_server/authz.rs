// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Authorization checks for broker operations.

use flux_common::ids::TopicId;
use flux_wire::{reader, writer};

use crate::auth::{Operation, Principal, ResourceType};
use crate::object_store::ObjectStore;

use super::BrokerState;

pub(crate) async fn can_append<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    principal: Option<&Principal>,
    req: &writer::AppendRequest,
) -> bool {
    if let Some(principal) = principal {
        for batch in &req.batches {
            let topic_name = batch.topic_id.0.to_string();
            let allowed = state
                .acl_checker
                .check(
                    principal,
                    ResourceType::Topic,
                    &topic_name,
                    Operation::Append,
                )
                .await;
            if !allowed {
                return false;
            }
        }
    }
    true
}

pub(crate) async fn can_read<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    principal: Option<&Principal>,
    req: &reader::ReadRequest,
) -> bool {
    if let Some(principal) = principal {
        let topic_name = req.topic_id.0.to_string();
        return state
            .acl_checker
            .check(
                principal,
                ResourceType::Topic,
                &topic_name,
                Operation::Consume,
            )
            .await;
    }
    true
}

pub(crate) async fn can_group_consume<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    principal: Option<&Principal>,
    group_id: &str,
) -> bool {
    if let Some(principal) = principal {
        return state
            .acl_checker
            .check(principal, ResourceType::Group, group_id, Operation::Consume)
            .await;
    }
    true
}

pub(crate) async fn can_consume_topic<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    principal: Option<&Principal>,
    topic_id: TopicId,
) -> bool {
    if let Some(principal) = principal {
        let topic_name = topic_id.0.to_string();
        return state
            .acl_checker
            .check(
                principal,
                ResourceType::Topic,
                &topic_name,
                Operation::Consume,
            )
            .await;
    }
    true
}
