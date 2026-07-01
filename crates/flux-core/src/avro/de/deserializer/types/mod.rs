// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

mod blocks;
mod boolean;
mod decimal;
mod discriminant;
mod duration;
mod enums;
mod length_delimited;
mod record;
mod union;

pub(super) use {
    blocks::*, boolean::*, decimal::*, discriminant::*, duration::*, enums::*, length_delimited::*,
    record::*, union::*,
};

use super::*;
