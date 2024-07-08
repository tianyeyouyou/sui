// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod coalesce_locals;
mod constant_fold;
mod eliminate_locals;
mod forwarding_jumps;
mod inline_blocks;
mod simplify_jumps;

use move_proc_macros::growing_stack;
use move_symbol_pool::Symbol;

use crate::{
    cfgir::cfg::MutForwardCFG,
    editions::FeatureGate,
    expansion::ast::Mutability,
    hlir::ast::*,
    parser::ast::ConstantName,
    shared::{unique_map::UniqueMap, CompilationEnv},
};

use std::collections::BTreeSet;

pub type Optimization = fn(
    &FunctionSignature,
    &UniqueMap<Var, (Mutability, SingleType)>,
    &UniqueMap<ConstantName, Value>,
    &mut MutForwardCFG,
) -> bool;

const OPTIMIZATIONS: &[Optimization] = &[
    eliminate_locals::optimize,
    constant_fold::optimize,
    simplify_jumps::optimize,
    inline_blocks::optimize,
];

const MOVE_2024_OPTIMIZATIONS: &[Optimization] = &[
    eliminate_locals::optimize,
    constant_fold::optimize,
    forwarding_jumps::optimize,
    simplify_jumps::optimize,
    inline_blocks::optimize,
];

#[growing_stack]
pub fn optimize(
    env: &mut CompilationEnv,
    package: Option<Symbol>,
    signature: &FunctionSignature,
    infinite_loop_starts: &BTreeSet<Label>,
    locals: &mut UniqueMap<Var, (Mutability, SingleType)>,
    constants: &UniqueMap<ConstantName, Value>,
    cfg: &mut MutForwardCFG,
    is_constant: bool,
) {
    let mut count = 0;

    let optimize_2024 = env.supports_feature(package, FeatureGate::Move2024Optimizations);

    let optimizations = if optimize_2024 {
        MOVE_2024_OPTIMIZATIONS
    } else {
        OPTIMIZATIONS
    };
    let opt_count = optimizations.len();
    for optimization in optimizations.iter().cycle() {
        // if we have fully cycled through the list of optimizations without a change,
        // it is safe to stop
        if count >= opt_count {
            debug_assert_eq!(count, opt_count);
            break;
        }
        // reset the count if something has changed
        if optimization(signature, locals, constants, cfg) {
            count = 0
        } else {
            count += 1
        }
    }
    // If we aren't optimizing a constant, attempt to coalesce locals and re-optimize.
    if !is_constant && optimize_2024 {
        let new_locals = coalesce_locals::optimize(
            signature,
            infinite_loop_starts,
            locals.clone(),
            cfg,
        );
        *locals = new_locals;
        cfg.recompute();

        // Re-optimize, but don't recur again.
        optimize(
            env,
            package,
            signature,
            infinite_loop_starts,
            locals,
            constants,
            cfg,
            true,
        );
    }
}
