// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow_array::types::*;
use arrow_array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray, RecordBatch};
use criterion::{criterion_group, criterion_main, Criterion};
use enum_trait::{encode_batch_dyn, encode_batch_enum, horizontal_sum_dyn, horizontal_sum_enum};
use rand::distributions::Standard;
use rand::prelude::*;
use std::sync::Arc;

pub fn make_array<T>(size: usize, null_density: f32) -> ArrayRef
where
    T: ArrowPrimitiveType,
    Standard: Distribution<T::Native>,
{
    let mut rng = StdRng::seed_from_u64(123456);

    let array: PrimitiveArray<T> = (0..size)
        .map(|_| (rng.gen::<f32>() > null_density).then(|| rng.gen()))
        .collect();
    Arc::new(array)
}

const NUM_ROWS: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let batch = RecordBatch::try_from_iter([
        ("int64", make_array::<Int64Type>(NUM_ROWS, 0.1)),
        ("int32", make_array::<Int32Type>(NUM_ROWS, 0.1)),
    ])
    .unwrap();

    c.bench_function("encode_integers_dyn", |b| {
        b.iter(|| encode_batch_dyn(&batch))
    });
    c.bench_function("encode_integers_enum", |b| {
        b.iter(|| encode_batch_enum(&batch))
    });
    c.bench_function("horizontal_sum_integers_dyn", |b| {
        b.iter(|| horizontal_sum_dyn(&batch))
    });
    c.bench_function("horizontal_sum_integers_enum", |b| {
        b.iter(|| horizontal_sum_enum(&batch))
    });

    let batch = RecordBatch::try_from_iter([
        ("float32", make_array::<Float32Type>(NUM_ROWS, 0.1)),
        ("float64", make_array::<Float64Type>(NUM_ROWS, 0.1)),
    ])
    .unwrap();

    c.bench_function("encode_floats_dyn", |b| b.iter(|| encode_batch_dyn(&batch)));
    c.bench_function("encode_floats_enum", |b| {
        b.iter(|| encode_batch_enum(&batch))
    });
    c.bench_function("horizontal_sum_floats_dyn", |b| {
        b.iter(|| horizontal_sum_dyn(&batch))
    });
    c.bench_function("horizontal_sum_floats_enum", |b| {
        b.iter(|| horizontal_sum_enum(&batch))
    });

    let batch = RecordBatch::try_from_iter([
        ("int64", make_array::<Int64Type>(NUM_ROWS, 0.1)),
        ("int32", make_array::<Int32Type>(NUM_ROWS, 0.1)),
        ("float32", make_array::<Float32Type>(NUM_ROWS, 0.1)),
        ("float64", make_array::<Float64Type>(NUM_ROWS, 0.1)),
    ])
    .unwrap();

    c.bench_function("encode_mixed_dyn", |b| b.iter(|| encode_batch_dyn(&batch)));
    c.bench_function("encode_mixed_enum", |b| {
        b.iter(|| encode_batch_enum(&batch))
    });
    c.bench_function("horizontal_sum_mixed_dyn", |b| {
        b.iter(|| horizontal_sum_dyn(&batch))
    });
    c.bench_function("horizontal_sum_mixed_enum", |b| {
        b.iter(|| horizontal_sum_enum(&batch))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
