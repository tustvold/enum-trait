use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_schema::DataType;
use std::fmt::Display;
use std::fmt::Write;
use arrow_buffer::ArrowNativeType;

/// Formats an array to a string
trait ArrayTrait {
    /// Encodes value at `idx` to `out`
    fn encode(&self, out: &mut String, idx: usize);

    /// Gets value at `idx` as `usize`
    fn usize_value(&self, idx: usize) -> Option<usize>;
}

impl<P: ArrowPrimitiveType> ArrayTrait for PrimitiveArray<P>
where
    P::Native: Display,
{
    fn encode(&self, out: &mut String, idx: usize) {
        if self.is_valid(idx) {
            let _ = write!(out, "{}", self.value(idx));
        }
    }

    fn usize_value(&self, idx: usize) -> Option<usize> {
        self.is_valid(idx).then(|| self.value(idx).as_usize())
    }
}

impl ArrayTrait for Box<dyn ArrayTrait> {
    fn encode(&self, out: &mut String, idx: usize) {
        self.as_ref().encode(out, idx)
    }

    fn usize_value(&self, idx: usize) -> Option<usize> {
        self.as_ref().usize_value(idx)
    }
}

enum ArrayVariant {
    Int32(Int32Array),
    Int64(Int64Array),
    Float32(Float32Array),
    Float64(Float64Array),
}

impl ArrayTrait for ArrayVariant {
    fn encode(&self, out: &mut String, idx: usize) {
        match self {
            ArrayVariant::Int32(v) => v.encode(out, idx),
            ArrayVariant::Int64(v) => v.encode(out, idx),
            ArrayVariant::Float32(v) => v.encode(out, idx),
            ArrayVariant::Float64(v) => v.encode(out, idx),
        }
    }

    fn usize_value(&self, idx: usize) -> Option<usize> {
        match self {
            ArrayVariant::Int32(v) => v.usize_value(idx),
            ArrayVariant::Int64(v) => v.usize_value(idx),
            ArrayVariant::Float32(v) => v.usize_value(idx),
            ArrayVariant::Float64(v) => v.usize_value(idx),
        }
    }
}
fn as_dyn(batch: &RecordBatch) -> Vec<Box<dyn ArrayTrait>> {
    batch
        .columns()
        .iter()
        .map(|a| match a.data_type() {
            DataType::Int32 => Box::new(a.as_primitive::<Int32Type>().clone()) as _,
            DataType::Int64 => Box::new(a.as_primitive::<Int64Type>().clone()) as _,
            DataType::Float32 => Box::new(a.as_primitive::<Float32Type>().clone()) as _,
            DataType::Float64 => Box::new(a.as_primitive::<Float64Type>().clone()) as _,
            d => unimplemented!("{d:?}"),
        })
        .collect()
}

fn as_enum(batch: &RecordBatch) -> Vec<ArrayVariant> {
    batch
        .columns()
        .iter()
        .map(|a| match a.data_type() {
            DataType::Int32 => ArrayVariant::Int32(a.as_primitive().clone()),
            DataType::Int64 => ArrayVariant::Int64(a.as_primitive().clone()),
            DataType::Float32 => ArrayVariant::Float32(a.as_primitive().clone()),
            DataType::Float64 => ArrayVariant::Float64(a.as_primitive().clone()),
            d => unimplemented!("{d:?}"),
        })
        .collect()
}

#[inline(never)]
pub fn encode_batch_dyn(batch: &RecordBatch) -> String {
    encode_batch_impl(batch.num_rows(), as_dyn(batch))
}

#[inline(never)]
pub fn encode_batch_enum(batch: &RecordBatch) -> String {
    encode_batch_impl(batch.num_rows(), as_enum(batch))
}

fn encode_batch_impl<T: ArrayTrait>(num_rows: usize, encoders: Vec<T>) -> String {
    let mut out = String::with_capacity(1024);
    for i in 0..num_rows {
        let mut iter = encoders.iter();
        if let Some(encoder) = iter.next() {
            encoder.encode(&mut out, i);
        }

        for encoder in iter {
            out.push(',');
            encoder.encode(&mut out, i);
        }
        out.push('\n');
    }
    out
}

#[inline(never)]
pub fn horizontal_sum_dyn(batch: &RecordBatch) -> Vec<usize> {
    horizontal_sum_impl(batch.num_rows(), as_dyn(batch))
}

#[inline(never)]
pub fn horizontal_sum_enum(batch: &RecordBatch) -> Vec<usize> {
    horizontal_sum_impl(batch.num_rows(), as_enum(batch))
}

fn horizontal_sum_impl<T: ArrayTrait>(num_rows: usize, encoders: Vec<T>) -> Vec<usize> {
    (0..num_rows)
        .map(|x| {
            encoders
                .iter()
                .map(|e| e.usize_value(x).unwrap_or_default())
                .sum()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_basic() {
        let batch = RecordBatch::try_from_iter([
            ("int32", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as _),
            ("int64", Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as _),
            (
                "float32",
                Arc::new(Float32Array::from(vec![1., 2., 3., 4.])) as _,
            ),
            (
                "float64",
                Arc::new(Float64Array::from(vec![1., 2., 3., 4.])) as _,
            ),
        ])
        .unwrap();

        let encoded = encode_batch_dyn(&batch);
        assert_eq!(encoded, "1,1,1,1\n2,2,2,2\n3,3,3,3\n4,4,4,4\n");
        let sum = horizontal_sum_dyn(&batch);
        assert_eq!(sum, &[4, 8, 12, 16]);

        let encoded = encode_batch_enum(&batch);
        assert_eq!(encoded, "1,1,1,1\n2,2,2,2\n3,3,3,3\n4,4,4,4\n");
        let sum = horizontal_sum_enum(&batch);
        assert_eq!(sum, &[4, 8, 12, 16]);
    }
}
