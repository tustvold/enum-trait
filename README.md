# Enum vs Trait Dispatch

A simple experiment into the performance differences between trait and enumeration based dispatch.

In many cases, careful program design can allow amortising dispatch overheads over thousands of values, however,
certain patterns do not lend themselves easily to this workflow.

In particular a common problem that arises is converting between columnar and row-oriented data representations, as in 
such a case it is often unavoidable to do per-value processing.

## Results

The results would indicate even for extremely dispatch heavy workloads, there is relatively little performance difference 
between the two approaches

```
encode_integers_dyn     time:   [371.18 µs 371.37 µs 371.62 µs]
encode_integers_enum    time:   [364.64 µs 364.81 µs 365.01 µs]

horizontal_sum_integers_dyn
                        time:   [44.259 µs 44.300 µs 44.347 µs]
horizontal_sum_integers_enum
                        time:   [44.016 µs 44.062 µs 44.110 µs]

encode_floats_dyn       time:   [963.26 µs 963.54 µs 963.82 µs]

encode_floats_enum      time:   [964.07 µs 964.35 µs 964.65 µs]

horizontal_sum_floats_dyn
                        time:   [52.249 µs 52.279 µs 52.311 µs]
horizontal_sum_floats_enum
                        time:   [51.881 µs 51.905 µs 51.931 µs]

encode_mixed_dyn        time:   [1.3516 ms 1.3526 ms 1.3538 ms]

encode_mixed_enum       time:   [1.2799 ms 1.2804 ms 1.2809 ms]

horizontal_sum_mixed_dyn
                        time:   [91.865 µs 91.894 µs 91.925 µs]
horizontal_sum_mixed_enum
                        time:   [97.526 µs 97.558 µs 97.591 µs]
```
