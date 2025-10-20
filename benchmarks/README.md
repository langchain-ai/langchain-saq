# Benchmarks

```
pip install arq && python benchmarks/simple.py
```

## Results
============================================================
Running SAQ Benchmark (Regular Mode)
============================================================
SAQ enqueue 10000 1.0054121017456055
SAQ process 10000 noop 12.63696002960205
SAQ process 1000 sleep 2.816277027130127

============================================================
Running SAQ Benchmark (Cluster Mode)
============================================================
SAQ enqueue 10000 0.4777801036834717
SAQ process 10000 noop 2.013828992843628
SAQ process 1000 sleep 2.9254660606384277