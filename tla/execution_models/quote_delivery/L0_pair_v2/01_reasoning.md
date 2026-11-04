# Observation and permission

The paired-record layout, JOIN translation, reader budgets, callback and freshness calculations retain the [original abstractions](../L0_pair/01_reasoning.md). Each distinct observation now advances the latest recorded sequence after checking its sequence and publication time. An unavailable view clears the gate. A later observation can advance the sequence even while the gate is closed. The per-order gate checks that permission separately.

The model fixes the feed and ring generation. JOIN establishes a nonzero mark. Every subsequent delivery in this model has a distinct ring sequence. Duplicate inputs, malformed records and switching the SHM resource lie outside its domain. The policy regression separately supplies a newer generation, tests its actual order gate and rejects old-generation and regressing records. The Linux test feeds the original unavailable pair through an authenticated JOIN and polls each physical record separately.
