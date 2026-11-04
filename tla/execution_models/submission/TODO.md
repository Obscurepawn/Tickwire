# Replay coverage

The current mock interleaves admissions, economic facts and unsent aborts in one durable event sequence. The historical economic model recovers admissions before replaying facts. A model of the current implementation needs that mixed sequence and an interruption between appending a fact and applying it. Use repeated trade IDs, terminal reports before a late fee, a partially applied history and a failed reopen as acceptance cases. [Ordered abort model](../mock_abort/ROADMAP.md) · [Historical economic model](L2_economic_facts/00_input.md)
