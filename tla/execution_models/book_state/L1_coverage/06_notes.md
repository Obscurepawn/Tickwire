# L1 review notes

Independent read-only review against `coverage()`, `BookView::quantity`, and `BookView::best` confirmed that `Through` is anchored to the worst nonzero snapshot level, incremental deltas never widen `Through`, out-of-window positive levels become `Known` until deleted back to `Unknown`, and out-of-window stored levels conservatively report `Unknown` best when in-window levels are empty.
