# Initial recovery-closure model

This directory retains the first model, configuration and logs. Crash originally selected the entire disk or cache, and DirSync synchronized only the current key. The revised fault model explores which unsynchronized renames independently survive a crash. A successful directory sync commits the entire cache. Use the parent runs directory for the results under the revised assumptions.
