# Who can spend shared request weight?

At source revision `9b2ed630721911e01046822bddc9db21b2fa5aee`, can overlapping issue preparations, a lost reply or a stopped consumer free the same capacity twice? One normal consumer competes with another, while both normal and recovery traffic charge one shared window. [Quota contract](../../../../specs/implementation/quota.md)
