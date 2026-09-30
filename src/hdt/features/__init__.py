"""Point-in-time feature computation (phase 03): every function takes `as_of` and reads the lake only
through `hdt.features.lake_io.LakeView`, which never returns a record with `fetched_at > as_of`."""
