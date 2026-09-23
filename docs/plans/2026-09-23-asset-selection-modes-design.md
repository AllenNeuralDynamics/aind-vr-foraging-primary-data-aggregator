# Asset selection modes

The capsule supports two selection modes through command-line parameters that
Code Ocean's App Panel can expose. `manifest` is the default and preserves the
existing behavior: it reads a CSV with a `session` column and requires a
one-to-one match between those rows and selected derived assets. `all` does not
read a CSV. It considers all derived VR-foraging packaging assets, without the
manifest mode's configured version bounds.

In `all` mode, candidates are sorted by packaging version and then creation
time, both descending. An asset is selected only when none of its raw
`source_data` values has already been claimed by a higher-ranked selection.
This makes the highest-version (and then newest) asset win for every raw input
without rejecting a run because older processed assets exist. The run records
the mode and, only for manifest mode, copies and records the exact input CSV.

Tests cover the version/time tie-break and that overlapping raw inputs do not
produce more than one selected processed asset.
