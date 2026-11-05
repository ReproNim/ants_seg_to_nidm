# ANTS-Based Structural Segmentation to NIDM Exporter

## Finding the subject in an input NIDM file

With `--nidm`, the stats are attached to the input's subject whose
`ndar:src_subject_id` matches `-subjid`, compared without a leading `sub-` and
leading zeros. A subject is any node with that property (under the `https://`
or the `http://` NDAR IRI), whatever its `rdf:type`. If the file has subjects
and none matches, the run fails (exit 1) and writes nothing, with or without
`--forcenidm`; `--forcenidm` adds a new subject only to a file that has none.
If several nodes match and all store the same ID, the one that owns an
anatomical MRI acquisition is used (the run prints which); if none or more than
one of them does, the run fails. If several match but store different IDs
(`sub-01` and `001`), the run fails.

The lookup is `ants_seg_to_nidm.ants_seg_to_nidm.find_subject(graph,
subject_id, forceagent=False, created_agents=())`. It returns the subject node
(or `None` for a file without subjects under `forceagent`), raises
`SubjectLookupError` otherwise, and never changes the graph, so a caller can
check an input before computing anything. `segstats_jsonld` has the same
function with the same rules.

## BVOL and LABELVOL

`BVOL` (data element `ants_000002`) is the brain volume from the brain
extraction mask, in mm^3. `LABELVOL` (`ants_000358`) is the sum of the
labelled structure volumes; the DKT31_CMA labels include no cerebral white
matter, so it is not a brain volume.

The meaning of `BVOL` changed: in outputs of ants-nidm 0.1.0, the `BVOL`
value was the label sum (what is now `LABELVOL`), while `ants_000002` now
describes the mask volume. Read `BVOL` from those outputs as a label sum.
