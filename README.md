# ANTS-Based Structural Segmentation to NIDM Exporter

## BVOL and LABELVOL

`BVOL` (data element `ants_000002`) is the brain volume from the brain
extraction mask, in mm^3. `LABELVOL` (`ants_000358`) is the sum of the
labelled structure volumes; the DKT31_CMA labels include no cerebral white
matter, so it is not a brain volume.

The meaning of `BVOL` changed: in outputs of ants-nidm 0.1.0, the `BVOL`
value was the label sum (what is now `LABELVOL`), while `ants_000002` now
describes the mask volume. Read `BVOL` from those outputs as a label sum.
