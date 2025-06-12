## Pipelines Currently Managed by algo features
```mermaid
---
title: cleanup_sideload_tables
---
graph LR
cleanup_sideload_tables

```
```mermaid
---
title: content_affinity
---
graph LR
check_base_range_partitions ---> lift_base
content_affinity ---> pavo_lift
content_affinity ---> pavo_non_franchise_lift_top_50
content_counts ---> content_affinity
content_subset_counts ---> permutation_counts
gold_top_content_editorial ---> top_content
lift_base ---> content_counts
lift_base ---> content_subset_counts
lift_base ---> user_counts
pavo_lift ---> pavo_lift_size_assertion
permutation_counts ---> content_affinity
top_content ---> lift_base
user_counts ---> content_affinity

```
```mermaid
---
title: content_affinity_base
---
graph LR
base ---> first_watch_timestamps
base ---> user_content_watchtimes
silver_user ---> base
silver_video ---> base

```
```mermaid
---
title: content_base
---
graph LR
ama_base ---> scc_ama
asset_length ---> scc_ama
asset_length_internal ---> asset_length
content_base ---> ama_base
content_base ---> asset_length_internal
scc_ama ---> content_audience_ama
silver_user ---> content_base
silver_video ---> content_base

```
```mermaid
---
title: reactivation_features
---
graph LR
silver_user
silver_video

```
```mermaid
---
title: restore_user_features
---
graph LR
restore_silver_user_daily_rollup
restore_silver_video_all_time_rollup
restore_silver_video_daily_rollup
restore_silver_video_daily_rollup_agg
restore_trial_features
restore_user_features

```
```mermaid
---
title: user_features_data_validation
---
graph LR
check_for_duplicate_rows_in_silver_user_daily_rollup
check_for_duplicate_rows_in_silver_video_all_time_rollup
check_for_duplicate_rows_in_silver_video_daily_rollup
check_for_duplicate_rows_in_silver_video_daily_rollup_agg
check_for_duplicate_rows_in_trial_features
check_for_duplicate_rows_in_user_features

```
```mermaid
---
title: user_features_v2
---
graph LR
silver_user ---> silver_user_daily_rollup_v2
silver_user ---> trial_features_v2
silver_user_daily_rollup_v2 ---> user_features_v2
silver_video ---> silver_video_daily_rollup_v2
silver_video_all_time_rollup_v2 ---> user_features_v2
silver_video_daily_rollup_agg_v2 ---> user_features_v2
silver_video_daily_rollup_v2 ---> silver_video_all_time_rollup_v2
silver_video_daily_rollup_v2 ---> silver_video_daily_rollup_agg_v2
silver_video_daily_rollup_v2 ---> user_features_v2
trial_features_v2 ---> user_features_v2

```
```mermaid
---
title: user_visit_features
---
graph LR
user_visit_features_agg ---> user_visit_features
user_visit_features_rollup ---> user_visit_features_agg
user_visit_level_features ---> user_visit_features_rollup

```
