# Mail Mapper

## Dynamic Scoring

- Match scoring now loads weights and optional calibration parameters from the database tables `scoring_weights`, `scoring_weight_entries`, and `scoring_calibrations`.
- Each persisted match and candidate stores the scoring configuration version and raw score to enable offline analysis and correlation with feedback.
- Default weights are migrated automatically; new configurations can be activated by inserting rows with `is_active = true`.
