Cross-module QA pass over track $ARGUMENTS.
Read every content/modules/$ARGUMENTS*.js fragment in the track and check:
1. Terminology consistency (same names for the same concepts across modules).
2. FRAUD_REVIEW_THRESHOLD usage: named constant everywhere, value 0.80 only.
3. No duplicated cold opens or field notes across the track.
4. Check-question difficulty rises through the track; each MCQ has one
   plausible-misconception distractor.
5. Freight Line analogies stay consistent with the blueprint table (never
   remap a concept to a different warehouse metaphor).
Report findings as a fix-list ordered by severity; apply fixes to fragments
and re-validate the track with scripts/validate.py.
