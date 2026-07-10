Create content/modules/$ARGUMENTS.js for module $ARGUMENTS.
1. Read docs/00-COURSE-BLUEPRINT.md and find the module row for $ARGUMENTS
   (title, core idea, lab tier). Read CLAUDE.md schema.
2. Read one completed module from the same track (or D3 if none) as the
   style reference.
3. Scaffold the full MODS entry with: coldOpen (NimbusMart incident),
   concept sections including exactly one `analogy` (Freight Line) and one
   `javaBridge` section, lab stub with correct tier, 4 check questions,
   fieldNotes. Mark unfinished prose with TODO markers, never lorem ipsum.
4. Run: python scripts/validate.py content/modules/$ARGUMENTS.js
5. Report what needs human/author attention as a short list.
