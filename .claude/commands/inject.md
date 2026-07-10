Run `python scripts/inject.py $ARGUMENTS`, then `python scripts/build.py` for the
node --check gate. Report pass/fail. On fail: diagnose, fix the *fragment*
(never patch the player directly), re-inject, re-check.
