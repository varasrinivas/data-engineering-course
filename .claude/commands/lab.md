Build the lab for module $1 at tier $2.
1. Read docs/03-LAB-ENGINE-SPEC.md section for tier $2 and docs/04-ENGINE-CONTRACT.md.
   Read the engine file to confirm supported ops/config keys — never invent engine API.
2. Understand-It path: write the engine config (T1: dataset + starter
   query; T2: sparksim scenario; T3: trace JSON in engine/traces/).
3. Build-It-with-AI path: write a complete, copy-paste Claude Code prompt
   that scaffolds the equivalent real local PySpark project (venv, pip
   install pyspark, dataset generation script, the exercise, a pytest
   assertion). The prompt must be self-contained — assume the student's
   Claude Code has never seen this course.
4. Verify: T1/T2 configs reference only ops the engine implements; T3
   trace validates against traces/schema.json.
