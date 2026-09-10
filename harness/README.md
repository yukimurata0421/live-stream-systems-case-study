# CRA Harness Engineering

このdirectoryは、CRA/Dell Recovery Agent Protocol v1のSUT検証とは別に、検証基盤そのものを検証するtest-only Harnessの入口である。

```text
Scenario Registry
      -> Environment Factory
      -> Fault Injector (requested -> armed -> triggered -> completed)
      -> SUT (CRA + Dell Agent + separate SQLite)
      -> Observer (raw facts only)
      -> append-only Evidence
      -> Independent Oracle
      -> Classifier
      -> Trust Gate / Report
```

`harness/scenarios/protocol_v1.json`がdeterministic、negative control、randomized explorationの共通registryである。実行は次のとおり。

```bash
.venv/bin/cra-harness --run-id <immutable-run-id>
```

同じrun IDへの上書きは拒否する。成果物は`artifacts/harness/<run_id>/`へ保存される。production credentialを読み込まず、実signal、process、Pod、Deployment、network操作は行わない。

探索caseをregressionへ昇格する場合は、元seed、case index、最小化したinputs/fault、期待不変条件、sourceを固定し、`deterministic=true`の新しいstable scenario IDを付ける。探索結果をそのまま合格根拠へ混ぜない。

Operational State Space v3は既存v2 randomized 200件を維持し、7-axis weighted randomized 500件、mandatory high-risk coverage、15 Negative Control、accelerated lifecycleを追加する。

```bash
tools/sqlite_runtime/run-fixed.sh .venv/bin/python -m cra_harness.runner.cli_v3 --run-id <immutable-v3-run-id>
```

`coverage/operational_state_matrix.json`は全直積を定義し、探索済みcellだけをmaterializeする。未探索cellをPASSとは扱わず、mandatory high-risk predicateがminimum hit未達なら`HARNESS_V3_TRUSTED=false`とする。

## 公開postmortem由来のI/O境界テスト

[現構成への適用・修正記録](../docs/engineering/records/2026-09-06_100_postmortem_io_fault_hardening.md)のPF-01〜06は、
次の独立したintegration suiteで再実行できる。このsuiteは一時file、loopback mTLS、専用test childの終了を使う。
上記Protocol v1のfake-only runnerとは別のtest populationであり、production host/network/credentialを使わない。

```bash
timeout 120s nice -n 19 tools/run_full_regression.sh \
  tests/harness/integration/test_postmortem_io_faults.py -q
```

入力不正、HTTP途中切断、error responseの解放、append/state境界、short write、FIFOを検証する。
faultが起きた証拠、保存bytes、連番、別hostのGET継続をassertし、PF-01〜06の各防御を壊した
negative control 6種類も検出する。事例、仮説、注入点、必須観測、test node、未証明範囲は
`harness/scenarios/postmortem_io_v1.json`へ固定し、次のrunnerは既存runを上書きしない。

```bash
tools/sqlite_runtime/run-fixed.sh .venv/bin/python -m tools.run_postmortem_io_harness \
  --output artifacts/postmortem-io-<immutable-run-id>/harness
```

候補全体の回帰と、関連6モジュールのbranch measurementは別populationとして保存する。

```bash
.venv/bin/python -m tools.run_candidate_full_validation \
  --output artifacts/postmortem-io-<immutable-run-id>/full
```

テスト成功を7日soak PASS、production fault injection、physical effectの実証へ読み替えない。
24時間checkpointと配備前gateは
[専用runbook](../docs/runbooks/2026-09-06_postmortem_io_predeploy_24h_checkpoint.md)を使う。
実装・Harness trust・未配備境界の結果は
[記録101](../docs/engineering/records/2026-09-06_101_postmortem_harness_and_24h_predeploy_gate.md)に固定した。

後続の[記録102](../docs/engineering/records/2026-09-06_102_checkpoint_resources_and_soak_evidence_gap.md)では、
実署名packetの2-hop→collector→復旧判定、実archiveのcheckpoint CLI→gate、collectorのprocess終了後再開を検証する。
`--preserve-incomplete`の診断prefixは欠落を保持し、配備gateを開かない。通常のpytest assertion失敗を
`HARNESS_FAILURE`へまとめず、`TEST_FAILURE`として原因未確定のまま記録する。
