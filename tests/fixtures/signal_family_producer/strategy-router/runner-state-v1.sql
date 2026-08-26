BEGIN TRANSACTION;
CREATE TABLE candidate_state (
                    occurrence_id TEXT NOT NULL PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    candidate_effective_trade_date TEXT,
                    candidate_variant TEXT,
                    candidate_generation_sha256 TEXT,
                    candidate_snapshot_schema_version INTEGER,
                    state TEXT NOT NULL,
                    last_feature_sequence INTEGER NOT NULL,
                    last_feature_batch_id TEXT,
                    updated_at TEXT NOT NULL,
                    eligible_high_price_raw REAL,
                    eligible_high_source_event_time TEXT,
                    eligible_high_available_at TEXT
                );
INSERT INTO "candidate_state" VALUES('600000.SH','600000.SH',NULL,NULL,NULL,NULL,'armed',0,'signal-family-producer-fixture-0','2026-08-24T07:00:00+00:00',NULL,NULL,NULL);
CREATE TABLE processed_batch (
                    feature_sequence INTEGER PRIMARY KEY,
                    feature_batch_id TEXT NOT NULL UNIQUE,
                    envelope_fingerprint TEXT NOT NULL,
                    feature_payload_hash TEXT NOT NULL,
                    dataset_snapshot_id TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    source_generation_id TEXT,
                    source_sequence INTEGER,
                    source_batch_id TEXT,
                    source_content_hash TEXT,
                    result_json TEXT NOT NULL
                );
INSERT INTO "processed_batch" VALUES(0,'signal-family-producer-fixture-0','1b8e4082028d5804b81f9167a7a8f573af6d88f92ea397b510360264fc7e1162','2eb330cc9aafc120f6c58870d4a4422333a17bd61c155ddef60db95bf103931c','dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd','2026-08-24T07:00:00+00:00','2026-08-24T07:00:00+00:00','2026-08-24T07:00:00+00:00','3333333333333333333333333333333333333333333333333333333333333333',0,'signal-family-producer-fixture-0','2eb330cc9aafc120f6c58870d4a4422333a17bd61c155ddef60db95bf103931c','{"feature_batch_id":"signal-family-producer-fixture-0","feature_sequence":0,"lifecycle_feature_fingerprints":{},"processed_candidates":1,"signals":[{"sequence":1,"signal":{"action":"watch","available_at":"2026-08-24T07:00:00Z","candidate_id":"600000.SH","dataset_snapshot_id":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","event_time":"2026-08-24T07:00:00Z","evidence":{"runner_transition":{"evaluator_contract_fingerprint":"2222222222222222222222222222222222222222222222222222222222222222","event":"entry_ready","feature_batch_id":"signal-family-producer-fixture-0","feature_sequence":0,"from_state":"idle","to_state":"armed"}},"expires_at":"2026-08-24T13:00:00Z","feature_snapshot_id":"2eb330cc9aafc120f6c58870d4a4422333a17bd61c155ddef60db95bf103931c","parameter_fingerprint":"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","producer_commit":"ffffffffffffffffffffffffffffffffffffffff","reason_codes":["signal_family_producer_fixture"],"schema_version":1,"signal_id":"37cf23850338b371f98b2513f54f90216c229b57553cb2fa4b65277db9a527cc","strategy_id":"n-shape","strategy_version":"1"}}],"skipped_candidates":0,"transitioned_candidates":1}');
CREATE TABLE runner_metadata (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    strategy_spec_fingerprint TEXT NOT NULL,
                    strategy_spec_json TEXT NOT NULL,
                    evaluator_contract_fingerprint TEXT NOT NULL,
                    candidate_input_mode TEXT
                        CHECK(candidate_input_mode IN ('flat', 'occurrence'))
                );
INSERT INTO "runner_metadata" VALUES(1,'dd467e4c66d1b740a058b250f1932de5335f3a248670212696e8d45f530fcf2e','{"allowed_actions":["watch"],"feature_contract_id":"intraday-pit","initial_state":"idle","min_feature_contract_version":1,"optional_features":[],"parameters":{},"producer_commit":"ffffffffffffffffffffffffffffffffffffffff","required_features":[{"allow_degraded":false,"level":"required","min_contract_version":1,"name":"rel_same_minute"}],"run_mode":"shadow","strategy_id":"n-shape","transitions":[{"event":"entry_ready","from_state":"idle","to_state":"armed"}],"version":1}','2222222222222222222222222222222222222222222222222222222222222222','flat');
CREATE TABLE runner_session_close_receipt (
                        trade_date TEXT PRIMARY KEY,
                        receipt_id TEXT NOT NULL UNIQUE,
                        source_id TEXT NOT NULL,
                        signal_high_watermark INTEGER NOT NULL,
                        payload_json TEXT NOT NULL
                    );
CREATE TABLE runner_session_segment (
                        trade_date TEXT PRIMARY KEY,
                        runner_generation_id TEXT NOT NULL,
                        start_after_sequence INTEGER NOT NULL,
                        final_sequence INTEGER NOT NULL,
                        record_count INTEGER NOT NULL,
                        raw_bytes INTEGER NOT NULL,
                        chain_hash TEXT NOT NULL,
                        final_feature_sequence INTEGER NOT NULL,
                        final_feature_batch_id TEXT NOT NULL
                    );
INSERT INTO "runner_session_segment" VALUES('2026-08-24','7777777777777777777777777777777777777777777777777777777777777777',0,1,1,975,'305c0bedb4f3bfb751e6a982c2f5154237aeeaad59376dbf38cecdf0ff1c9fd3',0,'signal-family-producer-fixture-0');
CREATE TABLE runner_signal (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL UNIQUE,
                feature_sequence INTEGER NOT NULL,
                candidate_id TEXT NOT NULL,
                action TEXT NOT NULL,
                entry_signal_id TEXT,
                candidate_occurrence_id TEXT,
                event_time TEXT NOT NULL,
                available_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
INSERT INTO "runner_signal" VALUES(1,'37cf23850338b371f98b2513f54f90216c229b57553cb2fa4b65277db9a527cc',0,'600000.SH','watch',NULL,NULL,'2026-08-24T07:00:00Z','2026-08-24T07:00:00Z','2026-08-24T13:00:00Z','{"action":"watch","available_at":"2026-08-24T07:00:00Z","candidate_id":"600000.SH","dataset_snapshot_id":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","event_time":"2026-08-24T07:00:00Z","evidence":{"runner_transition":{"evaluator_contract_fingerprint":"2222222222222222222222222222222222222222222222222222222222222222","event":"entry_ready","feature_batch_id":"signal-family-producer-fixture-0","feature_sequence":0,"from_state":"idle","to_state":"armed"}},"expires_at":"2026-08-24T13:00:00Z","feature_snapshot_id":"2eb330cc9aafc120f6c58870d4a4422333a17bd61c155ddef60db95bf103931c","parameter_fingerprint":"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","producer_commit":"ffffffffffffffffffffffffffffffffffffffff","reason_codes":["signal_family_producer_fixture"],"schema_version":1,"signal_id":"37cf23850338b371f98b2513f54f90216c229b57553cb2fa4b65277db9a527cc","strategy_id":"n-shape","strategy_version":"1"}');
CREATE TABLE runner_source_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    source_generation_id TEXT NOT NULL
);
INSERT INTO "runner_source_identity" VALUES(1,'7777777777777777777777777777777777777777777777777777777777777777');
CREATE INDEX runner_signal_entry_lookup_idx
            ON runner_signal(candidate_id, candidate_occurrence_id, action, sequence DESC)
            ;
CREATE INDEX runner_signal_exit_lookup_idx
            ON runner_signal(
                candidate_id, candidate_occurrence_id, entry_signal_id,
                action, available_at, sequence
            )
            ;
CREATE UNIQUE INDEX processed_batch_source_sequence_uq
                    ON processed_batch(source_sequence)
                    WHERE source_sequence IS NOT NULL
                    ;
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('runner_signal',1);
COMMIT;
