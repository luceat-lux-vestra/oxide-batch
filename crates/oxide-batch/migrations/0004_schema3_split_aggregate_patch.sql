SET LOCAL search_path TO oxide_batch, pg_catalog;

DO $$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 3 THEN
        RAISE EXCEPTION 'oxide_batch schema version 3 is required before the split aggregate patch';
    END IF;
END
$$;

ALTER TABLE ob_flow_decision
    DROP CONSTRAINT ob_flow_decision_transition_kind_check;

ALTER TABLE ob_flow_decision
    ADD CONSTRAINT ob_flow_decision_transition_kind_check
        CHECK (transition_kind IN (
            'STEP_EXIT', 'DECIDER', 'COMPLETED_STEP_REUSE', 'SPLIT_AGGREGATE'
        ));
