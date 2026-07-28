CREATE TABLE ob_schema_version (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    version INTEGER NOT NULL CHECK (version > 0)
);

INSERT INTO ob_schema_version (singleton, version) VALUES (TRUE, 1);

CREATE TABLE ob_job_instance (
    id BIGSERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    instance_key TEXT NOT NULL,
    UNIQUE (job_name, instance_key)
);

CREATE TABLE ob_step_execution (
    step_id TEXT PRIMARY KEY,
    checkpoint BIGINT NOT NULL,
    write_count BIGINT NOT NULL,
    context JSONB NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 0)
);

CREATE TABLE ob_business_item (
    run_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (run_id, item_key)
);

CREATE FUNCTION ob_spike_delay_selected_commit()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.payload = '__delay_commit__' THEN
        PERFORM pg_sleep(2);
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER ob_spike_delay_selected_commit
AFTER INSERT ON ob_business_item
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION ob_spike_delay_selected_commit();
