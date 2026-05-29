CREATE OR REPLACE FUNCTION notify_stage_runs_changed()
RETURNS trigger AS $$
BEGIN
	PERFORM pg_notify(
		'stage_runs_changed',
		json_build_object(
			'run_id', NEW.run_id,
			'stage_run_id', NEW.stage_run_id,
			'stage', NEW.stage,
			'round', NEW.round,
			'status', NEW.status
		)::text
	);

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--> statement-breakpoint
CREATE TRIGGER stage_runs_changed_notify
AFTER INSERT OR UPDATE ON "stage_runs"
FOR EACH ROW
EXECUTE FUNCTION notify_stage_runs_changed();
