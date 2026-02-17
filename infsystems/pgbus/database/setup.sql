CREATE SCHEMA IF NOT EXISTS pgbus;

CREATE TABLE IF NOT EXISTS pgbus.message(
	 key VARCHAR(60) PRIMARY KEY NOT NULL
	,queue_name VARCHAR(60) NOT NULL
	,message_type VARCHAR(128) NOT NULL
	,payload TEXT NOT NULL
    ,priority INTEGER DEFAULT 0 NOT NULL
    ,correlation_key TEXT
	,scheduled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,dequeued_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS message_queue_name_idx ON pgbus.message(queue_name);


CREATE TABLE IF NOT EXISTS pgbus.dead_letter(
	 key VARCHAR(60) NOT NULL
	,queue_name VARCHAR(60) NOT NULL
	,message_type VARCHAR(128) NOT NULL
	,payload TEXT NOT NULL
    ,priority INTEGER DEFAULT 0 NOT NULL
	,error TEXT NOT NULL
    ,correlation_key TEXT
	,scheduled_at TIMESTAMP
    ,dequeued_at TIMESTAMP NOT NULL
	,captured_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pgbus.message_log(
	 key VARCHAR(60) NOT NULL
	,queue_name VARCHAR(60) NOT NULL
	,message_type VARCHAR(128) NOT NULL
	,payload TEXT NOT NULL
    ,priority INTEGER DEFAULT 0 NOT NULL
    ,correlation_key TEXT
    ,error TEXT
	,scheduled_at TIMESTAMP NOT NULL
    ,dequeued_at TIMESTAMP NOT NULL
	,processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS message_log_correlation_key_idx ON pgbus.message_log(correlation_key);

CREATE TABLE IF NOT EXISTS pgbus.registration(
	 id	INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY
	,queue_name VARCHAR(60) NOT NULL
	,message_type VARCHAR(128) NOT NULL
	,topic_name VARCHAR(60)
	,handler_function VARCHAR(128) NOT NULL
	,run_as_subprocess BOOLEAN NOT NULL DEFAULT FALSE
	,is_active BOOLEAN NOT NULL DEFAULT TRUE
	,prevent_duplication BOOLEAN NOT NULL DEFAULT TRUE
	,keep_log BOOLEAN NOT NULL DEFAULT TRUE
	,description VARCHAR(128)
	,inserted_at	TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
	,updated_at	TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,CONSTRAINT registration_queue_name_message_type_key UNIQUE (queue_name, message_type)
   );