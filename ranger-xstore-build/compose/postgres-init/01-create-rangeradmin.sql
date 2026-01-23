-- Ensure the application user exists and can access the ranger DB.
DO
$$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rangeradmin') THEN
    CREATE ROLE rangeradmin LOGIN PASSWORD 'rangerR0cks!';
  END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE ranger TO rangeradmin;


