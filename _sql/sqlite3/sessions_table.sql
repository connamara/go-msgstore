DROP TABLE IF EXISTS sessions;

CREATE TABLE sessions (
  session_id VARCHAR(64) NOT NULL,
  creation_time DATETIME NOT NULL,
  incoming_seqnum INT NOT NULL, 
  outgoing_seqnum INT NOT NULL,
  PRIMARY KEY (session_id)
);
