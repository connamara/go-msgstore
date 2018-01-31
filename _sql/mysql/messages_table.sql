USE msgstore;

DROP TABLE IF EXISTS messages;

CREATE TABLE messages (
  session_id VARCHAR(128) NOT NULL,
  msgseqnum INT NOT NULL, 
  message TEXT NOT NULL,
  PRIMARY KEY (session_id, msgseqnum)
);