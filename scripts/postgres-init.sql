-- One Postgres instance hosts a database per service. Each service's own
-- migrations create the pgvector extension inside its database.
CREATE DATABASE mail_processor;
CREATE DATABASE mail_mapper;
CREATE DATABASE gsa;
