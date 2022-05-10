use apache_avro::{AvroSchema};
use apache_avro::schema::derive::AvroSchemaComponent;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct AppendEntriesRequest<T> 
where
    T: AvroSchemaComponent
{
    term : i64,
    leader_id: String,
    prev_log_index: i64,
    prev_log_term: i64,
    entries: Vec<T>
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct AppendEntriesResult {
    term : i64,
    success: bool,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct RequestVoteRequest {
    term: i64,
    candidate_id: String,
    last_log_index: i64,
    last_log_term: i64
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct RequestVoteResult {
    term: i64,
    vote_granted: bool
}

mod tests {
    use super::*;
}
