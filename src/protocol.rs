use apache_avro::{AvroSchema, SpecificSingleObjectWriter};
use apache_avro::schema::derive::AvroSchemaComponent;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct AppendEntriesRequest<Command> 
where
    Command: AvroSchemaComponent + Sync + Send
{
    term : i64,
    leader_id: String,
    prev_log_index: i64,
    prev_log_term: i64,
    entries: Vec<LogEntry<Command>>,
    leader_commit: i64,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct LogEntry<Command>
where
    Command : AvroSchemaComponent + Sync + Send
{
    term: i64,
    log_idex: i64,
    command: Command
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

pub enum Message<Command> 
where
    Command: AvroSchemaComponent + Sync + Send
{
    AEReq(AppendEntriesRequest<Command>),
    AERes(AppendEntriesResult),
    RVReq(RequestVoteRequest),
    RVRes(RequestVoteResult),
}

pub struct MessageSender<Command> 
where 
    Command: AvroSchemaComponent + Sync + Send
{
    append_entry_request_sender: SpecificSingleObjectWriter<AppendEntriesRequest<Command>>,
    append_entry_result_sender: SpecificSingleObjectWriter<AppendEntriesResult>,
    request_vote_request_sender: SpecificSingleObjectWriter<RequestVoteRequest>,
    request_vote_result_sender: SpecificSingleObjectWriter<RequestVoteResult>
}

impl MessageSender<Command> {
    pub fn new() -> MessageSender<Command> {
        MessageSender::<Command> {
            append_entry_request_sender: SpecificSingleObjectWriter::<AppendEntriesRequest::<Command>>::new(),
            append_entry_result_sender: SpecificSingleObjectWriter::<AppendEntriesResult>::new(),
            request_vote_request_sender: SpecificSingleObjectWriter::<RequestVoteRequest>::new(),
            request_vote_result_sender: SpecificSingleObjectWriter::<RequestVoteResult>::new()
        }
    }

    pub fn send(&self, m: Message<Command>) -> Result<u64, apache_avro::Error> {

    }
}




}
where
    W: Write,
    Command: AvroSchemaComponent + Sync + Send
{

}
mod tests {
    use super::*;
}
