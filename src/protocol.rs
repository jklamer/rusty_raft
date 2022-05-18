use std::io::Cursor;
use apache_avro::{AvroSchema, SpecificSingleObjectWriter, SpecificSingleObjectReader};
use apache_avro::schema::derive::AvroSchemaComponent;
use bytes::{BytesMut, Buf};
use serde::de::DeserializeOwned;
use serde::{Serialize, Deserialize};
use tokio::io::{BufWriter, AsyncWriteExt, self, AsyncReadExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{ReadHalf, WriteHalf};

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


impl <Command> Message<Command> 
where 
    Command: AvroSchemaComponent + Sync + Send 
{
    fn get_frame_header(&self) -> u8 {
        match self {
            Message::AEReq(_) => b'+',
            Message::AERes(_) => b'-',
            Message::RVReq(_) => b'=',
            Message::RVRes(_) => b'/',
        }
    }
}


pub struct MessageWriter<Command> 
where 
    Command: AvroSchemaComponent + Sync + Send
{
    append_entry_request_writer: SpecificSingleObjectWriter<AppendEntriesRequest<Command>>,
    append_entry_result_writer: SpecificSingleObjectWriter<AppendEntriesResult>,
    request_vote_request_writer: SpecificSingleObjectWriter<RequestVoteRequest>,
    request_vote_result_writer: SpecificSingleObjectWriter<RequestVoteResult>
}

impl <Command> MessageWriter<Command> 
where 
    Command: AvroSchemaComponent + Serialize + DeserializeOwned + Sync + Send
{
    pub fn new() -> MessageWriter<Command> {
        MessageWriter::<Command> {
            append_entry_request_writer: SpecificSingleObjectWriter::<AppendEntriesRequest::<Command>>::with_capacity(1024).expect("Unable to resolve Schema"),
            append_entry_result_writer: SpecificSingleObjectWriter::<AppendEntriesResult>::with_capacity(1024).expect("Unable to resolve Schema"),
            request_vote_request_writer: SpecificSingleObjectWriter::<RequestVoteRequest>::with_capacity(1024).expect("Unable to resolve Schema"),
            request_vote_result_writer: SpecificSingleObjectWriter::<RequestVoteResult>::with_capacity(1024).expect("Unable to resolve Schema")
        }
    }

    pub async fn send(&mut self, m: Message<Command>,  buf: &mut BufWriter<WriteHalf<'_>>) -> Result<usize, ProtocolError> {
        let mut out = Vec::with_capacity(1024);
        out.push(m.get_frame_header());
        out.extend_from_slice(&0u32.to_be_bytes());
        let len  = match m {
            Message::AEReq(msg) => self.append_entry_request_writer.write(msg, &mut out)?,
            Message::AERes(msg) => self.append_entry_result_writer.write(msg, &mut out)?,
            Message::RVReq(msg) => self.request_vote_request_writer.write(msg, &mut out)?,
            Message::RVRes(msg) => self.request_vote_result_writer.write(msg, &mut out)?,
        };
        let len: [u8; 4] = (len as u32).to_be_bytes();
        for i in 1usize..5 {
            out.insert(i, len[i-1])
        }
        buf.write_all(&out[..]).await?;
        Ok(out.len())
    }
}

pub struct MessageReader<Command> 
where 
    Command: AvroSchemaComponent + Sync + Send
{
    append_entry_request_reader: SpecificSingleObjectReader<AppendEntriesRequest<Command>>,
    append_entry_result_reader: SpecificSingleObjectReader<AppendEntriesResult>,
    request_vote_request_reader: SpecificSingleObjectReader<RequestVoteRequest>,
    request_vote_result_reader: SpecificSingleObjectReader<RequestVoteResult>
}

impl <Command> MessageReader<Command> 
where 
    Command: AvroSchemaComponent + Serialize + DeserializeOwned + Sync + Send
{
    pub fn new() -> MessageReader<Command> {
        MessageReader::<Command> {
            append_entry_request_reader: SpecificSingleObjectReader::<AppendEntriesRequest::<Command>>::new().expect("Unable to resolve Schema"),
            append_entry_result_reader: SpecificSingleObjectReader::<AppendEntriesResult>::new().expect("Unable to resolve Schema"),
            request_vote_request_reader: SpecificSingleObjectReader::<RequestVoteRequest>::new().expect("Unable to resolve Schema"),
            request_vote_result_reader: SpecificSingleObjectReader::<RequestVoteResult>::new().expect("Unable to resolve Schema")
        }
    }

    pub async fn read(&mut self, buf: &mut Cursor<&[u8]>) -> Result<Message<Command>, ProtocolError> {
        let frame_id =  buf.read_u8().await?;
        let len = buf.read_u32().await?;
        if buf.remaining() >= len as usize {
            Ok(match frame_id {
                b'+' => Message::AEReq(self.append_entry_request_reader.read(buf)?),
                b'-' => Message::AERes(self.append_entry_result_reader.read(buf)?),
                b'=' => Message::RVReq(self.request_vote_request_reader.read(buf)?),
                b'/' => Message::RVRes(self.request_vote_result_reader.read(buf)?),
                _ => return Err(ProtocolError::FrameDeSynced),
            })
        } else {
            Err(ProtocolError::IncompleteFrame)
        }
    }
}

pub struct ReadConnection<'a, Command> 
where 
    Command: AvroSchemaComponent + Sync + Send
{
    stream: ReadHalf<'a>,
    reader: MessageReader<Command>,
    buffer: BytesMut,
}

pub struct WriteConnection<'a, Command> 
where 
    Command: AvroSchemaComponent + Sync + Send + Serialize
{
    stream: BufWriter<WriteHalf<'a>>,
    writer: MessageWriter<Command>,
}

impl <'a, Command> WriteConnection<'a, Command> 
where 
    Command: AvroSchemaComponent + Sync + Send + Serialize + DeserializeOwned
{
    pub async fn send_message(&mut self, msg: Message<Command>) -> Result<usize, ProtocolError> {
        self.writer.send(msg, &mut self.stream).await
    }
}


pub enum ProtocolError {
    Serialization(apache_avro::Error),
    Io(io::Error),
    IncompleteFrame,
    FrameDeSynced,
}

impl From<io::Error> for ProtocolError {
    fn from(io_error: io::Error) -> Self {
        Self::Io(io_error)
    }
}

impl From<apache_avro::Error> for ProtocolError {
    fn from(avro_error: apache_avro::Error) -> Self {
        Self::Serialization(avro_error)
    }
}
mod tests {
    use super::*;
}
