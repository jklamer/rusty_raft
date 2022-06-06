use apache_avro::schema::derive::AvroSchemaComponent;
use apache_avro::{AvroSchema, SpecificSingleObjectReader, SpecificSingleObjectWriter};
use bytes::{Buf, BytesMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};
use std::io::Cursor;
use tokio::io::{self, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;

#[derive(Clone, Debug, Serialize, Deserialize, AvroSchema, PartialEq, Eq)]
pub struct AppendEntriesRequest<C>
where
    C: AvroSchemaComponent + Sync + Send,
{
    term: i64,
    leader_id: String,
    prev_log_index: i64,
    prev_log_term: i64,
    entries: Vec<LogEntry<C>>,
    leader_commit: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, AvroSchema, PartialEq, Eq)]
pub struct LogEntry<C>
where
    C: AvroSchemaComponent + Sync + Send,
{
    term: i64,
    log_idex: i64,
    command: C,
}

#[derive(Clone, Debug, Serialize, Deserialize, AvroSchema, PartialEq, Eq)]
pub struct AppendEntriesResult {
    term: i64,
    success: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, AvroSchema, PartialEq, Eq)]
pub struct RequestVoteRequest {
    term: i64,
    candidate_id: String,
    last_log_index: i64,
    last_log_term: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, AvroSchema, PartialEq, Eq)]
pub struct RequestVoteResult {
    term: i64,
    vote_granted: bool,
}

#[derive(Clone, PartialEq, Eq, EnumDiscriminants)]
#[strum_discriminants(name(MessageKind))]
#[strum_discriminants(derive(EnumIter))]
pub enum Message<C>
where
    C: AvroSchemaComponent + Sync + Send,
{
    AEReq(AppendEntriesRequest<C>),
    AERes(AppendEntriesResult),
    RVReq(RequestVoteRequest),
    RVRes(RequestVoteResult),
    ServerId(String)
}

impl<C> Message<C>
where
    C: AvroSchemaComponent + Sync + Send,
{
    fn get_frame_header(&self) -> u8 {
        message_marker_from_kind(&self.into())
    }
}

fn message_marker_from_kind(message_kind: &MessageKind) -> u8
{
    match message_kind {
        MessageKind::AEReq => b'+',
        MessageKind::AERes => b'-',
        MessageKind::RVReq => b'=',
        MessageKind::RVRes => b'/',
        MessageKind::ServerId => b'*'
    }
}

fn message_marker_to_kind(marker: u8) -> Result<MessageKind, ProtocolError>
{
    Ok(match marker {
        b'+' => MessageKind::AEReq,
        b'-' => MessageKind::AERes,
        b'=' => MessageKind::RVReq,
        b'/' => MessageKind::RVRes,
        b'*' => MessageKind::ServerId,
        _ => return Err(ProtocolError::FrameDeSynced),
    })
}

pub struct MessageWriter<C>
where
    C: AvroSchemaComponent + Sync + Send,
{
    append_entry_request_writer: SpecificSingleObjectWriter<AppendEntriesRequest<C>>,
    append_entry_result_writer: SpecificSingleObjectWriter<AppendEntriesResult>,
    request_vote_request_writer: SpecificSingleObjectWriter<RequestVoteRequest>,
    request_vote_result_writer: SpecificSingleObjectWriter<RequestVoteResult>,
    server_id_writer: SpecificSingleObjectWriter<String>
}

impl<C> MessageWriter<C>
where
    C: AvroSchemaComponent + Serialize + DeserializeOwned + Sync + Send,
{
    pub fn new() -> MessageWriter<C> {
        MessageWriter::<C> {
            append_entry_request_writer:
                SpecificSingleObjectWriter::<AppendEntriesRequest<C>>::with_capacity(1024)
                    .expect("Unable to resolve Schema"),
            append_entry_result_writer:
                SpecificSingleObjectWriter::<AppendEntriesResult>::with_capacity(1024)
                    .expect("Unable to resolve Schema"),
            request_vote_request_writer:
                SpecificSingleObjectWriter::<RequestVoteRequest>::with_capacity(1024)
                    .expect("Unable to resolve Schema"),
            request_vote_result_writer:
                SpecificSingleObjectWriter::<RequestVoteResult>::with_capacity(1024)
                    .expect("Unable to resolve Schema"),
            server_id_writer:
                SpecificSingleObjectWriter::<String>::with_capacity(64)
                    .expect("Unable to resolve Schema"),
        }
    }

    pub async fn write<W: AsyncWrite + Unpin>(
        &mut self,
        m: Message<C>,
        buf: &mut BufWriter<W>,
    ) -> Result<usize, ProtocolError> {
        let mut out = Vec::with_capacity(1024);
        out.push(m.get_frame_header());
        out.extend_from_slice(&0u32.to_be_bytes());
        let len = match m {
            Message::AEReq(msg) => self.append_entry_request_writer.write(msg, &mut out)?,
            Message::AERes(msg) => self.append_entry_result_writer.write(msg, &mut out)?,
            Message::RVReq(msg) => self.request_vote_request_writer.write(msg, &mut out)?,
            Message::RVRes(msg) => self.request_vote_result_writer.write(msg, &mut out)?,
            Message::ServerId(id) => self.server_id_writer.write(id, &mut out)?
        };
        let len: [u8; 4] = (len as u32).to_be_bytes();
        for i in 1usize..5 {
            let _ = std::mem::replace(&mut out[i], len[i - 1]);
        }
        buf.write_all(&out[..]).await?;
        Ok(out.len())
    }
}

pub struct MessageReader<C>
where
    C: AvroSchemaComponent + Sync + Send,
{
    append_entry_request_reader: SpecificSingleObjectReader<AppendEntriesRequest<C>>,
    append_entry_result_reader: SpecificSingleObjectReader<AppendEntriesResult>,
    request_vote_request_reader: SpecificSingleObjectReader<RequestVoteRequest>,
    request_vote_result_reader: SpecificSingleObjectReader<RequestVoteResult>,
    server_id_reader: SpecificSingleObjectReader<String>
}

impl<C> MessageReader<C>
where
    C: AvroSchemaComponent + Serialize + DeserializeOwned + Sync + Send,
{
    pub fn new() -> MessageReader<C> {
        MessageReader::<C> {
            append_entry_request_reader:
                SpecificSingleObjectReader::<AppendEntriesRequest<C>>::new()
                    .expect("Unable to resolve Schema"),
            append_entry_result_reader: SpecificSingleObjectReader::<AppendEntriesResult>::new()
                .expect("Unable to resolve Schema"),
            request_vote_request_reader: SpecificSingleObjectReader::<RequestVoteRequest>::new()
                .expect("Unable to resolve Schema"),
            request_vote_result_reader: SpecificSingleObjectReader::<RequestVoteResult>::new()
                .expect("Unable to resolve Schema"),
            server_id_reader: SpecificSingleObjectReader::<String>::new().expect("unable to resolve schema")
        }
    }

    pub async fn read(&mut self, buf: &mut Cursor<&[u8]>) -> Result<Message<C>, ProtocolError> {
        if buf.remaining() < 5 {
            return Err(ProtocolError::IncompleteFrame);
        }
        let frame_id = message_marker_to_kind(buf.read_u8().await?)?;
        let len = buf.read_u32().await?;
        if buf.remaining() >= len as usize {
            Ok(match frame_id {
                MessageKind::AEReq => Message::AEReq(self.append_entry_request_reader.read(buf)?),
                MessageKind::AERes => Message::AERes(self.append_entry_result_reader.read(buf)?),
                MessageKind::RVReq => Message::RVReq(self.request_vote_request_reader.read(buf)?),
                MessageKind::RVRes => Message::RVRes(self.request_vote_result_reader.read(buf)?),
                MessageKind::ServerId => Message::ServerId(self.server_id_reader.read(buf)?)
            })
        } else {
            Err(ProtocolError::IncompleteFrame)
        }
    }
}

pub async fn connection_reader<'a, C>(
    mut tcp_stream_read: ReadHalf<'a>,
    message_queue: tokio::sync::mpsc::Sender<Message<C>>,
) where
    C: AvroSchemaComponent + Serialize + DeserializeOwned + Sync + Send,
{
    let mut input_buffer = BytesMut::new();
    let mut message_reader: MessageReader<C> = MessageReader::new();
    loop {
        if let Ok(read_amount) = tcp_stream_read.read(&mut input_buffer).await {
            if read_amount > 0 {
                let mut messages: Vec<Message<C>> = Vec::with_capacity(5);
                let mut cursor = Cursor::new(&input_buffer[..]);
                loop {
                    match message_reader.read(&mut cursor).await {
                        Ok(message) => messages.push(message),
                        Err(ProtocolError::IncompleteFrame) => break,
                        Err(ProtocolError::FrameDeSynced) => {
                            println!("FRAME DESYNCED");
                            return;
                        },
                        Err(protocol_error) => match protocol_error {
                            ProtocolError::Serialization(_) => todo!(),
                            ProtocolError::Io(_) => todo!(),
                            ProtocolError::IncompleteFrame | ProtocolError::FrameDeSynced => {
                                unreachable!()
                            }
                        },
                    }
                }
                let read_amount = cursor.position();
                input_buffer.advance(read_amount as usize);
                for message in messages.into_iter() {
                    if let Err(e) = message_queue.send(message).await {
                        // TODO log error, retry?
                        println!("ERROR PUSHING MESSAGE TO QUEUE")
                    }
                }
            } else {
                //TODO LOG connection reset, send reconnect request
                println!("CONNECTION RESET BY PEER");
                return;
            }
        } else {
            //TODO LOG connection error, send reconnect request
            println!("IO ERROR WHILE READING");
            return;
        }
    }
}

pub async fn connection_writer<'a, C>(
    tcp_stream_write: WriteHalf<'a>,
    mut message_queue: tokio::sync::mpsc::Receiver<Message<C>>,
) where
    C: AvroSchemaComponent + Serialize + DeserializeOwned + Sync + Send,
{
    let mut tcp_stream_write = BufWriter::new(tcp_stream_write);
    let mut message_writer = MessageWriter::<C>::new();
    while let Some(message) = message_queue.recv().await {
        match message_writer.write(message, &mut tcp_stream_write).await {
            Ok(_) => (), /*implement messages sent callback*/
            Err(protocol_error) => match protocol_error {
                ProtocolError::Serialization(_) => todo!(),
                ProtocolError::Io(_) => todo!(),
                ProtocolError::IncompleteFrame => unreachable!(),
                ProtocolError::FrameDeSynced => unreachable!(),
            },
        }
    }
}

#[derive(Debug)]
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
    use strum::IntoEnumIterator;

    use super::*;

    #[derive(Clone, Debug, Serialize, Deserialize, AvroSchema, PartialEq)]
    struct TestC {
        key: String,
        value: Option<String>,
    }

    #[test]
    pub fn test_message_protocol() {
        let m1 = Message::<TestC>::AEReq(AppendEntriesRequest {
            term: 42,
            leader_id: "Im the leader".into(),
            prev_log_index: 3,
            prev_log_term: 2,
            entries: vec![LogEntry {
                term: 3,
                log_idex: 4,
                command: TestC {
                    key: "key".into(),
                    value: Some("value".into()),
                },
            }],
            leader_commit: 3,
        });
        let m2 = Message::<TestC>::AERes(AppendEntriesResult {
            term: 3i64,
            success: true,
        });
        let m3 = Message::<TestC>::RVReq(RequestVoteRequest {
            term: 3,
            candidate_id: "Please make me leader".into(),
            last_log_index: 4,
            last_log_term: 3,
        });
        let m4 = Message::<TestC>::RVRes(RequestVoteResult {
            term: 5,
            vote_granted: false,
        });
        let m5 = Message::<TestC>::ServerId("id".into());
        let mut bites = BufWriter::new(Vec::<u8>::new());
        let mut mw = MessageWriter::<TestC>::new();
        let mut mr = MessageReader::<TestC>::new();

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                mw.write(m1.clone(), &mut bites)
                    .await
                    .expect("Should write");
                mw.write(m2.clone(), &mut bites)
                    .await
                    .expect("Should write");
                mw.write(m3.clone(), &mut bites)
                    .await
                    .expect("Should write");
                mw.write(m4.clone(), &mut bites)
                    .await
                    .expect("Should write");
                mw.write(m5.clone(), &mut bites)
                    .await
                    .expect("Should write");
                bites.flush().await.expect("Should flush");
                let bites = bites.into_inner();
                let mut inner = Cursor::new(&bites[..]);
                let read_m1 = mr.read(&mut inner).await.expect("Should read");
                let read_m2 = mr.read(&mut inner).await.expect("Should read");
                let read_m3 = mr.read(&mut inner).await.expect("Should read");
                let read_m4 = mr.read(&mut inner).await.expect("Should read");
                let read_m5 = mr.read(&mut inner).await.expect("Should read");

                assert_msg_eq(m1, read_m1);
                assert_msg_eq(m2, read_m2);
                assert_msg_eq(m3, read_m3);
                assert_msg_eq(m4, read_m4);
                assert_msg_eq(m5, read_m5);
            });
    }

    fn assert_msg_eq<T: AvroSchemaComponent + Sync + Send + PartialEq + std::fmt::Debug>(
        expect: Message<T>,
        actual: Message<T>,
    ) {
        match (expect, actual) {
            (Message::AEReq(expect), Message::AEReq(actual)) => assert_eq!(expect, actual),
            (Message::AERes(expect), Message::AERes(actual)) => assert_eq!(expect, actual),
            (Message::RVReq(expect), Message::RVReq(actual)) => assert_eq!(expect, actual),
            (Message::RVRes(expect), Message::RVRes(actual)) => assert_eq!(expect, actual),
            (Message::ServerId(expect), Message::ServerId(actual)) => assert_eq!(expect, actual),
            _ => assert!(false, "Non-matching types"),
        }
    }

    #[test]
    fn test_marker_serde() {
        for kind in MessageKind::iter() {
            assert_eq!(kind, message_marker_to_kind(message_marker_from_kind(&kind)).expect("match input bytes to outputs"), "match input bytes to outputs")
        }
    }
}
